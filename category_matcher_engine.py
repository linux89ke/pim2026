import os
import re
import json
import pickle
import threading
import numpy as np
import pandas as pd
import logging
import traceback
import sqlite3
# SentenceTransformers removed — TF-IDF used exclusively
#
# sklearn is imported lazily. Measured with -X importtime, importing this
# module cost 10.25s of which sklearn was 6.75s — paid on every cold start,
# including every Streamlit Cloud container spin-up, whether or not a single
# category is ever matched. Nothing at module scope needs it; the vectoriser
# and the similarity call are only reached once an index is actually built.
#
# _sk() memoises the two symbols so the cost is paid at most once, on the
# first real use, instead of at import.
_SK = {}


def _sk():
    """TfidfVectorizer and cosine_similarity, imported on first use."""
    if not _SK:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        _SK["TfidfVectorizer"] = TfidfVectorizer
        _SK["cosine_similarity"] = cosine_similarity
    return _SK


def __getattr__(name):
    """Module-level fallback so `engine.TfidfVectorizer` still resolves.

    Keeps the previous import surface working for anything that referenced
    these names from this module, without reintroducing the eager import.
    """
    if name in ("TfidfVectorizer", "cosine_similarity"):
        return _sk()[name]
    raise AttributeError(name)


logger = logging.getLogger(__name__)

def clean_text(text: str) -> str:
    if pd.isna(text): return ""
    text = str(text).lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def compile_rules_from_json(raw_rules: list, code_to_path: dict = None) -> dict:
    """
    Converts raw JSON category rules into the compiled format the engine expects.

    Each rule in raw_rules should look like:
        {
            "category_name": "Car Polishes & Waxes",
            "category_code": 1000089,
            "positive": {"car": 2, "polish": 3, "wax": 3}
        }

    Args:
        raw_rules:    List of rule dicts loaded from your JSON file.
        code_to_path: Optional dict mapping str(category_code) -> full category path
                      e.g. {"1000089": "Automobile > Car Care > Car Polishes & Waxes"}
                      If provided, the full path is used as the lookup key so it
                      aligns with what the TF-IDF index stores in engine.categories.

    Returns:
        A dict keyed by lowercase category path (or name), ready for set_compiled_rules().
    """
    if code_to_path is None:
        code_to_path = {}

    compiled = {}
    for rule in raw_rules:
        positive_kws = rule.get('positive', {})
        if not positive_kws:
            continue

        # Resolve the key: prefer the full path from code_to_path, fall back to category_name
        code_str = str(rule.get('category_code', ''))
        cat_key = code_to_path.get(code_str, rule.get('category_name', '')).lower().strip()
        if not cat_key:
            continue

        # Build one regex pattern covering all positive keywords
        pattern = re.compile(
            r'\b(' + '|'.join(re.escape(k.lower()) for k in positive_kws) + r')\b'
        )

        compiled[cat_key] = {
            'pattern': pattern,
            'weights': {k.lower(): float(v) for k, v in positive_kws.items()}
        }

    return compiled


# ── Global suppression constants ────────────────────────────────────────────────
_SAME_DOMAIN_CATEGORIES = {
    # Each entry: top-level domain -> set of leaf category names that
    # GENUINELY BELONG to that domain. Only suppress if the current
    # category is a legitimate sub-category of the predicted domain.
    # Do NOT add entries just because they were false-positived into
    # a domain — that causes over-suppression.

    'health & beauty': {
        'creams', 'strips', 'supplements', 'creams & moisturizers',
        'conditioners', 'face moisturizers', 'cleansers', 'soaps & cleansers',
        'hair & scalp treatments', 'toners', 'face', 'body',
        'cellulite massagers', 'serums', 'shaving creams', 'gels',
        'wrinkle & anti-aging devices', 'lips', 'soaps', 'washes',
        'body wash', 'joint & muscle pain relief', 'bubble bath', 'lotions',
        'essential oils', 'health & fitness', 'detox & cleanse', 'oils',
        'sets & kits', 'shaving gels', 'hair sprays', 'eau de parfum',
        'skin care', 'salon & spa chairs', 'massage chairs', 'heating pads',
        'makeup sets', 'foundation', 'face primer', 'makeup organizers',
        'hair color', 'back braces', 'cellulite massagers', 'serums',
        'face primer',  # facial massager word match
        'hairpieces', 'wrinkle & anti-aging devices', 'bubble bath',
        'body wash', 'shaving creams', 'soaps', 'washes', 'body',
        'face', 'lips', 'gels', 'essential oils', 'detox & cleanse',
        'health & fitness', 'body scrubs', 'nail care', 'eye care',
        'feminine care', 'oral care', 'medical supplies',
    },

    'home & office': {
        # Books/media in H&O
        'bestselling books', 'faith & spirituality',
        # Sets & kits that are home-related
        'sets & kits',
        # Medical/support items shelved in H&O
        'medical support hose',
        # Toys shelved under H&O sub-paths (kids bathroom etc)
        'push & pull toys', 'stacking & nesting toys',
        # Women's clothing sub-sections sometimes shelved in H&O
        "women's",
        # Kitchen appliances — genuinely in H&O
        'freezers', 'food processors', 'mixers & blenders', 'rice cookers',
        'deep fryers', 'air fryers', 'cookers', 'microwave ovens',
        'electric pressure cookers', 'pressure cookers', 'hot pots',
        'waffle makers', 'toasters', 'kettles', 'coffee makers',
        # Home tools/cleaning
        'vacuum cleaners', 'wet & dry vacuums', 'bagless vacuum cleaner',
        'washing machines', 'dishwashers',
        # Furniture/storage genuinely in H&O
        'standing shelf units', 'coat racks',
        # Arts/crafts genuinely in H&O
        'printer cutters', 'art set', 'canvas boards & panels',
        # Kitchen tools genuinely in H&O
        'kitchen utensils & gadgets', 'kitchen storage & organization accessories',
        'stemmed water glasses', 'whisks', 'wastebasket bags',
        # Bedding/home decor genuinely in H&O
        'bedding sets', 'curtain panels', 'duvet covers', 'mosquito net',
        # Small appliances
        'usb fans',
        # Home improvement
        'sprayers', 'security & filtering',
    },

    'electronics': {
        # Audio genuinely in Electronics
        'bluetooth speakers', 'bluetooth headsets', 'earphones & headsets',
        'portable bluetooth speakers', 'sound bars', 'headphone amplifiers',
        'earbud headphones', 'headphone extension cables',
        'wireless lavalier microphones',
        # Video/display
        'smart tvs', 'overhead projectors',
        # Accessories genuinely in Electronics
        'ceiling fans', 'ceiling fan light kits', 'usb fans',
        # Remote controls
        'tv remote controls', 'remote controls',
        # Portable electronics
        'gadgets',
    },

    'phones & tablets': {
        # Accessories genuinely in P&T
        'chargers', 'earbud headphones', 'rubber strap',
        'electrical device mounts', 'earphones & headsets',
        # Phones genuinely in P&T
        'cell phones', 'android phones', 'smartphones',
        'flip cases', 'cases', 'screen protectors',
    },

    'fashion': {
        # Footwear genuinely in Fashion
        'sandals', 'sneakers', 'slippers', 'shoes', 'rain boots', 'boots',
        # Clothing genuinely in Fashion
        'casual dresses', 'hats & caps', 'briefs', 'thongs', 'socks',
        'unisex fabrics', 'stockings', 'polos', 'bras', 'underwear',
        't-shirts', 'shirts', 'outerwear', 'clothing', 'dresses',
        'jackets', 'coats', 'jeans',
        # Accessories genuinely in Fashion
        'handbags', 'jewellery',
    },

    'computing': {
        'laptops', 'desktops', 'tablets', 'monitors', 'keyboards',
        'mice', 'printers', 'scanners', 'hard drives', 'ssds',
        'computer accessories', 'networking', 'routers',
        'portable power banks', 'bluetooth headsets',
    },

    'musical instruments': {
        'subwoofers', 'bags, cases & covers', 'racks & stands', 'musicals',
        'microphones', 'amplifiers', 'mixers',
    },

    'grocery': {
        # Only suppress genuinely-Grocery sub-categories — we WANT
        # to flag products put in wrong Grocery sub-paths
        'standard batteries',  # batteries incorrectly in Grocery
    },

    'baby products': {
        'pillows', 'lumbar supports', 'wipes, napkins & serviettes',
        'walkers', 'feminine washes', 'baby formula', 'diapers',
        'baby monitors', 'strollers',
    },

    'gaming': {
        'gaming headsets', 'gaming mice', 'gaming keyboards',
        'controllers', 'ps 5 games', 'ps4 games', 'xbox games',
        'pc gaming', 'gaming chairs', 'gaming desks',
    },
}

_CROSS_DOMAIN_BLOCKS = [
    # (current_leaf_keywords, forbidden_predicted_top_prefixes)

    # Supplements/medicine must not go to Phones & Tablets ("tablet" = pill)
    ({'supplements', 'tablets', 'capsules', 'vitamins', 'syrup', 'herbal',
      'herbs', 'strips', 'milk substitutes'},
     {'phones & tablets', 'electronics', 'automobile',
      'industrial & scientific', 'sporting goods'}),

    # Clothing/Fashion must not go to Grocery, Sporting Goods, or Automobile
    ({'fashion', 'clothing', 'outerwear', 'apparel', 'shoes', 'footwear',
      'sneakers', 'slippers', 'socks', 'polos', 'bras', 'underwear',
      't-shirts', 'shirts', 'dresses', 'jackets', 'coats', 'jeans',
      'sandals', 'rain boots', 'boots', 'stockings'},
     {'grocery', 'industrial & scientific', 'automobile',
      'sporting goods', 'electronics', 'home & office', 'pet supplies'}),

    # Electronics/Audio/Phones must not bleed into unrelated domains
    ({'electronics', 'cell phones', 'bluetooth speakers', 'bluetooth headsets',
      'earphones', 'headsets', 'smart watches', 'wrist watches', 'tv remote',
      'remote controls', 'wi-fi', 'dongles', 'power banks', 'earbuds',
      'headphones', 'laptops', 'cameras', 'speakers', 'portable bluetooth'},
     {'grocery', 'automobile', 'industrial & scientific',
      'garden & outdoors', 'sporting goods', 'fashion', 'pet supplies'}),

    # Watches/clocks must not go to Fashion accessories or Sporting Goods
    ({'wrist watches', "women's watches", "men's watches", 'kids watches',
      'smart watches', 'wall clocks', 'alarm clocks'},
     {'fashion', 'sporting goods', 'grocery', 'automobile'}),

    # Health/Beauty/Personal care must not bleed into Grocery or unrelated
    ({'health', 'beauty', 'skin care', 'creams', 'makeup', 'foundation',
      'heating pads', 'salon & spa', 'salon', 'spa', 'massage', 'medical',
      'shaving gels', 'hair sprays', 'eau de parfum', 'fragrance', 'perfume',
      'sets & kits'},
     {'grocery', 'industrial & scientific', 'sporting goods',
      'automobile', 'phones & tablets', 'toys & games', 'pet supplies'}),

    # Home/Kitchen/Furniture must not bleed into Grocery, Sporting Goods,
    # or Automobile
    ({'home', 'kitchen', 'storage', 'cleaning', 'toilet', 'coat racks',
      'sewing machines', 'pressure cookers', 'electric pressure cookers',
      'cookers', 'christian books', 'books', 'printer cutters', 'sprayers',
      'art set', 'security & filtering'},
     {'grocery', 'sporting goods', 'automobile',
      'industrial & scientific', 'garden & outdoors'}),

    # Baby/Kids play equipment must not go to Garden or Sporting Goods
    ({'outdoor safety', 'play yard', 'baby', 'strollers', 'nursery'},
     {'garden & outdoors', 'sporting goods', 'automobile'}),

    # Same-domain false positives: sub-categories of the same domain
    # e.g. Salon & Spa Chairs -> H&B/Massage Tools, Cell Phones -> P&T/SIM Trays,
    # Pressure Cookers -> H&O/Pressure Cooker Parts
    # These are handled by the segment check using code_to_path,
    # but as a safety net if code_to_path isn't available:
    ({'salon & spa chairs', 'massage chairs'},
     {'health & beauty'}),
    ({'cell phones', 'earphones & headsets'},
     {'phones & tablets'}),
    ({'pressure cookers', 'electric pressure cookers'},
     {'home & office'}),

    # Creams/Strips/Supplements must not bleed into unrelated domains
    ({'creams', 'strips', 'supplements', 'creams & moisturizers'},
     {'sporting goods', 'automobile', 'grocery',
      'phones & tablets', 'industrial & scientific'}),

    # Bluetooth Headsets/Remote Controls are sub-items of Electronics/P&T
    ({'bluetooth headsets', 'tv remote controls', 'remote controls',
      'android phones', 'musicals'},
     {'sporting goods', 'grocery', 'automobile', 'garden & outdoors',
      'industrial & scientific', 'fashion', 'pet supplies'}),

    # Books must not go to Office Electronics or unrelated domains
    ({'christian books & bibles', 'motivational & self-help',
      'business & economics'},
     {'home & office', 'industrial & scientific', 'automobile',
      'grocery', 'sporting goods'}),

    # Kitchen appliances/tools must not go to Automobile or Sporting Goods
    ({'freezers', 'mixers & blenders', 'food processors', 'rice cookers',
      'bakeware sets', 'utensils', 'printer cutters', 'art set',
      'push & pull toys'},
     {'automobile', 'sporting goods', 'grocery',
      'industrial & scientific', 'garden & outdoors'}),

    # Umbrellas must not bleed into Fashion sub-items
    ({'stick umbrellas', 'umbrellas'},
     {'fashion', 'grocery', 'automobile', 'sporting goods'}),

    # Bags/backpacks must not go to Electronics camera accessories
    ({'backpacks', 'camping backpacks', 'bags'},
     {'electronics', 'automobile', 'industrial & scientific'}),

    # Video/digital games must not go to H&B or unrelated domains
    ({'digital games', 'ps 5 games', 'ps4 games', 'xbox games'},
     {'health & beauty', 'grocery', 'automobile',
      'industrial & scientific', 'fashion'}),

    # Hair/fabric dyes must not go to Toys tie-dye kits
    ({'dyes', 'hair dye', 'fabric dye'},
     {'toys & games', 'grocery', 'automobile', 'industrial & scientific'}),
]

class CategoryMatcherEngine:
    def __init__(self, db_path="cat_learning.db"):
        self.db_path = db_path
        self.categories = []
        self._tfidf_built = False
        self.learning_db = {}
        # Corrections added via apply_learned_correction(auto_save=False) that
        # haven't been flushed to the DB yet. save_learning_db() only inserts
        # these — not the full learning_db — so a batch of N approvals no
        # longer re-inserts the entire correction history N times over.
        self._pending_corrections: dict = {}
        # Negative learning: categories a human explicitly REJECTED for a
        # product name. Excluded from future suggestions, and products
        # re-listed under a known-rejected category are auto-flagged.
        self.negative_db: dict = {}        # clean_name -> {lowercased rejected categories}
        self.negative_reasons: dict = {}   # (clean_name, category_lower) -> human reason text
        self._pending_negatives: list = [] # (clean_name, category, reason) awaiting flush
        self.compiled_rules = {}  # Store JSON rules directly in the engine
        # (vectorizer, classifier) kept as ONE tuple so a background retrain can
        # swap both halves atomically. A reader that caught a half-swap would
        # transform with a new vocabulary and predict with a classifier fitted
        # on the old one — a silent feature-dimension mismatch.
        self._corr_model = None
        self._retrain_lock = threading.Lock()
        self._retrain_thread = None
        self._retrain_pending = False
        self._counts_cache = None
        self._init_db()
        self.load_learning_db()

    # Read-only views over _corr_model so existing call sites keep working and
    # can never observe a partially-swapped model.
    @property
    def correction_vectorizer(self):
        m = self._corr_model
        return m[0] if m else None

    @property
    def correction_classifier(self):
        m = self._corr_model
        return m[1] if m else None

    def set_compiled_rules(self, rules, code_to_path: dict = None):
        """
        Loads heuristic rules into the engine.

        Accepts either:
          - A raw list of JSON rule dicts (auto-compiled via compile_rules_from_json)
          - An already-compiled dict (from a prior compile_rules_from_json call)
        """
        if isinstance(rules, list):
            # compile_rules_from_json builds a regex per category, which is
            # expensive and the rules rarely change between runs — memoize on a
            # content hash so repeat calls with the same rules are a no-op.
            import hashlib
            try:
                _rules_key = hashlib.md5(
                    (json.dumps(rules, sort_keys=True, default=str)
                     + "|" + json.dumps(sorted((code_to_path or {}).keys()))).encode()
                ).hexdigest()
            except Exception:
                _rules_key = None
            if _rules_key is not None and _rules_key == getattr(self, "_compiled_rules_key", None):
                return
            self.compiled_rules = compile_rules_from_json(rules, code_to_path or {})
            self._compiled_rules_key = _rules_key
        else:
            self.compiled_rules = rules or {}
            self._compiled_rules_key = None

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute('''
                    CREATE TABLE IF NOT EXISTS category_corrections (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        category TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS category_negatives (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        category TEXT,
                        reason TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
        except Exception as e:
            logger.warning(f"Failed to init category learning DB: {e}")

    def load_learning_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query("SELECT name, category FROM category_corrections", conn)
                if not df.empty:
                    self.learning_db = df.groupby('name')['category'].last().to_dict()
                    # Fitting this model is a multi-minute, one-vs-rest fit over
                    # thousands of categories. Load the pickled model when the
                    # corrections table has not changed since it was written;
                    # only fall back to fitting when there is nothing usable on
                    # disk (see _ensure_correction_model).
                    self._ensure_correction_model(df)
        except Exception as e:
            logger.warning(f"Failed to load category learning DB: {e}")
        try:
            with sqlite3.connect(self.db_path) as conn:
                ndf = pd.read_sql_query("SELECT name, category, reason FROM category_negatives", conn)
            if not ndf.empty:
                ndf['category_l'] = ndf['category'].astype(str).str.strip().str.lower()
                self.negative_db = {n: set(g) for n, g in ndf.groupby('name')['category_l']}
                self.negative_reasons = {
                    (n, cl): str(r).strip()
                    for n, cl, r in zip(ndf['name'], ndf['category_l'], ndf['reason'].fillna(''))
                    if str(r).strip()
                }
                logger.info(f"[NegLearn] Loaded {len(ndf)} category negatives for {len(self.negative_db)} names")
        except Exception as e:
            logger.warning(f"Failed to load category negatives: {e}")

    # Hard ceiling on training rows — prevents the classifier from ever
    # attempting to fit on the full (unbounded) correction history.
    _MAX_TRAIN_ROWS = 50_000
    # Categories with fewer than this many examples are dropped before
    # fitting — LogisticRegression/SGDClassifier need >=2 per class anyway,
    # and very rare classes add memory/compute cost for near-zero benefit.
    _MIN_EXAMPLES_PER_CATEGORY = 2

    # Bumped whenever the training pipeline or hyper-parameters below change,
    # so an on-disk model fitted by an older version is never reused.
    _MODEL_VERSION = 1
    # A burst of approvals fires one retrain request per item; wait this long
    # after the last one before fitting so a 50-item batch trains once.
    _RETRAIN_DEBOUNCE_SECS = 5.0

    @property
    def _model_cache_path(self) -> str:
        return f"{self.db_path}.clf.v{self._MODEL_VERSION}.pkl"

    def _corrections_fingerprint(self) -> str:
        """Identify the state of the corrections table.

        Rows are only ever INSERTed, and pruning always removes the oldest ids,
        so (count, min_id, max_id) changes on every insert and every prune.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT COUNT(*), MIN(id), MAX(id) FROM category_corrections"
                ).fetchone()
            return f"v{self._MODEL_VERSION}:{row[0]}:{row[1]}:{row[2]}"
        except Exception as e:
            logger.warning(f"Could not fingerprint corrections table: {e}")
            return ""

    def _load_cached_model(self, fingerprint: str, allow_stale: bool = False):
        """Load the persisted model.

        With allow_stale, a model fitted against a slightly different
        corrections table is still returned. That matters on an ephemeral host
        (Streamlit Cloud), where a committed model may not exactly match a
        committed DB: a slightly-out-of-date suggestion engine available
        instantly beats no suggestions for the minutes a refit would take.
        Exact-match lookups in learning_db are unaffected either way.
        """
        if not os.path.exists(self._model_cache_path):
            return None
        if not fingerprint and not allow_stale:
            return None
        try:
            with open(self._model_cache_path, "rb") as f:
                blob = pickle.load(f)
            if blob.get("fingerprint") != fingerprint and not allow_stale:
                return None
            return (blob["vectorizer"], blob["classifier"])
        except Exception as e:
            logger.warning(f"Ignoring unreadable correction-model cache: {e}")
            return None

    def _save_cached_model(self, model, fingerprint: str) -> None:
        if not model or not fingerprint:
            return
        # Write to a temp file and rename so a crash mid-write, or a second
        # process fitting concurrently, can't leave a half-written pickle that
        # the next start would have to discover by failing to load it.
        tmp = f"{self._model_cache_path}.{os.getpid()}.tmp"
        try:
            with open(tmp, "wb") as f:
                pickle.dump(
                    {"fingerprint": fingerprint, "vectorizer": model[0], "classifier": model[1]},
                    f, protocol=pickle.HIGHEST_PROTOCOL,
                )
            os.replace(tmp, self._model_cache_path)
        except Exception as e:
            logger.warning(f"Failed to persist correction model: {e}")
            try:
                os.unlink(tmp)
            except Exception:
                pass

    def _ensure_correction_model(self, df=None) -> None:
        """Make a model available without ever blocking start-up.

        Fitting takes minutes on a constrained host. Doing it inline here would
        stall the first page render — on Streamlit Cloud, past the boot timeout,
        and again after every container restart because that filesystem is
        ephemeral. So nothing here is allowed to block: load the exact model if
        it is on disk, otherwise fall back to a stale one, otherwise start with
        no classifier at all. Any gap is filled by a background refit.

        Until a model is ready, predict_category_from_learning() still serves
        exact matches from learning_db, which is the primary path anyway; only
        the fuzzy fallback is briefly unavailable.
        """
        fingerprint = self._corrections_fingerprint()
        cached = self._load_cached_model(fingerprint)
        if cached is not None:
            self._corr_model = cached
            logger.info("[CatLearn] Loaded correction model (matches corrections table)")
            return

        stale = self._load_cached_model(fingerprint, allow_stale=True)
        if stale is not None:
            self._corr_model = stale
            logger.warning(
                "[CatLearn] On-disk model does not match the corrections table; "
                "using it anyway and refitting in the background"
            )
        else:
            logger.warning(
                "[CatLearn] No usable model on disk — starting without the fuzzy "
                "category matcher; fitting in the background"
            )
        self._schedule_retrain()

    def _schedule_retrain(self) -> None:
        """Request a retrain without blocking the caller.

        Approving a correction used to trigger a full multi-minute refit inline,
        freezing the UI. Predictions keep using the previous model (and the
        exact-match learning_db, which is unaffected) until the new one is ready.
        """
        with self._retrain_lock:
            self._retrain_pending = True
            if self._retrain_thread is not None and self._retrain_thread.is_alive():
                return
            self._retrain_thread = threading.Thread(
                target=self._retrain_worker,
                name="cat-correction-retrain",
                daemon=True,
            )
            self._retrain_thread.start()

    def _retrain_worker(self) -> None:
        import time
        while True:
            # Debounce: sleep, then only proceed if nothing new arrived while
            # we slept. A burst of approvals collapses into a single fit.
            time.sleep(self._RETRAIN_DEBOUNCE_SECS)
            with self._retrain_lock:
                if not self._retrain_pending:
                    self._retrain_thread = None
                    return
                self._retrain_pending = False
            try:
                self._retrain_correction_classifier()
            except Exception as e:
                logger.warning(f"Background correction retrain failed: {e}")

    def _retrain_correction_classifier(self, df=None, fingerprint: str = None):
        if fingerprint is None:
            fingerprint = self._corrections_fingerprint()
        if df is None:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    df = pd.read_sql_query("SELECT name, category FROM category_corrections", conn)
            except Exception:
                return
        if df is None or df.empty or len(df['category'].unique()) < 2:
            return
        try:
            df = df.dropna(subset=["name", "category"]).copy()
            df["name"] = df["name"].astype(str).str.strip()
            df["category"] = df["category"].astype(str).str.strip()
            df = df[(df["name"] != "") & (df["category"] != "")]
            if df.empty or len(df["category"].unique()) < 2:
                return

            df = df.groupby("name", as_index=False)["category"].last()

            # --- drop categories with too few examples for stable fitting ---
            counts = df["category"].value_counts()
            keep_cats = counts[counts >= self._MIN_EXAMPLES_PER_CATEGORY].index
            df = df[df["category"].isin(keep_cats)]
            if df.empty or len(df["category"].unique()) < 2:
                return

            # --- cap total training rows so we never allocate against the
            #     full unbounded correction history (this is what caused
            #     the 19.4 GiB allocation failure) ---
            if len(df) > self._MAX_TRAIN_ROWS:
                # Sample proportionally across categories rather than a flat
                # random sample, so rare-but-kept categories aren't wiped out.
                df = (
                    df.groupby("category", group_keys=False)
                    .apply(lambda g: g.sample(
                        n=max(1, int(len(g) / len(df) * self._MAX_TRAIN_ROWS)),
                        random_state=42,
                    ))
                )
                # Re-check class count survived the proportional sample
                if len(df["category"].unique()) < 2:
                    return

            df['clean_name'] = df['name'].apply(clean_text)
            vectorizer = _sk()["TfidfVectorizer"](
                ngram_range=(1, 2), max_features=3000, dtype=np.float32
            )
            X = vectorizer.fit_transform(df['clean_name'])
            y = df['category']

            # SGDClassifier with log_loss scales to large sample counts and
            # many classes without densifying internally the way saga/OvR
            # LogisticRegression can with class_weight='balanced'.
            # n_jobs=-1 parallelises the one-vs-rest fit across cores. With a
            # few thousand categories that is a few thousand independent binary
            # fits, so this is close to a linear speed-up and does not change
            # the resulting model at all.
            from sklearn.linear_model import SGDClassifier
            classifier = SGDClassifier(
                loss='log_loss',
                class_weight='balanced',
                max_iter=1000,
                tol=1e-3,
                n_jobs=-1,
                random_state=42,
            )
            classifier.fit(X, y)
            # Single assignment — readers see the old model or the new one.
            self._corr_model = (vectorizer, classifier)
            self._save_cached_model(self._corr_model, fingerprint)
        except Exception as e:
            logger.warning(f"Failed to retrain correction classifier: {e}")

    # Cap on rows kept in the corrections table. When exceeded, oldest rows
    # are pruned (by id) after each insert so the table — and therefore
    # future retraining cost — stays bounded indefinitely.
    _MAX_DB_ROWS = 200_000

    def apply_learned_correction(self, name: str, category: str, auto_save=True):
        clean_n = clean_text(name)
        if not clean_n or not category: return
        self.learning_db[clean_n] = category
        if auto_save:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    c = conn.cursor()
                    c.execute("INSERT INTO category_corrections (name, category) VALUES (?, ?)", (clean_n, category))
                    c.execute("SELECT COUNT(*) FROM category_corrections")
                    total = c.fetchone()[0]
                    if total > self._MAX_DB_ROWS:
                        excess = total - self._MAX_DB_ROWS
                        c.execute(
                            "DELETE FROM category_corrections WHERE id IN "
                            "(SELECT id FROM category_corrections ORDER BY id ASC LIMIT ?)",
                            (excess,),
                        )
                    conn.commit()
                self._invalidate_counts()
                self._schedule_retrain()
            except Exception as e:
                logger.warning(f"Failed to save correction to DB: {e}")
        else:
            self._pending_corrections[clean_n] = category

    def add_negative_correction(self, name: str, category: str, reason: str = "", auto_save=False):
        """
        Record that `category` was human-rejected for this product name.
        Negatives are excluded from future suggestions, and products re-listed
        under a known-rejected category are auto-flagged by check_wrong_category.
        """
        clean_n = clean_text(name)
        cat = str(category).strip()
        if not clean_n or not cat or cat.lower() in ('nan', 'none'):
            return
        cat_l = cat.lower()
        bucket = self.negative_db.setdefault(clean_n, set())
        if cat_l in bucket:
            return
        bucket.add(cat_l)
        reason_txt = str(reason or "").strip()
        if reason_txt and reason_txt.lower() not in ('nan', 'none', 'rejected'):
            self.negative_reasons[(clean_n, cat_l)] = reason_txt[:500]
        self._pending_negatives.append((clean_n, cat, reason_txt[:500]))
        if auto_save:
            self.save_learning_db()

    def save_learning_db(self):
        # Only flush corrections queued since the last save (not the whole
        # learning_db, which also holds everything already persisted at load
        # time) — otherwise every batch-approve/reject re-inserts the entire
        # correction history into SQLite each time this is called.
        if not self._pending_corrections and not self._pending_negatives: return
        had_corrections = bool(self._pending_corrections)
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("BEGIN TRANSACTION")
                for name, cat in self._pending_corrections.items():
                    c.execute("INSERT INTO category_corrections (name, category) VALUES (?, ?)", (name, cat))
                for name, cat, reason in self._pending_negatives:
                    c.execute("INSERT INTO category_negatives (name, category, reason) VALUES (?, ?, ?)", (name, cat, reason))
                for table in ("category_corrections", "category_negatives"):
                    c.execute(f"SELECT COUNT(*) FROM {table}")
                    total = c.fetchone()[0]
                    if total > self._MAX_DB_ROWS:
                        excess = total - self._MAX_DB_ROWS
                        c.execute(
                            f"DELETE FROM {table} WHERE id IN "
                            f"(SELECT id FROM {table} ORDER BY id ASC LIMIT ?)",
                            (excess,),
                        )
                conn.commit()
            self._pending_corrections = {}
            self._pending_negatives = []
            self._invalidate_counts()
            if had_corrections:
                self._schedule_retrain()
        except Exception as e:
            logger.warning(f"Failed to batch save learning DB: {e}")

    # ── Admin: inspect / undo learned data ─────────────────────────────────
    # The engine silently reshapes its own suggestions based on what gets
    # written here (predict_category_from_learning / negative exclusion), so
    # a reviewer needs a way to see what was taught and undo a bad entry
    # (e.g. a mis-click that permanently poisons a suggestion) without going
    # into the SQLite file by hand.

    def list_corrections(self, limit: int = 500) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(
                    "SELECT id, name, category, timestamp FROM category_corrections "
                    "ORDER BY id DESC LIMIT ?", conn, params=(limit,),
                )
        except Exception as e:
            logger.warning(f"list_corrections failed: {e}")
            return pd.DataFrame(columns=["id", "name", "category", "timestamp"])

    def list_negatives(self, limit: int = 500) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql_query(
                    "SELECT id, name, category, reason, timestamp FROM category_negatives "
                    "ORDER BY id DESC LIMIT ?", conn, params=(limit,),
                )
        except Exception as e:
            logger.warning(f"list_negatives failed: {e}")
            return pd.DataFrame(columns=["id", "name", "category", "reason", "timestamp"])

    def _invalidate_counts(self) -> None:
        self._counts_cache = None

    def counts(self) -> tuple:
        """Returns (corrections_count, negatives_count) for a quick summary.

        Memoised: the sidebar calls this on every rerun, and these are two
        unindexed COUNT(*) scans over a 200k-row table (~325ms measured on a
        warm rerun — the single largest remaining cost of an idle interaction).
        Every method that writes to either table invalidates the cache, so the
        number a reviewer sees is never stale.
        """
        cached = getattr(self, "_counts_cache", None)
        if cached is not None:
            return cached
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM category_corrections")
                n_corr = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM category_negatives")
                n_neg = c.fetchone()[0]
                self._counts_cache = (n_corr, n_neg)
                return self._counts_cache
        except Exception as e:
            logger.warning(f"counts() failed: {e}")
            return (0, 0)

    def delete_corrections(self, ids: list) -> int:
        """Delete rows by id from category_corrections and refresh in-memory state."""
        if not ids: return 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.executemany("DELETE FROM category_corrections WHERE id = ?", [(i,) for i in ids])
                conn.commit()
            self._invalidate_counts()
        except Exception as e:
            logger.warning(f"delete_corrections failed: {e}")
            return 0
        # Rebuild explicitly rather than via load_learning_db(), which only
        # repopulates learning_db inside `if not df.empty` — so deleting the
        # last remaining correction would leave the stale in-memory dict
        # non-empty even though the table is now empty.
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query("SELECT name, category FROM category_corrections", conn)
            self.learning_db = df.groupby('name')['category'].last().to_dict() if not df.empty else {}
            self._corr_model = None
            if not df.empty:
                # Off the request path — the admin who just deleted a row gets
                # their click back immediately. Until the refit lands, deleted
                # entries are already gone from learning_db (the exact-match
                # path), so the stale model cannot resurrect them there.
                self._schedule_retrain()
        except Exception as e:
            logger.warning(f"delete_corrections: failed to rebuild learning_db: {e}")
        return len(ids)

    def delete_negatives(self, ids: list) -> int:
        """Delete rows by id from category_negatives and refresh in-memory state."""
        if not ids: return 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                c.executemany("DELETE FROM category_negatives WHERE id = ?", [(i,) for i in ids])
                conn.commit()
            self._invalidate_counts()
        except Exception as e:
            logger.warning(f"delete_negatives failed: {e}")
            return 0
        self.negative_db = {}
        self.negative_reasons = {}
        try:
            with sqlite3.connect(self.db_path) as conn:
                ndf = pd.read_sql_query("SELECT name, category, reason FROM category_negatives", conn)
            if not ndf.empty:
                ndf['category_l'] = ndf['category'].astype(str).str.strip().str.lower()
                self.negative_db = {n: set(g) for n, g in ndf.groupby('name')['category_l']}
                self.negative_reasons = {
                    (n, cl): str(r).strip()
                    for n, cl, r in zip(ndf['name'], ndf['category_l'], ndf['reason'].fillna(''))
                    if str(r).strip()
                }
        except Exception as e:
            logger.warning(f"delete_negatives: failed to reload negative_db: {e}")
        return len(ids)

    def build_tfidf_index(self, categories_list: list):
        """Builds a TF-IDF index for category matching."""
        if not categories_list: return
        self.categories = [str(c).strip() for c in categories_list if str(c).strip() and str(c).strip().lower() != 'nan']
        if not self.categories: return

        sep_count = sum(1 for c in self.categories if '/' in c or '>' in c)
        self._index_has_full_paths = (sep_count / max(len(self.categories), 1)) > 0.3

        try:
            self.vectorizer = _sk()["TfidfVectorizer"](ngram_range=(1, 2), stop_words='english')
            self.tfidf_matrix = self.vectorizer.fit_transform(self.categories)
            self._tfidf_built = True
            logger.info(f'[TF-IDF] Built index for {len(self.categories)} categories')
        except Exception as e:
            logger.warning(f"Failed to build TF-IDF index — wrong-category detection disabled for this run: {e}", exc_info=True)

    def predict_category_from_learning(self, name: str) -> str:
        clean_n = clean_text(name)
        neg = self.negative_db.get(clean_n, set())
        if clean_n in self.learning_db:
            cand = self.learning_db[clean_n]
            if str(cand).strip().lower() not in neg:
                return cand
        if self.correction_classifier and self.correction_vectorizer:
            try:
                vec = self.correction_vectorizer.transform([clean_n])
                probs = self.correction_classifier.predict_proba(vec)[0]
                max_prob_idx = np.argmax(probs)
                if probs[max_prob_idx] > 0.6:
                    cand = self.correction_classifier.classes_[max_prob_idx]
                    if str(cand).strip().lower() not in neg:
                        return cand
            except Exception as e:
                logger.debug(f"predict_category_from_learning: classifier prediction failed for {clean_n!r}: {e}")
        return None

    def get_category_with_fallback(self, name: str, kw_map: dict = None, categories_list: list = None) -> str:
        learned = self.predict_category_from_learning(name)
        if learned: return learned
        
        if self._tfidf_built:
            try:
                if hasattr(self, 'vectorizer'):
                    name_vec = self.vectorizer.transform([name])
                    similarities = _sk()["cosine_similarity"](name_vec, self.tfidf_matrix).flatten()
                    best_idx = int(np.argmax(similarities))
                    if similarities[best_idx] > 0.35:
                        return self.categories[best_idx]
            except Exception as e:
                logger.debug(f"get_category_with_fallback: TF-IDF lookup failed for {name!r}: {e}")

        if kw_map:
            name_lower = str(name).lower()
            for kw, cat in kw_map.items():
                if re.search(r'\b' + re.escape(kw) + r'\b', name_lower):
                    return cat
        return ""

    def get_category_with_boost(self, name: str, top_n: int = 20) -> str:
        """
        Gets the top N TF-IDF predictions, applies internal JSON heuristic boosts, 
        and returns the highest scoring category.
        """
        learned = self.predict_category_from_learning(name)
        if learned: 
            return learned

        if not getattr(self, '_tfidf_built', False):
            return ""
        
        try:
            name_clean = clean_text(name)
            name_vec = self.vectorizer.transform([name_clean])
            similarities = _sk()["cosine_similarity"](name_vec, self.tfidf_matrix).flatten()
            
            top_indices = similarities.argsort()[-top_n:][::-1]
            
            best_category = ""
            best_score = -1.0
            name_lower = str(name).lower()
            
            _neg = self.negative_db.get(name_clean, set())
            for idx in top_indices:
                cat_path = self.categories[idx]
                base_score = float(similarities[idx])
                boost = 0.0

                # 1. Convert the engine's category path to lowercase
                cat_path_lower = cat_path.lower()

                # Never re-suggest a category a human already rejected for this name
                if _neg and (cat_path_lower.strip() in _neg
                             or cat_path_lower.split('>')[-1].strip() in _neg
                             or cat_path_lower.split('/')[-1].strip() in _neg):
                    continue
                
                # 2. Check the JSON rules — try full path first, then leaf name as fallback.
                #    This handles both: rules keyed by full path ("automobile > car care > car polishes & waxes")
                #    and rules keyed by bare category_name ("car polishes & waxes").
                leaf_lower = cat_path_lower.split('>')[-1].strip()
                rule = self.compiled_rules.get(cat_path_lower) or self.compiled_rules.get(leaf_lower)
                if rule:
                    matches = rule['pattern'].findall(name_lower)
                    if matches:
                        boost = sum(rule['weights'].get(m.lower(), 0.0) for m in set(matches))
                
                final_score = base_score + (boost * 0.6) # Increased multiplier slightly for more authority
                
                if final_score > best_score:
                    best_score = final_score
                    best_category = cat_path
            
            # Reject garbage matches if confidence is too low.
            # Bare-leaf indexes score much lower than full-path indexes,
            # so use a lower threshold when we know the index is bare leaves.
            _threshold = 0.35 if getattr(self, '_index_has_full_paths', True) else 0.15
            if best_score < _threshold:
                return ""
                
            return best_category
            
        except Exception as e:
            logger.warning(f"Boosted prediction failed: {e}")
            return ""

    def batch_predict_categories(self, names: list, top_n: int = 20) -> list:
        """Predict categories for ALL names at once using semantic embeddings."""
        n = len(names)
        results = [''] * n

        pending_indices = []
        for i, name in enumerate(names):
            learned = self.predict_category_from_learning(name)
            if learned:
                results[i] = learned
            else:
                pending_indices.append(i)

        if not pending_indices or not self._tfidf_built:
            return results

        pending_names = [names[i] for i in pending_indices]
        try:
            name_vectors = self.vectorizer.transform(pending_names)
            sim_matrix = _sk()["cosine_similarity"](name_vectors, self.tfidf_matrix)
        except Exception as e:
            logger.warning(f"Batch prediction failed: {e}")
            return results

        threshold = 0.35

        for j, orig_idx in enumerate(pending_indices):
            similarities = sim_matrix[j]
            top_indices = similarities.argsort()[-top_n:][::-1]

            best_category = ""
            best_score = -1.0
            name_lower = names[orig_idx].lower()
            _neg = self.negative_db.get(clean_text(names[orig_idx]), set()) if self.negative_db else set()

            for idx in top_indices:
                base_score = float(similarities[idx])
                cat_path = self.categories[idx]
                boost = 0.0

                cat_path_lower = cat_path.lower()
                # Never re-suggest a category a human already rejected for this name
                if _neg and (cat_path_lower.strip() in _neg
                             or cat_path_lower.split('>')[-1].strip() in _neg
                             or cat_path_lower.split('/')[-1].strip() in _neg):
                    continue
                leaf_lower = cat_path_lower.split('>')[-1].strip()
                rule = self.compiled_rules.get(cat_path_lower) or self.compiled_rules.get(leaf_lower)
                if rule:
                    matches = rule['pattern'].findall(name_lower)
                    if matches:
                        boost = sum(rule['weights'].get(m.lower(), 0.0) for m in set(matches))

                final_score = base_score + (boost * 0.4) # Slightly lower boost for semantic matches
                if final_score > best_score:
                    best_score = final_score
                    best_category = cat_path

            if best_score >= threshold:
                results[orig_idx] = best_category

        return results

    def build_keyword_to_category_mapping(self) -> dict:
        kw_map = {}
        for cat in self.categories:
            parts = [p.strip().lower() for p in cat.split('>')]
            if len(parts) > 1:
                kw_map[parts[-1]] = cat
        return kw_map


_engine_instance = None

def get_engine(db_path="cat_learning.db"):
    global _engine_instance
    if _engine_instance is None:
        try:
            _engine_instance = CategoryMatcherEngine(db_path)
        except Exception as e:
            logger.error(f"Failed to initialize CategoryMatcherEngine: {e}")
            logger.error(traceback.format_exc())
            _engine_instance = None
    return _engine_instance

def check_wrong_category(data: pd.DataFrame, categories_list: list, compiled_rules: dict = None, cat_path_to_code: dict = None, code_to_path: dict = None, confidence_threshold: float = 0.0):
    if not {'NAME', 'CATEGORY'}.issubset(data.columns) or not categories_list:
        return pd.DataFrame(columns=data.columns)
        
    engine = get_engine()
    if engine is None:
        return pd.DataFrame(columns=data.columns)

    # If categories_list is bare leaf names (no path separators), the TF-IDF
    # index will produce near-zero similarities for most product names.
    # Prefer full paths from code_to_path if available.
    _effective_cats = categories_list
    if code_to_path:
        full_paths = list(code_to_path.values())
        sep_ratio = sum(1 for p in full_paths if '/' in p or '>' in p) / max(len(full_paths), 1)
        if sep_ratio > 0.3:
            _effective_cats = full_paths
            logger.info(f'[WrongCat] Using code_to_path full paths '
                        f'({len(_effective_cats)}) instead of categories_list '
                        f'({len(categories_list)}) for TF-IDF index')

    if not engine._tfidf_built:
        engine.build_tfidf_index(_effective_cats)
    elif not getattr(engine, '_index_has_full_paths', False) and _effective_cats is not categories_list:
        # Index was built on bare leaves (or before _index_has_full_paths was added)
        # but we now have full paths available — rebuild for better accuracy.
        logger.info('[WrongCat] Rebuilding TF-IDF index with full paths')
        engine._tfidf_built = False
        engine.build_tfidf_index(_effective_cats)

    # Guard: bare-leaf TF-IDF produces too many false positives to be useful.
    # Category leaf names like 'Cases', 'High-top', 'Leather' are too short
    # and ambiguous to reliably match against product names via cosine similarity.
    # Only run wrong-category detection when we have full paths (from category_map.xlsx).
    if not getattr(engine, '_index_has_full_paths', False):
        logger.warning(
            '[WrongCat] Skipping wrong-category detection: TF-IDF index was built '
            'on bare leaf category names (no category_map.xlsx full paths available). '
            'Ensure category_map.xlsx is present and loading correctly.'
        )
        return pd.DataFrame(columns=data.columns)

    # CRITICAL: Feed the engine the JSON rules so it can use them!
    if compiled_rules:
        engine.set_compiled_rules(compiled_rules)

    if cat_path_to_code is None: cat_path_to_code = {}
    if code_to_path is None: code_to_path = {}

    d = data.copy()
    d['_cat_clean'] = d['CATEGORY'].astype(str).str.strip()
    
    if 'CATEGORY_CODE' in d.columns and code_to_path:
        invalid_mask = d['_cat_clean'].fillna('').str.lower().isin({'', 'nan', 'none', 'miscellaneous'})
        if invalid_mask.any():
            codes = d.loc[invalid_mask, 'CATEGORY_CODE'].astype(str).str.strip().str.split('.').str[0]
            d.loc[invalid_mask, '_cat_clean'] = codes.map(code_to_path).fillna(d.loc[invalid_mask, '_cat_clean'])

    d['_cat_lower'] = d['_cat_clean'].str.lower()
    d['_name_clean'] = d['NAME'].astype(str).str.strip()
    
    # Pre-build a leaf->full_path cache so each product row doesn't scan all paths
    leaf_to_full_path = {}
    if code_to_path:
        for full_path in code_to_path.values():
            for sep in ('/', '>'):
                if sep in full_path:
                    leaf = full_path.split(sep)[-1].strip().lower()
                    break
            else:
                leaf = full_path.strip().lower()
            # First match wins — most specific path for this leaf
            if leaf not in leaf_to_full_path:
                leaf_to_full_path[leaf] = full_path

    flagged_indices = []
    comment_map = {}
    kw_map = engine.build_keyword_to_category_mapping()

    # ── Resolution diagnostics (logged once per run) ──────────────────────────
    _diag_logged = 0
    _has_code_col = 'CATEGORY_CODE' in d.columns
    logger.info(f'[WrongCat] code_to_path size={len(code_to_path)}, '
                f'cat_path_to_code size={len(cat_path_to_code)}, '
                f'leaf_cache size={len(leaf_to_full_path)}, '
                f'has_code_col={_has_code_col}')

    # ── BATCH PREDICT: single TF-IDF transform + cosine_similarity ────────────
    all_indices = d.index.tolist()
    all_names = d['_name_clean'].tolist()
    all_cats = d['_cat_clean'].tolist()
    all_cat_codes = d['CATEGORY_CODE'].astype(str).str.strip().tolist() if 'CATEGORY_CODE' in d.columns else [''] * len(all_indices)
    batch_predictions = engine.batch_predict_categories(all_names)

    # Keyword-map fallback for empty predictions
    if kw_map:
        sorted_kws = sorted(kw_map.keys(), key=len, reverse=True)
        kw_pattern_str = r'\b(' + '|'.join(re.escape(kw) for kw in sorted_kws) + r')\b'
        compiled_kw_regex = re.compile(kw_pattern_str)
        for i, pred in enumerate(batch_predictions):
            if not pred:
                name_lower = all_names[i].lower()
                match = compiled_kw_regex.search(name_lower)
                if match:
                    batch_predictions[i] = kw_map[match.group(1)]

    def _get_last_segments(path, n=2):
        """Return the last n segments of a category path, lowercased and joined."""
        for sep in ('/', '>'):
            if sep in path:
                parts = [p.strip().lower() for p in path.split(sep)]
                return ' '.join(parts[-n:])
        return path.strip().lower()

    def _get_top(path):
        """Return the top-level category segment, handling / and > separators."""
        for sep in ('/', '>'):
            if sep in path:
                return path.split(sep)[0].strip().lower()
        return path.strip().lower()

    def _get_leaf(path):
        """Return the leaf segment, handling / and > separators."""
        for sep in ('/', '>'):
            if sep in path:
                return path.split(sep)[-1].strip().lower()
        return path.strip().lower()

    def _get_segments(path, n):
        """Return the first n segments of a path as a lowercase tuple."""
        for sep in ('/', '>'):
            if sep in path:
                parts = [p.strip().lower() for p in path.split(sep)]
                return tuple(parts[:n])
        return (path.strip().lower(),)

    _TAIL_STOP = {'and', 'or', 'of', 'for', 'the', 'a', 'an', 'in', 'to',
                  'with', 'by', 'at', 'from', 'on', 'is', 'are', 'was', 'be',
                  'as', 'it', 'non', 'amp', 'new', 'set', 'pack'}
    _MIN_TOKEN_LEN = 4

    # Negative learning: clean names computed once, only when negatives exist.
    _neg_db = engine.negative_db
    _all_clean = [clean_text(n) for n in all_names] if _neg_db else None

    for row_i in range(len(all_indices)):
        idx = all_indices[row_i]
        current_cat = all_cats[row_i]
        name = all_names[row_i]

        if not current_cat or current_cat.lower() in ('nan', 'none', ''):
            continue

        # A human previously rejected this exact (name, category) pairing —
        # flag immediately with the stored reason, regardless of TF-IDF score.
        if _neg_db:
            _neg = _neg_db.get(_all_clean[row_i])
            if _neg:
                _cur_l = current_cat.strip().lower()
                _cur_leaf = _get_leaf(current_cat)
                if _cur_l in _neg or _cur_leaf in _neg:
                    _rsn = (engine.negative_reasons.get((_all_clean[row_i], _cur_l))
                            or engine.negative_reasons.get((_all_clean[row_i], _cur_leaf)))
                    flagged_indices.append(idx)
                    comment_map[idx] = (
                        f"Category '{current_cat}' was previously rejected for this product"
                        + (f" — {_rsn}" if _rsn else "")
                    )
                    continue

        if 'miscellaneous' in current_cat.lower():
            flagged_indices.append(idx)
            comment_map[idx] = "Category is 'Miscellaneous'"
            continue

        predicted = batch_predictions[row_i]

        # If predicted is a bare leaf name, resolve it to its full path via
        # code_to_path so segment comparison works correctly.
        if predicted and code_to_path and '/' not in predicted and '>' not in predicted:
            predicted = leaf_to_full_path.get(predicted.strip().lower(), predicted)
        
        if predicted and predicted.lower() != current_cat.lower():
            p_leaf = _get_leaf(predicted)
            c_leaf = _get_leaf(current_cat)

            # Skip if the current leaf already appears anywhere in the predicted path
            # e.g. current='Bluetooth Speakers', predicted='Electronics / Audio / Bluetooth Speakers'
            if c_leaf in predicted.lower():
                continue

            # Resolve current category to its full path using code_to_path so we
            # can compare top-level parents.
            # e.g. current='Smart Watches' -> 'Phones & Tablets / ... / Smart Watches'
            current_full = current_cat
            _resolution_method = 'unresolved'
            if code_to_path:
                # 1. Try resolving via CATEGORY_CODE directly (most reliable)
                row_code = all_cat_codes[row_i].split('.')[0]
                if row_code and row_code in code_to_path:
                    current_full = code_to_path[row_code]
                    _resolution_method = f'code({row_code})'
                else:
                    # 2. Try cat_path_to_code lookup
                    code = cat_path_to_code.get(current_cat.lower(), '')
                    if code and code in code_to_path:
                        current_full = code_to_path[code]
                        _resolution_method = f'cat_path_to_code({code})'
                    else:
                        # 3. Use pre-built leaf cache
                        resolved = leaf_to_full_path.get(current_cat.strip().lower())
                        if resolved:
                            current_full = resolved
                            _resolution_method = 'leaf_cache'
                        # else stays as bare leaf — log it
            if _diag_logged < 10:
                logger.info(f'[WrongCat] resolution: cat={current_cat!r} '
                            f'row_code={all_cat_codes[row_i]!r} '
                            f'method={_resolution_method} '
                            f'current_full={current_full!r}')
                _diag_logged += 1

            # ── Segment-similarity suppression ────────────────────────────────────
            # Suppress if both paths share at least 2 leading segments.
            # e.g. 'Phones & Tablets / Accessories / Smart Watches' vs
            #      'Phones & Tablets / Accessories / Smart Watch Cables'
            # → share 2 levels → suppress (same sub-family, not a wrong category).
            p_segs = _get_segments(predicted, 3)
            c_segs = _get_segments(current_full, 3)
            shared = sum(1 for a, b in zip(p_segs, c_segs) if a == b)
            if shared >= min(2, len(p_segs), len(c_segs)):
                continue

            # Pre-compute leaf/top values used by both suppression checks below
            c_leaf_lower = current_cat.strip().lower()
            c_full_lower = current_full.strip().lower()
            p_top_lower = _get_top(predicted).strip().lower()

            # ── Same-domain suppression ───────────────────────────────────────────
            # When code_to_path can't resolve a bare leaf name to its full path,
            # the segment check can't fire. This dict maps top-level domain names
            # to their known sub-category leaf names so we can suppress same-domain
            # false positives without needing code_to_path at all.
            same_domain_cats = _SAME_DOMAIN_CATEGORIES.get(p_top_lower, set())
            if c_leaf_lower in same_domain_cats:
                continue

            # ── Cross-domain noise suppression ────────────────────────────────────
            # Some product names contain incidental words (colors, materials, feature
            # keywords) that pull TF-IDF toward completely unrelated domains.
            # Block known noisy cross-domain leaps here.
            blocked = False
            for current_kws, forbidden_tops in _CROSS_DOMAIN_BLOCKS:
                # Check if current category matches this block's domain
                if any(kw in c_leaf_lower or kw in c_full_lower for kw in current_kws):
                    if any(p_top_lower.startswith(ft) for ft in forbidden_tops):
                        blocked = True
                        break
            if blocked:
                continue

            # ── Books domain exemption ────────────────────────────────────────────
            # Never flag products in the Books / Movies & Music domain — titles are
            # intentionally broad and TF-IDF picks up incidental topic words.
            _BOOKS_DOMAIN_PREFIXES = ('books', 'movies', 'music', 'books, movies')
            c_full_top = current_full.strip().lower().split('/')[0].strip() if '/' in current_full else \
                         current_full.strip().lower().split('>')[0].strip()
            if any(c_full_top.startswith(bp) for bp in _BOOKS_DOMAIN_PREFIXES):
                continue

            # ── Name-keyword-in-last-two-levels suppression ───────────────────────
            # If key words from the product name overlap with the last 1-2 segments
            # of the CURRENT category path, the product is correctly placed — suppress.
            #
            # Uses both exact token match AND substring match (handles minor typos /
            # plurals like "wax"->"waxes", "speaker"->"speakers", "polish"->"polishes").
            #
            # e.g. name="Car Wax Polish Paste 300g"
            #      current=".../Car Polishes & Waxes"  -> "polish" in "polishes" -> suppress
            # e.g. name="v622 hi-fi multimedia xbox speaker system"
            #      current=".../Speakers / Subwoofers" -> "speaker" in "speakers" -> suppress
            # e.g. name="800g Flour miller-Kisiagi"
            #      current=".../Sanders & Grinders / Power Grinders" -> no overlap -> FLAG
            _last_two = _get_last_segments(current_full, 2)
            _name_tokens = {t for t in re.sub(r'[^a-z0-9\s]', ' ', name.lower()).split()
                            if len(t) >= _MIN_TOKEN_LEN} - _TAIL_STOP
            _cat_tail_tokens = {t for t in re.sub(r'[^a-z0-9\s]', ' ', _last_two).split()
                                 if len(t) >= _MIN_TOKEN_LEN} - _TAIL_STOP

            _name_fits = False
            if _name_tokens and _cat_tail_tokens:
                # 1. Exact token overlap
                if _name_tokens & _cat_tail_tokens:
                    _name_fits = True
                else:
                    # 2. Substring match — handles plurals/typos
                    for _ct in _cat_tail_tokens:
                        for _nt in _name_tokens:
                            if _ct in _nt or _nt in _ct:
                                _name_fits = True
                                break
                        if _name_fits:
                            break

            if _name_fits:
                continue

            if p_leaf != c_leaf:
                flagged_indices.append(idx)
                comment_map[idx] = f"Wrong Category. Suggested: {predicted}"

    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)

    results_df = data.loc[flagged_indices].copy()
    results_df['Comment_Detail'] = results_df.index.map(comment_map)
    # Map the suggested category for display in the UI (dict lookup instead of a
    # list.index() scan per row, which was O(n^2) over large flagged sets)
    _idx_to_prediction = dict(zip(all_indices, batch_predictions))
    results_df['Suggested_Category'] = [_idx_to_prediction[i] for i in results_df.index]
    
    return results_df[data.columns.tolist() + ['Comment_Detail', 'Suggested_Category']].drop_duplicates(subset=['PRODUCT_SET_SID'])
