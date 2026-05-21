"""
main.py - Main Streamlit Application Entry Point
"""

import base64
import concurrent.futures
import hashlib
import json
import logging
import os
import pickle
import re
import shutil
import time
import traceback
import zipfile
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import requests
import st_yled
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



# ── Shared Image Fetching Session ──
_IMAGE_SESSION: Optional[requests.Session] = None


def get_image_session() -> requests.Session:
    global _IMAGE_SESSION
    if _IMAGE_SESSION is None:
        s = requests.Session()
        retry = Retry(total=2, backoff_factor=0.3, status_forcelist=[500, 502, 503])
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=50,  # keep 50 TCP connections alive
            pool_maxsize=100,  # up to 100 concurrent
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "image/*"})
        _IMAGE_SESSION = s
    return _IMAGE_SESSION


# ── Pre-compiled Regex Patterns ──
_RE_HTML_TAGS = re.compile(r"<[a-zA-Z/][^>]*>")
_RE_SPECIAL_CHARS = re.compile(r"[^\x00-\x7F★✓•®™]|[!@#$%^&*()]{3,}")
_RE_MODEL_NUMBER = re.compile(r"[A-Z0-9]{2,}[0-9]{2,}|[0-9]{2,}[A-Z]{2,}", re.I)
_RE_SIZE_TYPE = re.compile(r"\b(EU|UK|US|FR|CM|KE)\b", re.I)
_RE_BRAND_REPEAT = re.compile(r"\b(brand|by|from)\b", re.I)

# ──────────────────────────────────────────────────────────────────────────────
from api_client import (
    get_summary_metrics,
    invalidate,
    register_direct_pipeline,
    validate_and_load,
)

# ── NEW MODULAR IMPORTS ───────────────────────────────────────────────────────
from constants import (
    COUNTRY_VALIDATOR_CONFIG,
    FLAG_CACHE_DIR,
    GRID_COLS,
    JUMIA_COLORS,
    PARQUET_CACHE_DIR,
    REASON_MAP,
)
from data_utils import (
    _detect_and_read_csv,
    _get_image_from_zip,
    _repair_mojibake,
    clean_category_code,
    create_match_key,
    create_match_key_vectorized,
    df_hash,
    filter_by_country,
    load_df_parquet,
    normalize_text,
    propagate_metadata,
    save_df_parquet,
    standardize_input_data,
    validate_input_schema,
)
from ghana_rules import check_ghana_smart_glasses, load_ghana_qc_rules
from loaders import compile_regex_patterns, load_support_files_lazy
from morocco_rules import check_morocco_prohibited_brands, load_morocco_qc_rules
from nigeria_rules import (
    check_nigeria_apple,
    check_nigeria_books,
    check_nigeria_gift_card,
    check_nigeria_hp_toners,
    check_nigeria_powerbanks,
    check_nigeria_rice,
    check_nigeria_tvs,
    check_nigeria_xmas_tree,
    load_nigeria_qc_rules,
)
from pricing_rules import (
    CATEGORY_MAX_PRICES_USD,
    check_category_max_price,
    check_suspicious_discount,
    check_wrong_price,
)
from translations import LANGUAGES, get_translation
from ui_components import (
    apply_status_change,
    flag_pill_header,
    render_exports_section,
    render_flag_expander,
    render_image_grid,
    render_rejection_donut,
    render_summary_header,
)

# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL STYLES & BRANDING (Javascript Injector for targeted styling)
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="dashboard-marker" style="display:none;"></div>
    <script>
    function colorExpander() {
        const markers = document.querySelectorAll('.dashboard-marker');
        markers.forEach(marker => {
            // Streamlit wraps each element in an element-container div.
            // The marker and the expander are siblings at that level.
            let container = marker.closest('[data-testid="element-container"]');
            if (!container) return;
            let next = container.nextElementSibling;
            // Skip any non-expander siblings (e.g. empty containers)
            while (next && !next.querySelector('[data-testid="stExpander"]')) {
                next = next.nextElementSibling;
            }
            if (!next) return;
            let expander = next.querySelector('[data-testid="stExpander"]');
            if (!expander) return;
            let summary = expander.querySelector('summary');
            if (summary && summary.style.backgroundColor !== 'rgb(246, 139, 30)') {
                summary.style.setProperty('background-color', '#f68b1e', 'important');
                summary.style.setProperty('color', 'white', 'important');
                summary.style.setProperty('border-radius', '8px', 'important');
                summary.style.setProperty('margin-bottom', '4px', 'important');
                let p = summary.querySelector('p');
                if (p) {
                    p.style.setProperty('color', 'white', 'important');
                    p.style.setProperty('font-weight', '800', 'important');
                    p.style.setProperty('font-size', '1.05rem', 'important');
                }
                let svg = summary.querySelector('svg');
                if (svg) svg.style.setProperty('fill', 'white', 'important');
            }
        });
    }
    setTimeout(colorExpander, 300);
    setTimeout(colorExpander, 800);
    setInterval(colorExpander, 2000);
    </script>
""",
    unsafe_allow_html=True,
)
# ──────────────────────────────────────────────────────────────────────────────

PREFETCH_MAP = {
    # Native validator flags (unchanged)
    "wrong_category": "Wrong Category",
    "poor_images": "Poor images",
    "restricted_brands": "Restricted brands",
    "prohibited_products": "Prohibited products",
    "suspected_fake": "Suspected Fake product",
    "duplicate_product": "Duplicate product",
    "wrong_variation": "Wrong Variation",
    "missing_color": "Missing COLOR",
    "unnecessary_words": "Unnecessary words in NAME",
    "brand_repeated": "BRAND name repeated in NAME",
    "generic_brand": "Generic BRAND Issues",
    "incomplete_smartphone": "Incomplete Smartphone Name",
    "missing_weight": "Missing Weight/Volume",
    "product_warranty": "Product Warranty",
    # Prefetch-only flags — each ZIP column gets its own distinct flag
    "category_check": "Category Check",
    "warranty_check": "Warranty Check",
    "fda_check": "FDA",
    "color_check": "Color Check",
    "variation_check": "Variation Check",
    "product_name_brand_name": "Product Name Brand Name",
    "title_language_check": "Title Language Check",
    "image_quality_check": "Image Quality Check",
    "brand_image_check": "Brand Image Check",
}

PREFETCH_REASON_COLUMNS = {
    "category_check": ["Category_Check_Rejection_Reason"],
    "warranty_check": ["Warranty_Rejection_Reason"],
    "fda_check": ["FDA_Rejection_Reason"],
    "color_check": ["Color_Rejection_Reason"],
    "variation_check": ["Variation_Rejection_Reason"],
    "product_name_brand_name": [
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "title_language_check": ["Title_Language_Check_Reason"],
    "image_quality_check": ["Image_Quality_Check_Reason"],
    "brand_image_check": ["Brand_Image_Check_Reason"],
}
PROCESSING_CACHE_VERSION = "prefetch_context_v3"
PREFETCH_VALIDATOR_SKIP_MAP = {
    "category_check": ["Wrong Category", "Category Check"],
    "warranty_check": ["Product Warranty", "Warranty Check"],
    "fda_check": ["FDA"],
    "color_check": ["Missing COLOR", "Color Check"],
    "variation_check": ["Wrong Variation", "Variation Check"],
    "product_name_brand_name": ["BRAND name repeated in NAME", "Product Name Brand Name"],
    "brand_image_check": ["Brand Image Check"],
    "title_language_check": ["Missing Weight/Volume", "Title Language Check"],
    "image_quality_check": [
        "Poor images",
        "Image Quality Check",
        "Image Stretched",
        "Image Blurry",
        "Image Mismatch",
        "Image Infringing",
        "Image Too Many things displayed",
    ],
}


def _prefetch_key_from_status_col(col: str) -> str:
    return (
        re.sub(r"[_\s]*status$", "", str(col), flags=re.IGNORECASE)
        .strip()
        .lower()
        .replace(" ", "_")
    )


def _build_zip_sid_index(qc_df: pd.DataFrame) -> None:
    """Build session-state lookup tables so ui_components can show prefetch warnings on cards."""
    if qc_df.empty:
        return
    for possible in ("cod_productset_sid", "PRODUCT_SET_SID", "ProductSetSid", "SID"):
        if possible in qc_df.columns:
            st.session_state["_zip_sid_index"] = qc_df.set_index(
                qc_df[possible].astype(str).str.strip()
            )
            break
    status_cols = [c for c in qc_df.columns if "status" in c.lower()]
    st.session_state["_zip_status_cols"] = status_cols
    st.session_state["_zip_prefetch_map"] = {
        col: PREFETCH_MAP.get(_prefetch_key_from_status_col(col),
                              col.replace("_Status", "").replace("_", " ").title())
        for col in status_cols
    }


def _prefetch_reason_from_row(row, status_col: str, qc_columns) -> str:
    base_key = _prefetch_key_from_status_col(status_col)
    for candidate in PREFETCH_REASON_COLUMNS.get(base_key, []):
        if candidate in qc_columns:
            val = str(row.get(candidate, "")).strip()
            if val and val.lower() not in ("nan", "none", "rejected"):
                return val

    reason_col = re.sub(r"status$", "reason", str(status_col), flags=re.IGNORECASE)
    for candidate in (
        reason_col,
        reason_col.replace("_Status", "_Reason"),
        reason_col.replace("_status", "_reason"),
    ):
        if candidate in qc_columns:
            val = str(row.get(candidate, "")).strip()
            if val and val.lower() not in ("nan", "none", "rejected"):
                return val
    return ""


def _derive_prefetched_skip_list(qc_df: pd.DataFrame) -> List[str]:
    skip = set()
    if qc_df.empty:
        return []
    status_cols = [c for c in qc_df.columns if "status" in str(c).lower()]
    for col in status_cols:
        skip.update(
            PREFETCH_VALIDATOR_SKIP_MAP.get(_prefetch_key_from_status_col(col), [])
        )
    if "Duplicate_Flag" in qc_df.columns:
        skip.add("Duplicate product")
    return sorted(skip)


def restore_single_item(sid):
    fr = st.session_state.final_report
    sid_str = str(sid).strip()
    mask = fr["ProductSetSid"].astype(str).str.strip() == sid_str
    if not mask.any():
        return

    # 🚀 Track manually undone reasons to allow sequential rejections
    if "manual_undone_tracker" not in st.session_state:
        st.session_state.manual_undone_tracker = {}

    if len(st.session_state.manual_undone_tracker) > 200:
        # evict oldest 50%
        keys = list(st.session_state.manual_undone_tracker.keys())
        for k in keys[:100]:
            del st.session_state.manual_undone_tracker[k]

    current_flag = fr.loc[mask, "FLAG"].iloc[0]
    st.session_state.manual_undone_tracker.setdefault(sid_str, set()).add(current_flag)

    # Check for other rejections in ZIP
    qc_zip = st.session_state.get("zip_qc_results", pd.DataFrame())
    if not qc_zip.empty:
        sid_col = None
        for possible in [
            "PRODUCT_SET_SID",
            "ProductSetSid",
            "Product Set SID",
            "cod_productset_sid",
            "SID",
        ]:
            if possible in qc_zip.columns:
                sid_col = possible
                break

        if sid_col:
            zip_row = qc_zip[qc_zip[sid_col].astype(str).str.strip() == sid_str]
            if not zip_row.empty:
                r = zip_row.iloc[0]
                status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                fmap = st.session_state.support_files.get("flags_mapping", {})

                for col in status_cols:
                    if str(r[col]).lower() in ("rejected", "1", "yes", "true"):
                        col_key = _prefetch_key_from_status_col(col)
                        flag = PREFETCH_MAP.get(
                            col_key, col_key.replace("_", " ").title()
                        )
                        flag_prefetched = f"{flag} (Prefetched)"

                        if (
                            flag_prefetched
                            not in st.session_state.manual_undone_tracker[sid_str]
                        ):
                            # 🚀 Found another rejection! Switch to it instead of approving.
                            mapped_info = fmap.get(flag, {})
                            reason_code = mapped_info.get(
                                "reason", "1000007 - Other Reason"
                            )
                            default_cmt = mapped_info.get("comment", "Rejected")

                            # Specific reason columns logic
                            zip_cmt = _prefetch_reason_from_row(r, col, qc_zip.columns)
                            final_comment = (
                                zip_cmt
                                if (
                                    zip_cmt
                                    and zip_cmt.lower() not in ("rejected", "nan")
                                )
                                else default_cmt
                            )

                            apply_status_change(
                                [sid_str],
                                status="Rejected",
                                reason=reason_code,
                                comment=final_comment,
                                flag=flag_prefetched,
                                is_manual=True,
                                is_zip=True,
                            )
                            st.session_state.main_toasts.append(
                                f"Product still rejected for: {flag}"
                            )
                            return

    # If no other reasons, Approve
    apply_status_change(
        [sid_str],
        status="Approved",
        reason="",
        comment="",
        flag="Approved by User",
        is_manual=True,
        is_zip=False,
    )
    st.session_state.main_toasts.append("Product Approved.")


try:
    from postqc import (
        detect_file_type,
        load_category_map,
        normalize_post_qc,
        render_post_qc_section,
    )
    from postqc import run_checks as run_post_qc_checks
except ImportError:
    pass

try:
    import _preqc_registry as _reg
except ImportError:
    _reg = None

try:
    from jumia_scraper import COUNTRY_BASE_URLS as _SCRAPER_URLS
    from jumia_scraper import enrich_post_qc_df

    _SCRAPER_AVAILABLE = True
except ImportError:
    _SCRAPER_AVAILABLE = False

# ── Category Matcher Engine ───────────────────────────────────────────────────
try:
    from category_matcher_engine import (
        CategoryMatcherEngine,
        check_wrong_category,
        get_engine,
    )

    _CAT_MATCHER_AVAILABLE = True
except ImportError:
    _CAT_MATCHER_AVAILABLE = False

    def check_wrong_category(
        data,
        categories_list=None,
        cat_path_to_code=None,
        code_to_path=None,
        confidence_threshold=0.0,
    ):
        if "CATEGORY" not in data.columns:
            return pd.DataFrame(columns=data.columns)
        flagged = data[
            data["CATEGORY"]
            .astype(str)
            .str.contains("miscellaneous", case=False, na=False)
        ].copy()
        if not flagged.empty:
            flagged["Comment_Detail"] = "Category contains 'Miscellaneous'"
        return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


@st.cache_resource(show_spinner=False)
def _get_cat_matcher_engine():
    if not _CAT_MATCHER_AVAILABLE:
        return None
    try:
        return get_engine()
    except Exception as e:
        logging.warning("CategoryMatcherEngine init failed: %s", e)
        return None


logger = logging.getLogger(__name__)

# -------------------------------------------------
# CACHE HELPERS
# -------------------------------------------------
os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
os.makedirs(FLAG_CACHE_DIR, exist_ok=True)


def prune_cache_dir(directory: str, max_files: int = 500, max_age_days: int = 7):
    """
    Remove files older than max_age_days and cap the total count to max_files.
    Handles both .pkl and .parquet extensions.
    """
    now = time.time()
    try:
        patterns = ["*.pkl", "*.parquet"]
        files = []
        for p in patterns:
            files.extend(list(Path(directory).glob(p)))

        # 1. Age-based eviction
        for f in files:
            if (now - f.stat().st_mtime) > max_age_days * 86400:
                f.unlink(missing_ok=True)

        # 2. Count-based eviction (oldest first)
        remaining = []
        for p in patterns:
            remaining.extend(list(Path(directory).glob(p)))
        remaining.sort(key=os.path.getmtime)

        for f in remaining[:-max_files]:
            f.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Cache pruning failed for {directory}: {e}")


prune_cache_dir(FLAG_CACHE_DIR)
prune_cache_dir(PARQUET_CACHE_DIR)





class CountryValidator:
    COUNTRY_CONFIG = COUNTRY_VALIDATOR_CONFIG

    def __init__(self, country: str):
        self.country = country
        self.config = self.COUNTRY_CONFIG.get(country, self.COUNTRY_CONFIG["Kenya"])
        self.code = self.config["code"]
        self.skip_validations = self.config["skip_validations"]

    def should_skip_validation(self, validation_name: str) -> bool:
        return validation_name in self.skip_validations

    def ensure_status_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if not df.empty and "Status" not in df.columns:
            df["Status"] = "Approved"
        return df


# -------------------------------------------------
# CACHE-AWARE VALIDATION CHECKS (STANDARD)
# -------------------------------------------------
FLAG_RELEVANT_COLS = {
    "Wrong Category": ["NAME", "CATEGORY", "CATEGORY_CODE"],
    "Restricted brands": ["NAME", "BRAND", "SELLER_NAME", "CATEGORY_CODE"],
    "Suspected Fake product": [
        "CATEGORY_CODE",
        "BRAND",
        "GLOBAL_SALE_PRICE",
        "GLOBAL_PRICE",
    ],
    "Seller Not approved to sell Refurb": [
        "PRODUCT_SET_SID",
        "CATEGORY_CODE",
        "SELLER_NAME",
        "NAME",
    ],
    "Product Warranty": ["PRODUCT_WARRANTY", "WARRANTY_DURATION", "CATEGORY_CODE"],
    "Seller Approve to sell books": ["CATEGORY_CODE", "SELLER_NAME"],
    "Seller Approved to Sell Perfume": [
        "CATEGORY_CODE",
        "SELLER_NAME",
        "BRAND",
        "NAME",
    ],
    "Counterfeit Sneakers": ["CATEGORY_CODE", "NAME", "BRAND"],
    "Suspected counterfeit Jerseys": ["CATEGORY_CODE", "NAME", "SELLER_NAME"],
    "Suspected Fake Perfume": ["CATEGORY_CODE", "NAME", "BRAND"],
    "Unnecessary words in NAME": ["NAME"],
    "Single-word NAME": ["CATEGORY_CODE", "NAME"],
    "Generic BRAND Issues": ["CATEGORY_CODE", "BRAND"],
    "Fashion brand issues": ["CATEGORY_CODE", "BRAND"],
    "BRAND name repeated in NAME": ["BRAND", "NAME"],
    "Brand Image Check": ["BRAND", "NAME", "Brand_Image_Check_Reason", "Brand_Detected_On_Product"],
    "Product Name Brand Name": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Wrong Variation": ["COUNT_VARIATIONS", "CATEGORY_CODE"],
    "Generic branded products with genuine brands": ["NAME", "BRAND", "CATEGORY"],
    "Missing COLOR": ["CATEGORY_CODE", "NAME", "COLOR"],
    "Missing Weight/Volume": ["CATEGORY_CODE", "NAME"],
    "Incomplete Smartphone Name": ["CATEGORY_CODE", "NAME"],
    "Duplicate product": ["NAME", "SELLER_NAME", "BRAND", "CATEGORY_CODE"],
    "Perfume Tester": ["CATEGORY_CODE", "NAME"],
    "Discount too high": ["GLOBAL_PRICE", "GLOBAL_SALE_PRICE"],
    # "Category Max Price Exceeded": ["CATEGORY_CODE", "GLOBAL_PRICE", "GLOBAL_SALE_PRICE"],
    "Suspicious Discount": ["GLOBAL_PRICE", "GLOBAL_SALE_PRICE"],
    "Poor images": ["MAIN_IMAGE"],
    "Image Stretched": ["MAIN_IMAGE"],
    "Image Blurry": ["MAIN_IMAGE"],
    "Image Mismatch": ["MAIN_IMAGE"],
    "Image Infringing": ["MAIN_IMAGE"],
    "Image Too Many things displayed": ["MAIN_IMAGE"],
    "NG - Gift Card Seller": ["CATEGORY_CODE", "SELLER_NAME"],
    "NG - Books Seller": ["NAME", "SELLER_NAME"],
    "NG - TV Brand Seller": ["CATEGORY_CODE", "BRAND", "SELLER_NAME"],
    "NG - HP Toners Seller": ["CATEGORY_CODE", "BRAND", "SELLER_NAME"],
    "NG - Apple Seller": ["BRAND", "SELLER_NAME"],
    "NG - Xmas Tree Seller": ["NAME", "SELLER_NAME"],
    "NG - Rice Brand Seller": ["CATEGORY_CODE", "BRAND", "SELLER_NAME"],
    "Powerbank Not Authorized": ["CATEGORY_CODE", "NAME", "BRAND"],
    "GH - Smart Glasses with Camera": ["NAME", "CATEGORY_CODE"],
}


def compute_flag_input_hash(data: pd.DataFrame, flag_name: str, kwargs: dict) -> str:
    cols = FLAG_RELEVANT_COLS.get(flag_name, data.columns.tolist())
    available_cols = [c for c in cols if c in data.columns]
    if not available_cols:
        return "empty"
    df_hash_str = df_hash(data[available_cols])
    kwargs_repr = ""
    _skip_keys = {"categories_list", "cat_path_to_code", "code_to_path"}
    for k, v in kwargs.items():
        if k == "data" or k in _skip_keys:
            continue
        if isinstance(v, pd.DataFrame):
            kwargs_repr += df_hash(v)
        else:
            kwargs_repr += repr(v)
    return hashlib.md5((df_hash_str + kwargs_repr).encode()).hexdigest()


def run_cached_check(func, cache_path, ckwargs):
    if func is check_miscellaneous_category:
        return func(**ckwargs)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            try:
                os.unlink(cache_path)  # Delete corrupt cache file
            except Exception:
                pass
    res = func(**ckwargs)
    try:
        with open(cache_path, "wb") as f:
            pickle.dump(res, f)
    except Exception:
        pass
    return res


# -------------------------------------------------
# STANDARD VALIDATION LOGIC
# -------------------------------------------------

import threading

from collections import OrderedDict


class _BoundedDict(OrderedDict):
    def __init__(self, maxsize=5000):
        super().__init__()
        self._maxsize = maxsize

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if len(self) > self._maxsize:
            self.popitem(last=False)


_IMAGE_DIM_CACHE = _BoundedDict(maxsize=5000)  # url/path -> (width, height)
_IMAGE_HASH_CACHE = _BoundedDict(maxsize=5000)  # url/path -> phash hex string
_IMAGE_DIM_LOCK = threading.Lock()


def _compute_phash(img_bytes: bytes) -> str:
    """
    Compute a perceptual hash of the image bytes using imagehash.phash.
    Returns a 16-char hex string (64-bit hash), or '' on failure.

    Two identical images (even if uploaded separately and assigned different
    CDN URLs) will produce the same hash.  Very similar images differ by only
    a few bits (Hamming distance), so we also accept near-matches (<= 8 bits).
    """
    try:
        import imagehash

        img = Image.open(BytesIO(img_bytes)).convert("RGB")
        return str(imagehash.phash(img))
    except Exception:
        return ""


if "zip_image_store" not in st.session_state:
    st.session_state.zip_image_store = {}
if "zip_image_index" not in st.session_state:
    st.session_state.zip_image_index = {}
if "zip_image_source_bytes" not in st.session_state:
    st.session_state.zip_image_source_bytes = None

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
SID_COLUMN_CANDIDATES = [
    "PRODUCT_SET_SID",
    "ProductSetSid",
    "Product Set SID",
    "cod_productset_sid",
    "SID",
]


def _find_sid_col(df: pd.DataFrame) -> Optional[str]:
    return next((c for c in SID_COLUMN_CANDIDATES if c in df.columns), None)


def _basename_lower(value) -> str:
    name = str(value).strip().replace("\\", "/").split("/")[-1].lower()
    return name if name and name != "nan" else ""


def _index_zip_images(zf: zipfile.ZipFile) -> Dict[str, str]:
    """Map image filenames to ZIP members without decompressing image bytes."""
    return {
        _basename_lower(info.filename): info.filename
        for info in zf.infolist()
        if info.filename.lower().startswith("images/")
        and info.filename.lower().endswith(IMAGE_EXTENSIONS)
    }


def _prepare_lazy_zip_images(uploaded_file_records: List[Dict]) -> None:
    st.session_state.zip_image_store = {}
    st.session_state.zip_image_index = {}
    st.session_state.zip_image_source_bytes = None
    for uf in uploaded_file_records:
        if not uf["name"].lower().endswith(".zip"):
            continue
        try:
            with zipfile.ZipFile(BytesIO(uf["bytes"])) as zf:
                index = _index_zip_images(zf)
            if index:
                st.session_state.zip_image_index = index
                st.session_state.zip_image_source_bytes = uf["bytes"]
        except Exception as e:
            logger.warning("Failed indexing ZIP images from %s: %s", uf["name"], e)


def _fetch_all_image_dimensions(data: pd.DataFrame) -> dict:
    """
    Download all unique images ONCE and cache:
      - (width, height)  in _IMAGE_DIM_CACHE
      - perceptual hash  in _IMAGE_HASH_CACHE

    Both caches are filled in a single network pass so there is no
    extra download cost for the duplicate-image check.
    Thread-safe.
    """
    if "MAIN_IMAGE" not in data.columns:
        return {}
    _all_urls = data["MAIN_IMAGE"].astype(str)
    urls = _all_urls[_all_urls.str.strip().str.startswith("http")].unique()
    with _IMAGE_DIM_LOCK:
        # Skip URLs already cached; deduplicate before submitting
        new_urls = list(dict.fromkeys(u for u in urls if u and u not in _IMAGE_DIM_CACHE))

    # ZIP images: read raw bytes from the store so we can hash them too
    zip_images_to_check = []  # list of (key, raw_bytes)
    store = st.session_state.get("zip_image_store", {})
    if store:
        for row in data.itertuples():
            img_val = str(getattr(row, "MAIN_IMAGE", ""))
            if not img_val.startswith("http") and img_val:
                name = getattr(row, "NAME", "")
                brand = getattr(row, "BRAND", "")
                img_bytes = _get_image_from_zip(name, brand, img_val)
                if img_bytes and img_val not in _IMAGE_DIM_CACHE:
                    zip_images_to_check.append((img_val, img_bytes))

    if not new_urls and not zip_images_to_check:
        return _IMAGE_DIM_CACHE

    # Returns (url, size_tuple_or_None, phash_str_or_empty)
    def fetch(url):
        session = get_image_session()
        try:
            r = session.get(
                url.replace("http://", "https://"), timeout=6
            )  # full download
            r = requests.get(url, timeout=5)  # full download — needed for hashing
            if r.status_code == 200:
                raw = r.content
                img = Image.open(BytesIO(raw))
                size = img.size
                ph = _compute_phash(raw)
                return url, size, ph
        except Exception:
            pass
        return url, None, ""

    def process_zip_img(tup):
        """tup = (key, raw_bytes)  — bytes may be raw or a data-URI string."""
        key, payload = tup
        try:
            # payload from zip_image_store is a data-URI string
            if isinstance(payload, str) and payload.startswith("data:"):
                header, encoded = payload.split(",", 1)
                raw = base64.b64decode(encoded)
            else:
                raw = payload if isinstance(payload, (bytes, bytearray)) else b""
            img = Image.open(BytesIO(raw))
            size = img.size
            ph = _compute_phash(raw)
            return key, size, ph
        except Exception:
            pass
        return key, None, ""

    results = []
    if new_urls:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(64, (os.cpu_count() or 4) * 8)) as executor:
            results.extend(list(executor.map(fetch, new_urls)))

    if zip_images_to_check:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 4) * 4)) as executor:
            results.extend(list(executor.map(process_zip_img, zip_images_to_check)))

    with _IMAGE_DIM_LOCK:
        for key, size, ph in results:
            if size:
                _IMAGE_DIM_CACHE[key] = size
            if ph:
                _IMAGE_HASH_CACHE[key] = ph
    return _IMAGE_DIM_CACHE


def check_image_stretched(
    data: pd.DataFrame, _image_cache: dict = None
) -> pd.DataFrame:
    """Flags images with tall (ratio > 1.5) or wide (ratio < 0.6) aspect ratios."""
    if "MAIN_IMAGE" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    target = data[data["MAIN_IMAGE"].astype(str).str.strip() != ""].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    url_data = _image_cache if _image_cache else _fetch_all_image_dimensions(data)

    url_issues = {}
    for url, (w, h) in url_data.items():
        if w > 0:
            ratio = h / w
            if ratio > 1.5:
                url_issues[url] = f"Image Stretched - Tall Aspect Ratio ({w}x{h})"
            elif ratio < 0.6:
                url_issues[url] = f"Image Stretched - Wide Aspect Ratio ({w}x{h})"

    if not url_issues:
        return pd.DataFrame(columns=data.columns)
    mask = target["MAIN_IMAGE"].isin(url_issues.keys())
    flagged = target[mask].copy()
    flagged["Comment_Detail"] = flagged["MAIN_IMAGE"].map(url_issues)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_image_blurry(data: pd.DataFrame, _image_cache: dict = None) -> pd.DataFrame:
    if "MAIN_IMAGE" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    target = data[data["MAIN_IMAGE"].astype(str).str.strip() != ""].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    url_data = _image_cache if _image_cache else _fetch_all_image_dimensions(data)

    reject_map = {}
    commentary_map = {}
    for url, (w, h) in url_data.items():
        if w <= 200 and h <= 200:
            reject_map[url] = f"Image too small/blurry ({w}x{h}px) — below 200x200"
        elif w < 300 and h < 300:
            commentary_map[url] = (
                f"Image resolution low ({w}x{h}px) — consider upgrading"
            )

    try:
        existing = st.session_state.get("_image_blurry_commentary", {})
        sid_to_comment = {}
        for row in target.itertuples():
            url = str(getattr(row, "MAIN_IMAGE", ""))
            sid = str(getattr(row, "PRODUCT_SET_SID", ""))
            if url in commentary_map:
                sid_to_comment[sid] = commentary_map[url]
        existing.update(sid_to_comment)
        st.session_state["_image_blurry_commentary"] = existing
    except Exception:
        pass

    if not reject_map:
        return pd.DataFrame(columns=data.columns)

    mask = target["MAIN_IMAGE"].isin(reject_map.keys())
    flagged = target[mask].copy()
    flagged["Comment_Detail"] = flagged["MAIN_IMAGE"].map(reject_map)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_image_mismatch(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(columns=data.columns)


def check_image_infringing(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(columns=data.columns)


def check_image_too_many_things(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    return pd.DataFrame(columns=data.columns)


def check_poor_images_aspect_ratio(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Backwards-compatibility alias → delegates to check_image_stretched."""
    return check_image_stretched(data)


def check_miscellaneous_category(
    data: pd.DataFrame,
    categories_list: list = None,
    compiled_rules: dict = None,
    cat_path_to_code: dict = None,
    code_to_path: dict = None,
) -> pd.DataFrame:
    if not categories_list or not code_to_path:
        try:
            _sf = st.session_state.get("support_files", {})
            categories_list = categories_list or _sf.get("categories_names_list", [])
            cat_path_to_code = cat_path_to_code or _sf.get("cat_path_to_code", {})
            code_to_path = code_to_path or _sf.get("code_to_path", {})
        except:
            pass

    if _CAT_MATCHER_AVAILABLE:
        try:
            _engine = _get_cat_matcher_engine()
            if _engine is not None:
                if categories_list and not _engine._tfidf_built:
                    _engine.build_tfidf_index(categories_list)
                return check_wrong_category(
                    data,
                    categories_list=categories_list,
                    cat_path_to_code=cat_path_to_code,
                    code_to_path=code_to_path,
                )
        except Exception as _e:
            logger.warning("check_wrong_category engine error: %s", _e)

    if "CATEGORY" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    flagged = data[
        data["CATEGORY"].astype(str).str.contains("miscellaneous", case=False, na=False)
    ].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = "Category contains 'Miscellaneous'"
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


@st.cache_data(show_spinner=False)
def _to_polars_cached(data_hash: str, data: pd.DataFrame):
    import polars as pl
    return pl.from_pandas(data)


def check_restricted_brands(
    data: pd.DataFrame, country_rules: List[Dict]
) -> pd.DataFrame:
    if data.empty or not country_rules:
        return pd.DataFrame(columns=data.columns)

    ldf = _to_polars_cached(df_hash(data), data)

    all_keywords = set()
    for rule in country_rules:
        all_keywords.add(rule["brand"])
        all_keywords.update(rule.get("variations", []))

    # 🚀 Polars Filtering (10x faster than pandas .str.contains)
    candidate_ldf = ldf.filter(
        pl.col("_brand_lower").is_in(list(all_keywords))
        | pl.col("_name_lower").str.contains(
            "(?i)" + "|".join(re.escape(k) for k in all_keywords)
        )
    )

    if candidate_ldf.is_empty():
        return pd.DataFrame(columns=data.columns)

    d = candidate_ldf.to_pandas()

    flagged_indices = set()
    comment_map = {}
    match_details = {}
    for rule in country_rules:
        brand_name = rule["brand"]
        brand_raw = rule["brand_raw"]
        brand_pattern = r"(?<!\w)" + re.escape(brand_name) + r"(?!\w)"
        main_brand_matches = d["_brand_lower"] == brand_name
        main_name_matches = d["_name_lower"].str.contains(
            brand_pattern, regex=True, na=False
        )
        current_match_mask = main_brand_matches | main_name_matches
        for idx in d[main_brand_matches].index:
            match_details[idx] = ("main_brand", brand_raw)
        for idx in d[main_name_matches & ~main_brand_matches].index:
            match_details[idx] = ("main_name", brand_raw)
        if rule["variations"]:
            sorted_vars = sorted(rule["variations"], key=len, reverse=True)
            var_pattern = (
                r"(?<!\w)(?:" + "|".join([re.escape(v) for v in sorted_vars]) + r")(?!\w)"
            )
            var_brand_matches = d["_brand_lower"].str.contains(
                var_pattern, regex=True, na=False
            )
            var_name_matches = d["_name_lower"].str.contains(
                var_pattern, regex=True, na=False
            )
            for idx in d[var_brand_matches | var_name_matches].index:
                if idx not in match_details:
                    text_to_check = (
                        d.loc[idx, "_brand_lower"]
                        if var_brand_matches[idx]
                        else d.loc[idx, "_name_lower"]
                    )
                    for var in sorted_vars:
                        if var in text_to_check:
                            match_details[idx] = (
                                "variation",
                                f"{brand_raw} (as '{var}')",
                            )
                            break
            current_match_mask = (
                current_match_mask | var_brand_matches | var_name_matches
            )
        if not current_match_mask.any():
            continue
        current_match = d[current_match_mask]
        if rule["categories"]:
            current_match = current_match[
                current_match["_cat_clean"].isin(rule["categories"])
            ]
        if current_match.empty:
            continue
        rejected = current_match[~current_match["_seller_lower"].isin(rule["sellers"])]
        if not rejected.empty:
            for idx in rejected.index:
                flagged_indices.add(idx)
                match_type, match_info = match_details.get(idx, ("unknown", brand_raw))
                seller_status = (
                    "Seller not in approved list"
                    if rule["sellers"]
                    else "No sellers approved"
                )
                comment_map[idx] = f"Restricted Brand: {match_info} - {seller_status}"
    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)
    result = data.loc[list(flagged_indices)].copy()
    result["Comment_Detail"] = result.index.map(comment_map)
    return result.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_prohibited_products(
    data: pd.DataFrame, prohibited_rules: List[Dict]
) -> pd.DataFrame:
    if not {"NAME", "CATEGORY_CODE"}.issubset(data.columns) or not prohibited_rules:
        return pd.DataFrame(columns=data.columns)

    # ── PRE-FILTER: single combined regex for ALL keywords ────────────────────
    all_kws = sorted(
        set(rule["keyword"] for rule in prohibited_rules), key=len, reverse=True
    )
    combined_pattern = re.compile(
        r"(?<!\w)(" + "|".join(re.escape(k) for k in all_kws) + r")(?!\w)",
        re.IGNORECASE,
    )
    match_mask = data["_name_lower"].str.contains(combined_pattern, na=False)
    if not match_mask.any():
        return pd.DataFrame(columns=data.columns)
    candidates = data[match_mask]

    # Build keyword -> categories lookup
    kw_to_cats = {}
    for rule in prohibited_rules:
        kw_to_cats.setdefault(rule["keyword"], set()).update(rule["categories"])

    flagged_indices = set()
    comment_map = {}
    name_replacements = {}
    for idx in candidates.index:
        name_lower = data.loc[idx, "_name_lower"]
        cat_clean = data.loc[idx, "_cat_clean"]
        raw_name = str(data.loc[idx, "NAME"])
        matches = combined_pattern.findall(name_lower)
        if not matches:
            continue
        matched_kws = []
        for m in set(matches):
            m_lower = m.lower()
            cats = kw_to_cats.get(m_lower, set())
            if cats and cat_clean not in cats:
                continue
            matched_kws.append(m_lower)
        if matched_kws:
            flagged_indices.add(idx)
            comment_map[idx] = "Prohibited: " + ", ".join(matched_kws)
            highlighted = combined_pattern.sub(
                lambda m: f"[!]{m.group(0)}[!]", raw_name
            )
            name_replacements[idx] = highlighted

    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)
    result = data.loc[list(flagged_indices)].copy()
    result["Comment_Detail"] = result.index.map(lambda i: comment_map[i])
    for idx, new_name in name_replacements.items():
        result.loc[idx, "NAME"] = new_name
    return result.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_suspected_fake_products(
    data: pd.DataFrame, suspected_fake_df: pd.DataFrame
) -> pd.DataFrame:
    if (
        not all(
            c in data.columns
            for c in ["CATEGORY_CODE", "BRAND", "GLOBAL_SALE_PRICE", "GLOBAL_PRICE"]
        )
        or suspected_fake_df.empty
    ):
        return pd.DataFrame(columns=data.columns)
    try:
        ref_data = suspected_fake_df.copy()
        brand_cat_price = {}
        for brand in [
            c
            for c in ref_data.columns
            if c not in ["Unnamed: 0", "Brand", "Price"] and pd.notna(c)
        ]:
            try:
                pt = pd.to_numeric(ref_data[brand].iloc[0], errors="coerce")
                if pd.isna(pt) or pt <= 0:
                    continue
            except:
                continue
            for cat in ref_data[brand].iloc[1:].dropna():
                cat_base = str(cat).strip().split(".")[0]
                if cat_base and cat_base.lower() != "nan":
                    brand_cat_price[(brand.strip().lower(), cat_base)] = pt
        if not brand_cat_price:
            return pd.DataFrame(columns=data.columns)
        d = data.copy()
        d["price_to_use"] = pd.to_numeric(
            d["GLOBAL_SALE_PRICE"].where(
                d["GLOBAL_SALE_PRICE"].notna()
                & (pd.to_numeric(d["GLOBAL_SALE_PRICE"], errors="coerce") > 0),
                d["GLOBAL_PRICE"],
            ),
            errors="coerce",
        ).fillna(0)
        prices = d["price_to_use"].values
        brands = d["_brand_lower"].values
        cats = d["_cat_clean"].values
        d["is_fake"] = [
            p < brand_cat_price.get((b, c), -1) for p, b, c in zip(prices, brands, cats)
        ]
        return d[d["is_fake"] == True][data.columns].drop_duplicates(
            subset=["PRODUCT_SET_SID"]
        )
    except Exception as e:
        logger.warning(f"check_suspected_fake_products: {e}")
        return pd.DataFrame(columns=data.columns)


def check_refurb_seller_approval(
    data: pd.DataFrame, refurb_data: dict, country_code: str
) -> pd.DataFrame:
    required = {"PRODUCT_SET_SID", "CATEGORY_CODE", "SELLER_NAME", "NAME"}
    if not required.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    phone_cats = refurb_data.get("categories", {}).get("Phones", set())
    laptop_cats = refurb_data.get("categories", {}).get("Laptops", set())
    keywords = refurb_data.get("keywords", set())
    sellers = refurb_data.get("sellers", {}).get(country_code, {})
    if not phone_cats and not laptop_cats:
        return pd.DataFrame(columns=data.columns)
    if not keywords:
        return pd.DataFrame(columns=data.columns)
    kw_pattern = re.compile(
        r"\b(?:"
        + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
        + r")\b",
        re.IGNORECASE,
    )
    d = data
    is_phone = d["_cat_clean"].isin(phone_cats)
    is_laptop = d["_cat_clean"].isin(laptop_cats)
    in_scope = is_phone | is_laptop
    has_keyword = d["NAME"].astype(str).str.contains(kw_pattern, na=False)
    approved_phones = sellers.get("Phones", set())
    approved_laptops = sellers.get("Laptops", set())
    not_approved = (is_phone & ~d["_seller_lower"].isin(approved_phones)) | (
        is_laptop & ~d["_seller_lower"].isin(approved_laptops)
    )
    flagged = d[in_scope & has_keyword & not_approved].copy()
    if not flagged.empty:

        def build_comment(row):
            ptype = "Phone" if row["_cat_clean"] in phone_cats else "Laptop"
            match = kw_pattern.search(str(row["NAME"]))
            kw_found = match.group(0) if match else "?"
            return f"Unapproved {ptype} refurb seller — keyword '{kw_found}' in name (cat: {row['_cat_clean']})"

        flagged["Comment_Detail"] = flagged.apply(build_comment, axis=1)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_product_warranty(
    data: pd.DataFrame, warranty_category_codes: List[str]
) -> pd.DataFrame:
    d = data.copy()
    for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]:
        if c not in d.columns:
            d[c] = ""
        d[c] = d[c].astype(str).fillna("").str.strip()
    if not warranty_category_codes:
        return pd.DataFrame(columns=d.columns)
    target = d[
        d["_cat_clean"].isin([clean_category_code(c) for c in warranty_category_codes])
    ]
    if target.empty:
        return pd.DataFrame(columns=d.columns)

    def is_present(s):
        return (s != "nan") & (s != "") & (s != "none") & (s != "nat") & (s != "n/a")

    # Only flag rows that actually came from a file with warranty columns — when
    # multiple files are merged, rows from file formats without warranty data will
    # have blank warranty columns even though no data was supplied.
    # Use _has_warranty_data sentinel set during merge; fall back to any non-empty
    # value in the column across the whole dataset as a proxy.
    if "_has_warranty_data" in target.columns:
        target = target[target["_has_warranty_data"] == True]
    elif not is_present(d["PRODUCT_WARRANTY"]).any() and not is_present(d["WARRANTY_DURATION"]).any():
        return pd.DataFrame(columns=d.columns)

    if target.empty:
        return pd.DataFrame(columns=d.columns)

    mask = ~(
        is_present(target["PRODUCT_WARRANTY"]) | is_present(target["WARRANTY_DURATION"])
    )
    return target[mask].drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_seller_approved_for_books(
    data: pd.DataFrame,
    books_data: Dict,
    country_code: str,
    book_category_codes: List[str],
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "SELLER_NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    category_codes = books_data.get("category_codes") or set(
        clean_category_code(c) for c in book_category_codes
    )
    if not category_codes:
        return pd.DataFrame(columns=data.columns)
    approved_sellers = books_data.get("sellers", {}).get(country_code, set())
    if not approved_sellers:
        return pd.DataFrame(columns=data.columns)
    books = data[data["_cat_clean"].isin(category_codes)].copy()
    if books.empty:
        return pd.DataFrame(columns=data.columns)
    not_approved = ~books["_seller_lower"].isin(approved_sellers)
    flagged = books[not_approved].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = "Seller not approved to sell books: " + flagged[
            "SELLER_NAME"
        ].astype(str)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_seller_approved_for_perfume(
    data: pd.DataFrame,
    perfume_category_codes: List[str],
    perfume_data: Dict,
    country_code: str,
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "SELLER_NAME", "BRAND", "NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    sheet_cat_codes = perfume_data.get("category_codes")
    cat_codes = (
        sheet_cat_codes
        if sheet_cat_codes
        else set(clean_category_code(c) for c in perfume_category_codes)
    )
    perfume = data[data["_cat_clean"].isin(cat_codes)].copy()
    if perfume.empty:
        return pd.DataFrame(columns=data.columns)
    keywords = perfume_data.get("keywords", set())
    approved_sellers = perfume_data.get("sellers", {}).get(country_code, set())
    has_seller_list = bool(approved_sellers)
    GENERIC_PLACEHOLDERS = {
        "designers collection",
        "smart collection",
        "generic",
        "original",
        "fashion",
        "",
        "nan",
        "unbranded",
        "no brand",
        "new",
    }
    if keywords:
        kw_pattern = re.compile(
            r"\b(?:"
            + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
            + r")\b",
            re.IGNORECASE,
        )
        sneaky_mask = perfume["_brand_lower"].isin(GENERIC_PLACEHOLDERS) & perfume[
            "_name_lower"
        ].str.contains(kw_pattern, na=False)
    else:
        sneaky_mask = pd.Series([False] * len(perfume), index=perfume.index)
    brand_sens_mask = (
        perfume["_brand_lower"].str.contains(kw_pattern, na=False)
        if keywords
        else pd.Series([False] * len(perfume), index=perfume.index)
    )
    needs_approval = sneaky_mask | brand_sens_mask
    if has_seller_list:
        not_approved = ~perfume["_seller_lower"].isin(approved_sellers)
        flagged_mask = needs_approval & not_approved
    else:
        # No approved seller list for this country → flag ALL that need approval
        flagged_mask = needs_approval
    flagged = perfume[flagged_mask].copy()
    if not flagged.empty:

        def describe(row):
            b, n = str(row["BRAND"]).strip(), str(row["NAME"]).strip()[:40]
            if b.lower() in GENERIC_PLACEHOLDERS:
                return f"Sneaky brand in name: '{n}'"
            return f"Sensitive brand '{b}' — seller not approved"

        flagged["Comment_Detail"] = flagged.apply(describe, axis=1)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_perfume_tester(
    data: pd.DataFrame, perfume_category_codes: List[str], perfume_data: Dict
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    sheet_cat_codes = perfume_data.get("category_codes")
    cat_codes = (
        sheet_cat_codes
        if sheet_cat_codes
        else set(clean_category_code(c) for c in perfume_category_codes)
    )
    if not cat_codes:
        return pd.DataFrame(columns=data.columns)
    perfume = data[data["_cat_clean"].isin(cat_codes)].copy()
    if perfume.empty:
        return pd.DataFrame(columns=data.columns)
    tester_pattern = re.compile(
        r"\b(tester|testeur)s?\b|\btester(?=[\d\-_])", re.IGNORECASE
    )
    flagged = perfume[
        perfume["_name_lower"].str.contains(tester_pattern, na=False)
    ].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = (
            "Perfume tester listed for sale: " + flagged["NAME"].astype(str).str[:60]
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_counterfeit_sneakers(
    data: pd.DataFrame,
    sneaker_category_codes: List[str],
    sneaker_sensitive_brands: List[str],
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    sneakers = data[
        data["_cat_clean"].isin(
            set(clean_category_code(c) for c in sneaker_category_codes)
        )
    ].copy()
    if sneakers.empty:
        return pd.DataFrame(columns=data.columns)
    return sneakers[
        sneakers["_brand_lower"].isin(["generic", "fashion"])
        & sneakers["_name_lower"].apply(
            lambda x: any(b in x for b in sneaker_sensitive_brands)
        )
    ].drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_counterfeit_jerseys(
    data: pd.DataFrame, jerseys_data: Dict, country_code: str
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME", "SELLER_NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    categories = jerseys_data.get("categories", set())
    keywords = jerseys_data.get("keywords", {}).get(country_code, set())
    exempted = jerseys_data.get("exempted", {}).get(country_code, set())
    if not categories or not keywords:
        return pd.DataFrame(columns=data.columns)
    kw_pattern = re.compile(
        r"(?<!\w)(?:"
        + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
        + r")(?!\w)",
        re.IGNORECASE,
    )
    d = data
    in_scope = d["_cat_clean"].isin(categories)
    has_keyword = d["NAME"].astype(str).str.contains(kw_pattern, na=False)
    not_exempted = ~d["_seller_lower"].isin(exempted)
    flagged = d[in_scope & has_keyword & not_exempted].copy()
    if not flagged.empty:

        def build_comment(row):
            match = kw_pattern.search(str(row["NAME"]))
            kw_found = match.group(0) if match else "?"
            return f"Suspected counterfeit jersey — keyword '{kw_found}' (cat: {row['_cat_clean']})"

        flagged["Comment_Detail"] = flagged.apply(build_comment, axis=1)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_all_caps_name(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    s = data["NAME"].astype(str).str.strip()
    mask = (
        (s.str.len() > 6)
        & (s == s.str.upper())
        & s.str.replace(" ", "", regex=False).str.isalpha()
    )
    return data[mask].copy()


def check_name_too_short(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    mask = data["NAME"].astype(str).str.strip().str.len() < 15
    return data[mask].copy()


def check_variation_name_consistency_polars(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    if not {"PRODUCT_SET_SID", "NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    import polars as pl

    lf = pl.from_pandas(data[["PRODUCT_SET_SID", "NAME"]]).lazy()
    result = (
        lf.with_columns(pl.col("NAME").str.to_lowercase())
        .group_by("PRODUCT_SET_SID")
        .agg(pl.col("NAME").n_unique().alias("name_variants"))
        .filter(pl.col("name_variants") > 3)
        .collect()
    )
    flagged_sids = result["PRODUCT_SET_SID"].to_list()
    return data[data["PRODUCT_SET_SID"].isin(flagged_sids)].copy()


def check_suspected_fake_perfume(
    data: pd.DataFrame,
    perfume_catalog: Dict,
    perfume_category_codes: List[str],
    **kwargs,
) -> pd.DataFrame:
    """
    Flags products where:
      1. The BRAND field is a known fake/generic brand (e.g. Generic, Designer,
         Smart Collection, Fragrance World, Testa, Tester …) — from perfume_catalog['fake_brands']
      2. The product NAME contains a known legitimate perfume brand name, alias,
         or model name — from perfume_catalog['legit_brand_terms'] + ['model_terms']
      3. The product is in a perfume category (perfume_category_codes)

    Example trigger:
        BRAND = "Designer"
        NAME  = "Designer Creed EAU DE PARFUM AVENTUS PERFUME"
        → flagged because "creed" (legit brand) and "aventus" (Creed model) found in name

    Same reason code as Counterfeit Sneakers: 1000023
    """
    if not {"CATEGORY_CODE", "NAME", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    if not perfume_catalog:
        return pd.DataFrame(columns=data.columns)

    fake_brands = perfume_catalog.get("fake_brands", set())
    legit_brand_terms = perfume_catalog.get("legit_brand_terms", set())
    model_terms = perfume_catalog.get("model_terms", set())
    term_to_brand = perfume_catalog.get("term_to_brand", {})

    if not fake_brands or not (legit_brand_terms or model_terms):
        return pd.DataFrame(columns=data.columns)

    all_terms = legit_brand_terms | model_terms
    term_pattern = re.compile(
        r"\b("
        + "|".join(re.escape(t) for t in sorted(all_terms, key=len, reverse=True))
        + r")\b",
        re.IGNORECASE,
    )

    cat_codes = set(clean_category_code(c) for c in perfume_category_codes)
    d = data[data["_cat_clean"].isin(cat_codes)].copy()
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    d = d[d["_brand_lower"].isin(fake_brands)].copy()
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    def _find_match(name_lower):
        m = term_pattern.search(str(name_lower))
        return m.group(0).lower() if m else None

    d["_pfume_match"] = d["_name_lower"].apply(_find_match)
    flagged = d[d["_pfume_match"].notna()].copy()

    if not flagged.empty:

        def build_comment(row):
            term = row["_pfume_match"]
            brand = term_to_brand.get(term, term.title())
            return f"Suspected fake {brand} perfume — '{term}' in name"

        flagged["Comment_Detail"] = flagged.apply(build_comment, axis=1)

    return flagged.drop(columns=["_pfume_match"]).drop_duplicates(
        subset=["PRODUCT_SET_SID"]
    )


def check_unnecessary_words(data: pd.DataFrame, pattern: re.Pattern) -> pd.DataFrame:
    if not {"NAME"}.issubset(data.columns) or pattern is None:
        return pd.DataFrame(columns=data.columns)
    mask = data["_name_lower"].str.contains(pattern, na=False)
    flagged = data[mask].copy()
    if not flagged.empty:

        def get_matches(text):
            if pd.isna(text):
                return ""
            matches = pattern.findall(str(text))
            return ", ".join(set(m.lower() for m in matches if isinstance(m, str)))

        def highlight_matches(text):
            if pd.isna(text):
                return text
            return pattern.sub(lambda m: f"[*]{m.group(0)}[*]", str(text))

        flagged["Comment_Detail"] = "Unnecessary: " + flagged["NAME"].apply(get_matches)
        flagged["NAME"] = flagged["NAME"].apply(highlight_matches)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_single_word_name(
    data: pd.DataFrame, book_category_codes: List[str], books_data: Dict = None
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    cat_codes = (books_data or {}).get("category_codes") or set(
        clean_category_code(c) for c in book_category_codes
    )
    d = data
    names = d["NAME"].astype(str).str.strip()
    word_counts = names.str.split().str.len()
    char_counts = names.str.len()
    bad_name_mask = (word_counts <= 2) | (char_counts < 15)
    if "_cat_clean" in d.columns:
        non_books_mask = ~d["_cat_clean"].isin(cat_codes)
    else:
        non_books_mask = ~d["CATEGORY_CODE"].apply(clean_category_code).isin(cat_codes)
    flagged = d[bad_name_mask & non_books_mask].copy()
    if not flagged.empty:
        import numpy as np
        _fw = flagged["NAME"].astype(str).str.strip()
        _wc = _fw.str.split().str.len().fillna(0).astype(int)
        _cc = _fw.str.len()
        flagged["Comment_Detail"] = np.where(
            (_wc <= 2) & (_cc < 15),
            _wc.astype(str) + " words, " + _cc.astype(str) + " chars",
            np.where(_wc <= 2, _wc.astype(str) + " words", _cc.astype(str) + " chars"),
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_generic_brand_issues(
    data: pd.DataFrame, valid_category_codes_fas: List[str]
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    return data[
        data["_cat_clean"].isin(
            set(clean_category_code(c) for c in valid_category_codes_fas)
        )
        & (data["_brand_lower"] == "generic")
    ].drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_fashion_brand_issues(
    data: pd.DataFrame, valid_category_codes_fas: List[str], code_to_path: Dict = None
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    if code_to_path is None:
        code_to_path = {}
    fashion_brand = data[data["_brand_lower"] == "fashion"].copy()
    if fashion_brand.empty:
        return pd.DataFrame(columns=data.columns)

    def _in_fashion_domain(cat_code: str) -> bool:
        full_path = code_to_path.get(str(cat_code).strip(), "")
        if full_path:
            return full_path.strip().lower().startswith("fashion")
        return clean_category_code(cat_code) in fas_codes

    fas_codes = set(clean_category_code(c) for c in valid_category_codes_fas)
    flagged = fashion_brand[
        ~fashion_brand["CATEGORY_CODE"].apply(
            lambda c: _in_fashion_domain(clean_category_code(c))
        )
    ].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = (
            "Brand 'Fashion' used outside Fashion category: "
            + flagged["CATEGORY_CODE"].astype(str)
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_brand_in_name(data: pd.DataFrame) -> pd.DataFrame:
    if not {"BRAND", "NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    import re

    brands = data["_brand_lower"].values
    names = data["_name_lower"].values
    # 🚀 Use word boundaries to avoid false positives (e.g., "Mi" in "Xiaomi")
    mask = [
        bool(re.search(r"\b" + re.escape(str(b)) + r"\b", str(n)))
        if b and str(b) != "nan"
        else False
        for b, n in zip(brands, names)
    ]
    return data[mask].drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_wrong_variation(
    data: pd.DataFrame, allowed_variation_codes: List[str]
) -> pd.DataFrame:
    d = data.copy()
    if "COUNT_VARIATIONS" not in d.columns:
        d["COUNT_VARIATIONS"] = 1
    if "CATEGORY_CODE" not in d.columns:
        return pd.DataFrame(columns=data.columns)
    d["qty_var"] = (
        pd.to_numeric(d["COUNT_VARIATIONS"], errors="coerce").fillna(1).astype(int)
    )
    flagged = d[
        (d["qty_var"] >= 3)
        & (
            ~d["_cat_clean"].isin(
                set(clean_category_code(c) for c in allowed_variation_codes)
            )
        )
    ].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = (
            "Variations: "
            + flagged["qty_var"].astype(str)
            + ", Category: "
            + flagged["_cat_clean"]
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_generic_with_brand_in_name(
    data: pd.DataFrame, brands_list: List[str]
) -> pd.DataFrame:
    if not {"NAME", "BRAND"}.issubset(data.columns) or not brands_list:
        return pd.DataFrame(columns=data.columns)
    _PSEUDO_BRANDS = {"generic", "fashion", "unbranded", "no brand", "original", "new"}
    mask = data["_brand_lower"].isin(_PSEUDO_BRANDS)
    if "CATEGORY" in data.columns:
        mask = mask & ~data["CATEGORY"].astype(str).str.lower().str.contains(
            r"\b(case|cases|cover|covers)\b", regex=True, na=False
        )
    gen = data[mask].copy()
    if gen.empty:
        return pd.DataFrame(columns=data.columns)
    sorted_b = sorted(
        [str(b).strip().lower() for b in brands_list if b], key=len, reverse=True
    )

    def detect(n):
        nc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", str(n).lower())).strip()
        for b in sorted_b:
            bc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", b)).strip()
            if nc.startswith(bc) and (len(nc) == len(bc) or not nc[len(bc)].isalnum()):
                return b.title()
        return None

    gen["Detected_Brand"] = [detect(n) for n in gen["NAME"].values]
    flagged = gen[gen["Detected_Brand"].notna()].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = (
            "Brand field '"
            + flagged["_brand_lower"].str.title()
            + "' but name starts with: "
            + flagged["Detected_Brand"]
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


@st.cache_data(show_spinner=False)
def load_valid_colors() -> set:
    valid_set = set()
    try:
        if os.path.exists("colors.txt"):
            with open("colors.txt", "r", encoding="utf-8") as f:
                for line in f:
                    color = line.strip().lower()
                    if color:
                        valid_set.add(color)
    except Exception as e:
        logger.warning(f"Could not load colors.txt: {e}")
    return valid_set


def check_missing_color(
    data: pd.DataFrame,
    pattern: re.Pattern,
    color_categories: List[str],
    country_code: str,
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME"}.issubset(data.columns) or pattern is None:
        return pd.DataFrame(columns=data.columns)
    target = data[
        data["_cat_clean"].isin(set(clean_category_code(c) for c in color_categories))
    ].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)
    has_color = "COLOR" in data.columns
    names = target["NAME"].astype(str).values
    colors = (
        target["COLOR"].astype(str).str.strip().str.lower().values
        if has_color
        else [""] * len(target)
    )
    valid_colors = load_valid_colors()
    null_like = {"nan", "", "none", "null", "n/a", "na", "-"}
    _JUNK_COLORS = {
        "random",
        "random color",
        "random colour",
        "assorted",
        "various",
        "as in the picture",
        "as in the pictures",
        "as the picture",
        "as per image",
        "as shown",
        "see image",
        "see photo",
        "all color available",
        "all color availble",
        "all colors available",
        "multicolour",
        "multicolored",
        "multicoloured",
        "multi colour",
        "multi color",
        "multi-colour",
        "multi-color",
        "multicolors",
        "mult",
        "multic",
    }
    _MODIFIER_WORDS = {
        "dark",
        "light",
        "bright",
        "deep",
        "pale",
        "soft",
        "matte",
        "matt",
        "glossy",
        "metallic",
        "neon",
        "pastel",
        "dusty",
        "warm",
        "cool",
        "royal",
        "navy",
        "olive",
        "mustard",
        "burnt",
        "forest",
        "sky",
        "baby",
        "hot",
        "ice",
        "mint",
        "rose",
        "coral",
        "nude",
        "tan",
        "charcoal",
        "ash",
        "sand",
        "cream",
        "ivory",
        "champagne",
        "coffee",
        "chocolate",
        "caramel",
        "wine",
        "burgundy",
        "nordic",
        "jungle",
        "emerald",
        "sapphire",
        "ruby",
        "amber",
        "teal",
        "aqua",
        "indigo",
        "violet",
        "lavender",
        "lilac",
        "magenta",
        "fuchsia",
        "maroon",
        "copper",
        "bronze",
        "gold",
        "silver",
        "platinum",
        "dominantly",
        "accent",
        "accents",
        "print",
        "stripe",
        "striped",
        "check",
        "checked",
        "pattern",
        "bead",
        "beaded",
        "ring",
        "with",
        "and",
        "or",
    }

    def _is_valid_color(color_str: str, valid_set: set) -> bool:
        c = color_str.strip().lower()
        if c in _JUNK_COLORS:
            return False
        if re.match(r"^[.\-_*]{1,5}$", c):
            return False
        if not valid_set:
            return True
        parts = re.split(r"[,/&|\-]|\s+and\s+|\s+or\s+|\s+with\s+", c)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part in valid_set:
                return True
            tokens = part.split()
            for token in tokens:
                token = token.strip()
                if token in valid_set and token not in _MODIFIER_WORDS:
                    return True
        return False

    mask = []
    for n, c in zip(names, colors):
        is_name_valid = bool(pattern.search(n))
        is_col_valid = False
        if has_color and c not in null_like:
            is_col_valid = _is_valid_color(c, valid_colors)
        if is_col_valid or is_name_valid:
            mask.append(False)
        else:
            mask.append(True)
    flagged = target[mask].copy()
    if not flagged.empty:

        def get_reason(row):
            c_val = str(row.get("COLOR", "")).strip().lower()
            if c_val and c_val not in null_like:
                return f"Invalid color value provided: '{str(row.get('COLOR', '')).strip()}'"
            return "Color missing in both NAME and COLOR attributes"

        flagged["Comment_Detail"] = flagged.apply(get_reason, axis=1)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_weight_volume_in_name(
    data: pd.DataFrame, weight_category_codes: List[str]
) -> pd.DataFrame:
    if (
        not {"CATEGORY_CODE", "NAME"}.issubset(data.columns)
        or not weight_category_codes
    ):
        return pd.DataFrame(columns=data.columns)
    target = data[
        data["_cat_clean"].isin(
            set(clean_category_code(c) for c in weight_category_codes)
        )
    ].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)
    pat = re.compile(
        r"\b\d+(?:\.\d+)?\s*(?:[a-z]{1,20}\s*){0,3}"
        r"(?:kg|kgs|g|gm|gms|grams|mg|mcg|ml|l|ltr|liter|litres|litre|cl|oz|ounce|ounces|lb|lbs|m"
        r"|tablets?|tabs?|capsules?|caps?|sachets?|count|ct|sticks?|iu"
        r"|tea\s*bags?|teabags?|bags?|softgels?|lozenges?|gummies|gummy|vials?|ampoules?|tubes?"
        r"|pieces?|pcs|pack|packs|pairs?|rolls?|sheets?|wipes?|pods?|units?|serves?|servings?|vegan\s+pieces?"
        r"|dozens?|box|boxes|set|sets|bundle|bundles|lot|lots|collection|kit|kits)"
        r"|\b\d+[\u0027\u2019]?s\b"
        r"|\b(?:a\s+)?dozen\b"
        r"|\b(?:pack|box|set|bundle|lot)\s+of\s+\d+\b"
        r"|\bper\s+(?:kg|kgs?|g|gm|grams?|mg|mcg|ml|l|ltr|oz|lb)\b"
        r"|\d+\s*(?:\xc2\xb5g|\xce\xbcg|\xb5g|\u00b5g|\u03bcg|mcg|µg|μg)",
        re.IGNORECASE,
    )
    return target[~target["_name_lower"].str.contains(pat, na=False)].drop_duplicates(
        subset=["PRODUCT_SET_SID"]
    )


def check_incomplete_smartphone_name(
    data: pd.DataFrame, smartphone_category_codes: List[str]
) -> pd.DataFrame:
    if (
        not {"CATEGORY_CODE", "NAME"}.issubset(data.columns)
        or not smartphone_category_codes
    ):
        return pd.DataFrame(columns=data.columns)
    target = data[
        data["_cat_clean"].isin(
            set(clean_category_code(c) for c in smartphone_category_codes)
        )
    ].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)
    pat = re.compile(r"\b\d+\s*(gb|tb)\b", re.IGNORECASE)
    flagged = target[~target["_name_lower"].str.contains(pat, na=False)].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = "Name missing Storage/Memory spec (e.g., 64GB)"
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


# ---------------------------------------------------------------------------
# Size / capacity token extraction for duplicate detection
# Handles patterns like: 4L, 4 L, 4 Litres, 9litres, 500ml, 3kg, 15cm, 64GB …
# The extracted token is INCLUDED in the dedup key so that two products from
# the same seller that differ only in size are never flagged as duplicates,
# regardless of how similar their names or images look.
# ---------------------------------------------------------------------------
_SIZE_UNIT_PATTERN = re.compile(
    r"(?<![\w.])"
    r"(\d+(?:[.,]\d+)?)"
    r"\s*"
    r"(l(?:itres?|iters?)?|ml|cl"
    r"|kg(?:s)?|g(?:ram(?:s)?)?|mg|lb(?:s)?|oz"
    r'|cm|mm|m(?:etres?|eters?)?|ft|inch(?:es)?|"'
    r"|w(?:atts?)?|kw(?:h)?|v(?:olts?)?|mah|ah"
    r"|gb|tb|mb|gig(?:s)?"
    r"|hp|cc|pcs?|pieces?|pack(?:s)?|set(?:s)?|port(?:s)?|slot(?:s)?"
    r"|(?:x\d+))"
    r"(?![\w.])"
    r"|(?<![\w.])(?:x|by)\s*\d+(?:[.,]\d+)?(?![\w.])"
    r"|\bsize\s+\d+\b"
    r"|\b\d+\s*(?:x\s*\d+)+\b",
    re.IGNORECASE,
)

_SIZE_UNIT_NORMALISE = {
    "litre": "l",
    "litres": "l",
    "liter": "l",
    "liters": "l",
    "ml": "ml",
    "cl": "cl",
    "kg": "kg",
    "kgs": "kg",
    "gram": "g",
    "grams": "g",
    "mg": "mg",
    "lbs": "lb",
    "lb": "lb",
    "oz": "oz",
    "cm": "cm",
    "mm": "mm",
    "meter": "m",
    "meters": "m",
    "metre": "m",
    "metres": "m",
    "ft": "ft",
    "inch": "in",
    "inches": "in",
    "watt": "w",
    "watts": "w",
    "kw": "kw",
    "kwh": "kwh",
    "volt": "v",
    "volts": "v",
    "mah": "mah",
    "ah": "ah",
    "gb": "gb",
    "tb": "tb",
    "mb": "mb",
    "gig": "gb",
    "gigs": "gb",
    "hp": "hp",
    "cc": "cc",
    "pcs": "pcs",
    "pc": "pcs",
    "piece": "pcs",
    "pieces": "pcs",
    "pack": "pack",
    "packs": "pack",
    "set": "set",
    "sets": "set",
    "port": "port",
    "ports": "port",
    "slot": "slot",
    "slots": "slot",
}


def _extract_size_key(name: str) -> str:
    """
    Pull all numeric size/capacity tokens from a product name and return
    a sorted, canonical string that can be used as part of a dedup key.

    Examples
    --------
    '4 L Silver'          -> '4l'
    '5 Litres'            -> '5l'
    '9litres'             -> '9l'
    '11 Litres'           -> '11l'
    '15 Litres(C15)'      -> '15l'
    '500ml Bottle'        -> '500ml'
    '3kg + 5kg'           -> '3kg+5kg'
    'no size here'        -> ''
    """
    tokens = []
    for m in _SIZE_UNIT_PATTERN.finditer(name.lower()):
        full = m.group(0).replace(",", ".").replace(" ", "")
        # separate number from unit
        num_m = re.match(r"([\d.]+)(.*)", full)
        if num_m:
            num = num_m.group(1).rstrip(".")
            unit = num_m.group(2).strip().lower()
            unit = _SIZE_UNIT_NORMALISE.get(unit, unit)
            tokens.append(f"{num}{unit}")
        else:
            tokens.append(full)
    return "+".join(sorted(set(tokens))) if tokens else ""


def check_duplicate_products(
    data: pd.DataFrame,
    exempt_categories: List[str] = None,
    similarity_threshold: float = 0.70,
    known_colors: List[str] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Flags duplicate products using TWO complementary signals:

    1. IMAGE signal (primary)
       same seller + same MAIN_IMAGE URL/path + same color-in-name + same size
       → definite duplicate regardless of minor name wording differences

    2. TEXT signal (fallback / additional coverage)
       same seller + brand + normalised name + color + size
       → catches renames / products without images

    Guards that PREVENT false positives
    ------------------------------------
    • Size guard  : 4L vs 5L pressure cooker → different _size_key  → NOT flagged
    • Color guard : Red vs Blue headlamp     → different _color_key → NOT flagged
      (color is extracted from the NAME first, then falls back to the COLOR column)
    """
    if not {"NAME", "SELLER_NAME", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    d = data.copy()
    if exempt_categories and "CATEGORY_CODE" in d.columns:
        d = d[
            ~d["_cat_clean"].isin(
                set(clean_category_code(c) for c in exempt_categories)
            )
        ]
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    # ── compile color pattern from known_colors list ──────────────────────────
    _color_pat = (
        re.compile(
            r"\b("
            + "|".join(
                re.escape(c) for c in sorted(known_colors, key=len, reverse=True)
            )
            + r")\b",
            re.IGNORECASE,
        )
        if known_colors
        else None
    )

    # ── 1. Size key — must be extracted BEFORE any digit-stripping ────────────
    d["_size_key"] = d["NAME"].astype(str).apply(_extract_size_key)

    # ── 2. Color key — extracted from the NAME first, then the COLOR column ───
    _names_lower = d["NAME"].astype(str).str.lower()
    if _color_pat:
        _from_name = _names_lower.str.extract(_color_pat.pattern, flags=re.IGNORECASE, expand=False).str.lower().str.strip().fillna("")
    else:
        _from_name = pd.Series("", index=d.index)
    _fallback = pd.Series("", index=d.index)
    for _fc in ("COLOR", "COLOR_FAMILY"):
        if _fc in d.columns:
            _v = d[_fc].astype(str).str.strip().str.lower()
            _valid = ~_v.isin(["nan", "none", "", "n/a"]) & (_fallback == "")
            _fallback = _fallback.where(~_valid, _v)
    d["_color_key"] = _from_name.where(_from_name != "", _fallback)

    # ── 3. Normalised name (noise words + punctuation + spaces stripped) ───────
    d["_norm_name"] = d["NAME"].astype(str).str.lower()
    d["_norm_name"] = d["_norm_name"].str.replace(
        r"\b(new|sale|original|genuine|authentic|official|premium|quality|best|hot|2024|2025)\b",
        "",
        regex=True,
    )
    d["_norm_name"] = d["_norm_name"].str.replace(r"[^\w\s]", "", regex=True)
    d["_norm_name"] = d["_norm_name"].str.replace(r"\s+", "", regex=True)

    # ── accumulate flagged rows from both signals ─────────────────────────────
    flagged_indices: dict = {}  # idx -> comment string

    # ── SIGNAL A: PERCEPTUAL-HASH-BASED IMAGE CHECK ──────────────────────────
    # Uses the _IMAGE_HASH_CACHE filled by _fetch_all_image_dimensions during
    # the image-quality checks (stretched / blurry).  No extra downloads.
    #
    # Key = seller | phash | color-in-name | size
    # • Same seller, same image content (even different CDN URLs) → same hash → FLAGGED
    # • Same image but different size in name (4L vs 5L cooker)   → different key → safe
    # • Same image but different color in name (Red vs Blue)       → different key → safe
    #
    # Threshold: exact phash match (Hamming distance = 0).
    # Identical file uploaded twice always produces the same phash.
    if "MAIN_IMAGE" in d.columns:
        with _IMAGE_DIM_LOCK:
            _hash_snap = dict(_IMAGE_HASH_CACHE)  # thread-safe snapshot

        img_vals = d["MAIN_IMAGE"].astype(str).str.strip()
        _null_like = {"nan", "none", "", "n/a", "-", "null"}
        valid_img = (img_vals.str.len() > 5) & (~img_vals.str.lower().isin(_null_like))

        if valid_img.any():
            img_d = d[valid_img].copy()
            # Map each MAIN_IMAGE value to its perceptual hash
            img_d["_phash"] = img_vals[valid_img].map(lambda u: _hash_snap.get(u, ""))
            # Only proceed for rows where we actually have a hash
            has_hash = img_d["_phash"].str.len() > 0
            if has_hash.any():
                hash_d = img_d[has_hash].copy()
                hash_d["_img_key"] = (
                    hash_d["_seller_lower"]
                    + "|"
                    + hash_d["_phash"]
                    + "|"
                    + hash_d["_color_key"]
                    + "|"
                    + hash_d["_size_key"]
                )
                img_dup_mask = hash_d.duplicated(subset=["_img_key"], keep="first")
                if img_dup_mask.any():
                    _first_img = hash_d.drop_duplicates(
                        subset=["_img_key"], keep="first"
                    ).set_index("_img_key")["NAME"]
                    for idx in hash_d[img_dup_mask].index:
                        k = hash_d.loc[idx, "_img_key"]
                        flagged_indices[idx] = (
                            f"Duplicate (same image): '{str(_first_img.get(k, ''))[:40]}'"
                        )

    # ── SIGNAL B: TEXT-BASED ──────────────────────────────────────────────────
    # Same seller + brand + normalised name + color + size
    # Catches renamed copies and handles cases where no image URL is available.
    d["_text_key"] = (
        d["_seller_lower"]
        + "|"
        + d["_brand_lower"]
        + "|"
        + d["_norm_name"]
        + "|"
        + d["_color_key"]
        + "|"
        + d["_size_key"]
    )
    text_dup_mask = d.duplicated(subset=["_text_key"], keep="first")
    if text_dup_mask.any():
        _first_text = d.drop_duplicates(subset=["_text_key"], keep="first").set_index(
            "_text_key"
        )["NAME"]
        for idx in d[text_dup_mask].index:
            if idx not in flagged_indices:  # image signal takes priority for comment
                k = d.loc[idx, "_text_key"]
                flagged_indices[idx] = (
                    f"Duplicate: '{str(_first_text.get(k, ''))[:40]}'"
                )

    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)

    rdf = d.loc[list(flagged_indices.keys())].copy()
    rdf["Comment_Detail"] = rdf.index.map(flagged_indices)
    base_cols = data.columns.tolist()
    extra_cols = [c for c in ["Comment_Detail"] if c not in base_cols]
    return rdf[base_cols + extra_cols].drop_duplicates(subset=["PRODUCT_SET_SID"])


if _reg is not None:
    _reg.REGISTRY.update(
        {
            "check_restricted_brands": check_restricted_brands,
            "check_suspected_fake_products": check_suspected_fake_products,
            "check_refurb_seller_approval": check_refurb_seller_approval,
            "check_product_warranty": check_product_warranty,
            "check_seller_approved_for_books": check_seller_approved_for_books,
            "check_seller_approved_for_perfume": check_seller_approved_for_perfume,
            "check_perfume_tester": check_perfume_tester,
            "check_counterfeit_sneakers": check_counterfeit_sneakers,
            "check_counterfeit_jerseys": check_counterfeit_jerseys,
            "check_suspected_fake_perfume": check_suspected_fake_perfume,
            "check_prohibited_products": check_prohibited_products,
            "check_unnecessary_words": check_unnecessary_words,
            "check_single_word_name": check_single_word_name,
            "check_generic_brand_issues": check_generic_brand_issues,
            "check_fashion_brand_issues": check_fashion_brand_issues,
            "check_brand_in_name": check_brand_in_name,
            "check_wrong_variation": check_wrong_variation,
            "check_generic_with_brand_in_name": check_generic_with_brand_in_name,
            "check_missing_color": check_missing_color,
            "check_weight_volume_in_name": check_weight_volume_in_name,
            "check_incomplete_smartphone_name": check_incomplete_smartphone_name,
            "check_duplicate_products": check_duplicate_products,
        }
    )


def validate_products(
    data: pd.DataFrame,
    support_files: Dict,
    country_validator: CountryValidator,
    data_has_warranty_cols: bool,
    common_sids: Optional[set] = None,
    skip_validators: Optional[List[str]] = None,
    on_progress: Optional[callable] = None,
):
    data = data.copy()
    data["PRODUCT_SET_SID"] = data["PRODUCT_SET_SID"].astype(str).str.strip()

    if "_name_lower" not in data.columns:
        data["_name_lower"] = data["NAME"].astype(str).str.lower().fillna("")
    if "_brand_lower" not in data.columns:
        data["_brand_lower"] = (
            data["BRAND"].astype(str).str.lower().str.strip().fillna("")
        )
    if "_seller_lower" not in data.columns:
        data["_seller_lower"] = (
            data["SELLER_NAME"].astype(str).str.lower().str.strip().fillna("")
        )
    if "_cat_clean" not in data.columns:
        data["_cat_clean"] = data["CATEGORY_CODE"].apply(clean_category_code)
    if "_sid_clean" not in data.columns:
        data["_sid_clean"] = data["PRODUCT_SET_SID"]
    if "_norm_name" not in data.columns:
        data["_norm_name"] = data["NAME"].astype(str).map(normalize_text)

    validations = [
        (
            "Wrong Category",
            check_miscellaneous_category,
            {
                "categories_list": support_files.get("categories_names_list", []),
                "compiled_rules": st.session_state.get("compiled_json_rules", {}),
                "cat_path_to_code": support_files.get("cat_path_to_code", {}),
                "code_to_path": support_files.get("code_to_path", {}),
            },
        ),
        (
            "Restricted brands",
            check_restricted_brands,
            {"country_rules": support_files.get("restricted_brands_all", {}).get(country_validator.country, [])},
        ),
        (
            "Suspected Fake product",
            check_suspected_fake_products,
            {"suspected_fake_df": support_files.get("suspected_fake", {}).get(country_validator.code, pd.DataFrame())},
        ),
        (
            "Seller Not approved to sell Refurb",
            check_refurb_seller_approval,
            {
                "refurb_data": support_files.get("refurb_data", {}),
                "country_code": country_validator.code,
            },
        ),
        (
            "Product Warranty",
            check_product_warranty,
            {
                "warranty_category_codes": support_files.get(
                    "warranty_category_codes", []
                )
            },
        ),
        (
            "Seller Approve to sell books",
            check_seller_approved_for_books,
            {
                "books_data": support_files.get("books_data", {}),
                "country_code": country_validator.code,
                "book_category_codes": support_files.get("book_category_codes", []),
            },
        ),
        (
            "Seller Approved to Sell Perfume",
            check_seller_approved_for_perfume,
            {
                "perfume_category_codes": support_files.get(
                    "perfume_category_codes", []
                ),
                "perfume_data": support_files.get("perfume_data", {}),
                "country_code": country_validator.code,
            },
        ),
        (
            "Perfume Tester",
            check_perfume_tester,
            {
                "perfume_category_codes": support_files.get(
                    "perfume_category_codes", []
                ),
                "perfume_data": support_files.get("perfume_data", {}),
            },
        ),
        (
            "Counterfeit Sneakers",
            check_counterfeit_sneakers,
            {
                "sneaker_category_codes": support_files.get(
                    "sneaker_category_codes", []
                ),
                "sneaker_sensitive_brands": support_files.get(
                    "sneaker_sensitive_brands", []
                ),
            },
        ),
        (
            "Suspected counterfeit Jerseys",
            check_counterfeit_jerseys,
            {
                "jerseys_data": support_files.get("jerseys_data", {}),
                "country_code": country_validator.code,
            },
        ),
        (
            "Suspected Fake Perfume",
            check_suspected_fake_perfume,
            {
                "perfume_catalog": support_files.get("perfume_catalog", {}),
                "perfume_category_codes": support_files.get(
                    "perfume_category_codes", []
                ),
            },
        ),
        (
            "Prohibited products",
            check_prohibited_products,
            {"prohibited_rules": support_files.get("prohibited_words_all", {}).get(country_validator.code, [])},
        ),
        (
            "Unnecessary words in NAME",
            check_unnecessary_words,
            {
                "pattern": compile_regex_patterns(
                    support_files.get("unnecessary_words", [])
                )
            },
        ),
        (
            "Single-word NAME",
            check_single_word_name,
            {
                "book_category_codes": support_files.get("book_category_codes", []),
                "books_data": support_files.get("books_data", {}),
            },
        ),
        (
            "Generic BRAND Issues",
            check_generic_brand_issues,
            {"valid_category_codes_fas": support_files.get("category_fas", [])},
        ),
        (
            "Fashion brand issues",
            check_fashion_brand_issues,
            {
                "valid_category_codes_fas": support_files.get("category_fas", []),
                "code_to_path": support_files.get("code_to_path", {}),
            },
        ),
        ("BRAND name repeated in NAME", check_brand_in_name, {}),
        (
            "Wrong Variation",
            check_wrong_variation,
            {
                "allowed_variation_codes": list(
                    set(
                        support_files.get("variation_allowed_codes", [])
                        + support_files.get("category_fas", [])
                    )
                )
            },
        ),
        (
            "Generic branded products with genuine brands",
            check_generic_with_brand_in_name,
            {"brands_list": support_files.get("known_brands", [])},
        ),
        (
            "Missing COLOR",
            check_missing_color,
            {
                "pattern": compile_regex_patterns(support_files.get("colors", [])),
                "color_categories": support_files.get("color_categories", []),
                "country_code": country_validator.code,
            },
        ),
        (
            "Missing Weight/Volume",
            check_weight_volume_in_name,
            {"weight_category_codes": support_files.get("weight_category_codes", [])},
        ),
        (
            "Incomplete Smartphone Name",
            check_incomplete_smartphone_name,
            {
                "smartphone_category_codes": support_files.get(
                    "smartphone_category_codes", []
                )
            },
        ),
        (
            "Duplicate product",
            check_duplicate_products,
            {
                "exempt_categories": support_files.get("duplicate_exempt_codes", []),
                "known_colors": support_files.get("colors", []),
            },
        ),
        ("Image Stretched", check_image_stretched, {}),
        ("Image Blurry", check_image_blurry, {}),
        ("Image Mismatch", check_image_mismatch, {}),
        ("Image Infringing", check_image_infringing, {}),
        ("Image Too Many things displayed", check_image_too_many_things, {}),
        (
            "Discount too high",
            check_wrong_price,
            {"country_code": country_validator.code},
        ),
        (
            "Suspicious Discount",
            check_suspicious_discount,
            {"country_code": country_validator.code},
        ),
        ("ALL CAPS Product Name", check_all_caps_name, {}),
        ("Product Name Too Short", check_name_too_short, {}),
        ("Variation Name Mismatch", check_variation_name_consistency_polars, {}),
    ]

    if country_validator.code == "NG":
        _ng = support_files.get("ng_qc_rules", {})
        validations += [
            ("NG - Gift Card Seller", check_nigeria_gift_card, {"ng_rules": _ng}),
            ("NG - Books Seller", check_nigeria_books, {"ng_rules": _ng}),
            ("NG - TV Brand Seller", check_nigeria_tvs, {"ng_rules": _ng}),
            ("NG - HP Toners Seller", check_nigeria_hp_toners, {"ng_rules": _ng}),
            ("NG - Apple Seller", check_nigeria_apple, {"ng_rules": _ng}),
            ("NG - Xmas Tree Seller", check_nigeria_xmas_tree, {"ng_rules": _ng}),
            ("NG - Rice Brand Seller", check_nigeria_rice, {"ng_rules": _ng}),
            ("Powerbank Not Authorized", check_nigeria_powerbanks, {"ng_rules": _ng}),
        ]
    if country_validator.code in ("KE", "UG"):
        _ng = support_files.get("ng_qc_rules", {})
        validations += [("Powerbank Not Authorized", check_nigeria_powerbanks, {"ng_rules": _ng})]
    if country_validator.code == "MA":
        _ma = load_morocco_qc_rules()
        validations = [v for v in validations if v[0] != "Restricted brands"]
        validations.insert(1, ("Restricted brands", check_restricted_brands, {"country_rules": _ma.get("restricted", [])}))
        ma_prohibited_rules = [{"keyword": kw, "categories": set()} for kw in _ma.get("prohibited_keywords", [])]
        validations = [v for v in validations if v[0] != "Prohibited products"]
        validations.append(("Prohibited products", check_prohibited_products, {"prohibited_rules": ma_prohibited_rules}))
        validations.append(("MA - Marque Interdite", check_morocco_prohibited_brands, {"ma_rules": _ma}))
    if country_validator.code == "GH":
        _gh = load_ghana_qc_rules()
        validations += [("GH - Smart Glasses with Camera", check_ghana_smart_glasses, {"gh_rules": _gh})]

    results = {}
    rejected_sids: set = set()
    dup_groups = {}
    if {"NAME", "BRAND", "SELLER_NAME", "COLOR"}.issubset(data.columns):
        dt = data[["NAME", "BRAND", "SELLER_NAME", "COLOR", "PRODUCT_SET_SID"]].copy()
        dt["dup_key"] = (
            dt["NAME"].astype(str).str.strip().str.lower() + "||" +
            dt["BRAND"].astype(str).str.strip().str.lower() + "||" +
            dt["SELLER_NAME"].astype(str).str.strip().str.lower() + "||" +
            dt["COLOR"].astype(str).str.strip().str.lower()
        )
        for k, v in dt.groupby("dup_key")["PRODUCT_SET_SID"].apply(list).items():
            if len(v) > 1:
                for sid in v:
                    dup_groups[sid] = v

    _overall_data_hash = df_hash(data)
    EXPENSIVE_VALIDATORS = {
        "Image Stretched", "Image Blurry", "Image Mismatch", "Image Infringing",
        "Image Too Many things displayed", "Duplicate product", "Wrong Category",
        "Variation Name Mismatch"
    }
    _skip_set = {s.lower() for s in (skip_validators or [])}
    if not data_has_warranty_cols:
        # Warranty columns (PRODUCT_WARRANTY, WARRANTY_DURATION) only exist in the
        # third file format. Skip the native check when they're absent so products
        # aren't incorrectly flagged — the prefetch ZIP check covers this instead.
        _skip_set.add("product warranty")
    _needs_image_cache = any(v[0].lower() not in _skip_set and v[1] in (check_image_stretched, check_image_blurry, check_duplicate_products) for v in validations)
    _image_cache = _fetch_all_image_dimensions(data) if _needs_image_cache else {}

    total_tasks = len([v for v in validations if v[0].lower() not in _skip_set and not country_validator.should_skip_validation(v[0])])
    processed_count = 0
    restricted_keys = {}
    validation_errors = []

    def run_batch(v_list, current_data):
        nonlocal processed_count
        batch_results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 4) * 4)) as executor:
            future_to_name = {}
            for name, func, kwargs in v_list:
                if name.lower() in _skip_set or country_validator.should_skip_validation(name):
                    continue

                working_data = current_data
                if name in EXPENSIVE_VALIDATORS and rejected_sids:
                    working_data = current_data[~current_data["PRODUCT_SET_SID"].astype(str).isin(rejected_sids)]
                    if working_data.empty:
                        processed_count += 1
                        if on_progress: on_progress(name, processed_count, total_tasks)
                        continue

                ckwargs = {"data": working_data, **kwargs}
                if func in (check_image_stretched, check_image_blurry):
                    ckwargs["_image_cache"] = _image_cache
                
                flag_hash = hashlib.md5((_overall_data_hash + name).encode()).hexdigest()
                cache_path = os.path.join(FLAG_CACHE_DIR, f"{flag_hash}.pkl")
                future_to_name[executor.submit(run_cached_check, func, cache_path, ckwargs)] = name

            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]
                processed_count += 1
                if on_progress: on_progress(name, processed_count, total_tasks)
                try:
                    res = future.result()
                    if not res.empty and "PRODUCT_SET_SID" in res.columns:
                        res = res.loc[:, ~res.columns.duplicated()].copy()
                        res["PRODUCT_SET_SID"] = res["PRODUCT_SET_SID"].astype(str).str.strip()
                        
                        if name in [
                            "Seller Approve to sell books", "Seller Approved to Sell Perfume",
                            "Counterfeit Sneakers", "Seller Not approved to sell Refurb",
                            "Restricted brands", "MA - Marque Interdite", "GH - Smart Glasses with Camera"
                        ]:
                            res["match_key"] = create_match_key_vectorized(res)
                            restricted_keys.setdefault(name, set()).update(res["match_key"].unique())

                        _sids = set(res["PRODUCT_SET_SID"].unique())
                        _expanded = set()
                        for _s in _sids: _expanded.update(dup_groups.get(_s, [_s]))
                        
                        final_res = data[data["PRODUCT_SET_SID"].astype(str).isin(_expanded)].copy()
                        if "Comment_Detail" in res.columns:
                            _cd = res.set_index("PRODUCT_SET_SID")["Comment_Detail"].to_dict()
                            final_res["Comment_Detail"] = final_res["PRODUCT_SET_SID"].astype(str).map(_cd)
                        if "Reason" in res.columns:
                            _r = res.set_index("PRODUCT_SET_SID")["Reason"].to_dict()
                            final_res["Reason"] = final_res["PRODUCT_SET_SID"].astype(str).map(_r)
                        
                        batch_results[name] = final_res
                        rejected_sids.update(_expanded)
                    else:
                            results[name] = pd.DataFrame(columns=data.columns)
                except Exception as e:
                    logger.error(f"Validation error in '{name}': {e}")
                    validation_errors.append((name, str(e)))
        return batch_results

    # Run in two stages: Cheap then Expensive
    cheap_v = [v for v in validations if v[0] not in EXPENSIVE_VALIDATORS]
    expensive_v = [v for v in validations if v[0] in EXPENSIVE_VALIDATORS]

    results.update(run_batch(cheap_v, data))
    results.update(run_batch(expensive_v, data))

    if validation_errors:
        st.warning(f"{len(validation_errors)} validation checks encountered errors.")
        with st.expander("View Error Details"):
            for e_name, e_msg in validation_errors:
                st.error(f"**{e_name}**: {e_msg}")

    if restricted_keys:
        data["match_key"] = create_match_key_vectorized(data)
        for fname, keys in restricted_keys.items():
            extra = data[data["match_key"].isin(keys)].copy()
            results[fname] = pd.concat(
                [results.get(fname, pd.DataFrame()), extra]
            ).drop_duplicates(subset=["PRODUCT_SET_SID"])

    return derive_status_report(data, results, support_files, country_validator)


def derive_status_report(data, results, support_files, country_validator):
    flags_mapping = support_files.get("flags_mapping", {})
    validations = [
        ("Wrong Category", None, None),
        ("Restricted brands", None, None),
        ("Suspected Fake product", None, None),
        ("Seller Not approved to sell Refurb", None, None),
        ("Product Warranty", None, None),
        ("Seller Approve to sell books", None, None),
        ("Seller Approved to Sell Perfume", None, None),
        ("Perfume Tester", None, None),
        ("Counterfeit Sneakers", None, None),
        ("Suspected counterfeit Jerseys", None, None),
        ("Prohibited products", None, None),
        ("Unnecessary words in NAME", None, None),
        ("Single-word NAME", None, None),
        ("Generic BRAND Issues", None, None),
        ("Fashion brand issues", None, None),
        ("BRAND name repeated in NAME", None, None),
        ("Wrong Variation", None, None),
        ("Generic branded products with genuine brands", None, None),
        ("Missing COLOR", None, None),
        ("Missing Weight/Volume", None, None),
        ("Incomplete Smartphone Name", None, None),
        ("Duplicate product", None, None),
        ("Discount too high", None, None),
        # ("Category Max Price Exceeded", None, None),
        ("Suspicious Discount", None, None),
        ("NG - Gift Card Seller", None, None),
        ("NG - TV Brand Seller", None, None),
        ("NG - HP Toners Seller", None, None),
        ("NG - Apple Seller", None, None),
        ("NG - Xmas Tree Seller", None, None),
        ("NG - Rice Brand Seller", None, None),
        ("GH - Smart Glasses with Camera", None, None),
        ("MA - Marque Interdite", None, None),
        ("Powerbank Not Authorized", None, None),
    ]

    target_lang = "fr" if country_validator.country == "Morocco" else "en"

    rows = []
    processed = set()
    # Build a combined list of flags to check: hardcoded ones first (for priority), then any extra from results
    known_flags = [v[0] for v in validations]
    all_flags = known_flags + [f for f in results.keys() if f not in known_flags]

    for name in all_flags:
        if name not in results or results[name].empty:
            continue
        res = results[name].copy()

        # Ensure PRODUCT_SET_SID exists and is standardized
        if "PRODUCT_SET_SID" not in res.columns:
            if "ProductSetSid" in res.columns:
                res.rename(columns={"ProductSetSid": "PRODUCT_SET_SID"}, inplace=True)
            elif "sid" in res.columns.str.lower():
                sid_col = next(c for c in res.columns if c.lower() == "sid")
                res.rename(columns={sid_col: "PRODUCT_SET_SID"}, inplace=True)

        if "PRODUCT_SET_SID" not in res.columns:
            continue

        rinfo = flags_mapping.get(
            name,
            {
                "reason": "1000007 - Other Reason",
                "en": f"Flagged by {name}",
                "fr": f"Flagged by {name}",
                "ar": f"Flagged by {name}",
            },
        )
        base_comment = rinfo.get(target_lang, rinfo.get("en"))
        res["PRODUCT_SET_SID"] = res["PRODUCT_SET_SID"].astype(str).str.strip()

        # Merge with original data to get full details
        flagged = pd.merge(
            res[["PRODUCT_SET_SID", "Comment_Detail", "Reason"]]
            if "Reason" in res.columns and "Comment_Detail" in res.columns
            else (
                res[["PRODUCT_SET_SID", "Comment_Detail"]]
                if "Comment_Detail" in res.columns
                else res[["PRODUCT_SET_SID"]]
            ),
            data,
            on="PRODUCT_SET_SID",
            how="left",
        )
        if "Comment_Detail" not in flagged.columns and "Comment_Detail" in res.columns:
            if isinstance(res["Comment_Detail"], pd.DataFrame):
                flagged["Comment_Detail"] = res["Comment_Detail"].iloc[:, 0]
            else:
                flagged["Comment_Detail"] = res["Comment_Detail"]

        if "Reason" in res.columns:
            reason_map = res.set_index("PRODUCT_SET_SID")["Reason"].to_dict()
        else:
            reason_map = {}

        if "CAT_MAX_PRICE" in res.columns:
            _cat_max_map = res.set_index("PRODUCT_SET_SID")["CAT_MAX_PRICE"].to_dict()
        else:
            _cat_max_map = {}

        for _, r in flagged.iterrows():
            sid = str(r["PRODUCT_SET_SID"]).strip()
            if sid in processed:
                continue
            processed.add(sid)
            det = r.get("Comment_Detail", "")
            det_str = str(det) if pd.notna(det) and det else ""
            if name == "Powerbank Not Authorized":
                _pb_reason = reason_map.get(sid, "")
                _is_wrong_cat = (
                    "wrong category" in str(_pb_reason).lower()
                    or "power bank" in det_str.lower()
                    and "category" in det_str.lower()
                )
                if _is_wrong_cat:
                    rows.append(
                        {
                            "ProductSetSid": sid,
                            "ParentSKU": r.get("PARENTSKU", ""),
                            "Status": "Rejected",
                            "Reason": _pb_reason or "1000007 - Wrong Category",
                            "Comment": det_str
                            or flags_mapping.get("Wrong Category", rinfo).get(
                                target_lang, ""
                            ),
                            "FLAG": "Wrong Category",
                            "SellerName": r.get("SELLER_NAME", ""),
                        }
                    )
                    continue
            if det_str and len(det_str) > 60:
                comment_str = det_str
            elif det_str:
                comment_str = f"{base_comment} ({det_str})"
            else:
                comment_str = base_comment
            row_reason = reason_map.get(sid, rinfo["reason"])
            rows.append(
                {
                    "ProductSetSid": sid,
                    "ParentSKU": r.get("PARENTSKU", ""),
                    "Status": "Rejected",
                    "Reason": row_reason,
                    "Comment": comment_str,
                    "FLAG": name,
                    "SellerName": r.get("SELLER_NAME", ""),
                    "CAT_MAX_PRICE": _cat_max_map.get(sid, "")
                    if name == "Category Max Price Exceeded"
                    else "",
                }
            )

    for _, r in data[
        ~data["PRODUCT_SET_SID"].astype(str).str.strip().isin(processed)
    ].iterrows():
        sid = str(r["PRODUCT_SET_SID"]).strip()
        if sid not in processed:
            rows.append(
                {
                    "ProductSetSid": sid,
                    "ParentSKU": r.get("PARENTSKU", ""),
                    "Status": "Approved",
                    "Reason": "",
                    "Comment": "",
                    "FLAG": "",
                    "SellerName": r.get("SELLER_NAME", ""),
                }
            )
            processed.add(sid)
    final_df = pd.DataFrame(rows)
    # Standardize column naming for compatibility
    if "ProductSetSid" in final_df.columns:
        final_df["PRODUCT_SET_SID"] = final_df["ProductSetSid"]
    elif "PRODUCT_SET_SID" in final_df.columns:
        final_df["ProductSetSid"] = final_df["PRODUCT_SET_SID"]

    for c in [
        "ProductSetSid",
        "PRODUCT_SET_SID",
        "ParentSKU",
        "Status",
        "Reason",
        "Comment",
        "FLAG",
        "SellerName",
    ]:
        if c not in final_df.columns:
            final_df[c] = ""
    # Is_Zip / Is_Manual must exist from the start so render_flag_expander
    # can always select them without a KeyError (even before any status change).
    for _bool_col in ("Is_Zip", "Is_Manual"):
        if _bool_col not in final_df.columns:
            final_df[_bool_col] = False
    return country_validator.ensure_status_column(final_df), results


@st.cache_data(show_spinner=False, ttl=3600)
def cached_validate_products(
    data_hash: str,
    _data: pd.DataFrame,
    _support_files: Dict,
    country_code: str,
    data_has_warranty_cols: bool,
    skip_validators: Optional[List[str]] = None,
    _on_progress: Optional[callable] = None,
):
    country_name = next(
        (
            k
            for k, v in CountryValidator.COUNTRY_CONFIG.items()
            if v["code"] == country_code
        ),
        "Kenya",
    )
    cv = CountryValidator(country_name)
    return validate_products(
        _data,
        _support_files,
        cv,
        data_has_warranty_cols,
        skip_validators=skip_validators,
        on_progress=_on_progress,
    )


# -------------------------------------------------
# Register callables for the api_client direct-mode fallback.
# Placed here so that validate_products (defined above) is already in scope.
# api_client stores these in _DIRECT_FUNCS; _run_direct() reads from there
# instead of re-importing streamlit_app (which would create duplicate widget IDs).
# -------------------------------------------------
try:
    register_direct_pipeline(
        country_validator_cls=CountryValidator,
        validate_products_fn=validate_products,
        prefetch_map=PREFETCH_MAP,
        prefetch_key_fn=_prefetch_key_from_status_col,
        prefetch_reason_fn=_prefetch_reason_from_row,
    )
except Exception as _rdp_err:
    logger.warning("register_direct_pipeline failed: %s", _rdp_err)


# ==========================================
# APP INITIALIZATION & UI
# ==========================================

if "layout_mode" not in st.session_state:
    st.session_state.layout_mode = "wide"
if "ui_lang" not in st.session_state:
    st.session_state.ui_lang = "en"
if "final_report" not in st.session_state:
    st.session_state.final_report = pd.DataFrame()
if "all_data_map" not in st.session_state:
    st.session_state.all_data_map = pd.DataFrame()
if "all_data_rows" not in st.session_state:
    st.session_state.all_data_rows = pd.DataFrame()
if "post_qc_summary" not in st.session_state:
    st.session_state.post_qc_summary = pd.DataFrame()
if "post_qc_results" not in st.session_state:
    st.session_state.post_qc_results = {}
if "post_qc_data" not in st.session_state:
    st.session_state.post_qc_data = pd.DataFrame()
if "file_mode" not in st.session_state:
    st.session_state.file_mode = None
if "no_computation_zip" not in st.session_state:
    st.session_state.no_computation_zip = True
if "zip_qc_results" not in st.session_state:
    st.session_state.zip_qc_results = pd.DataFrame()
if "intersection_sids" not in st.session_state:
    st.session_state.intersection_sids = set()
if "intersection_count" not in st.session_state:
    st.session_state.intersection_count = 0
if "grid_page" not in st.session_state:
    st.session_state.grid_page = 0
if "grid_items_per_page" not in st.session_state:
    st.session_state.grid_items_per_page = 50
if "main_toasts" not in st.session_state:
    st.session_state.main_toasts = []
if "exports_cache" not in st.session_state:
    st.session_state.exports_cache = {}
if "do_scroll_top" not in st.session_state:
    st.session_state.do_scroll_top = False
if "display_df_cache" not in st.session_state:
    st.session_state.display_df_cache = {}
if "main_bridge_counter" not in st.session_state:
    st.session_state.main_bridge_counter = 0



try:
    st.set_page_config(page_title="Product Tool", layout=st.session_state.layout_mode)
except:
    pass

st_yled.init()


def _t(key):
    return get_translation(st.session_state.get("ui_lang", "en"), key)


rtl_css = (
    """
    div[data-testid="stTextArea"] textarea, div[data-testid="stTextInput"] input {
        direction: rtl !important;
        text-align: right !important;
    }
"""
    if st.session_state.get("ui_lang", "en") == "ar"
    else ""
)

st.markdown(
    f"""
    <style>
        {rtl_css}
        div[data-testid="stTextInput"]:has(input[placeholder="JTBRIDGE_UNIQUE_DO_NOT_USE"]),
        div[data-testid="stTextInput"]:has(input[placeholder="COUNTRY_BRIDGE_DO_NOT_USE"]) {{
            position: absolute !important; width: 1px !important; height: 1px !important;
            padding: 0 !important; margin: -1px !important; overflow: hidden !important;
            clip: rect(0, 0, 0, 0) !important; white-space: nowrap !important;
            border: 0 !important; opacity: 0 !important; z-index: -9999 !important;
        }}
        @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined');
        :root {{
            --jumia-orange: {JUMIA_COLORS["primary_orange"]};
            --jumia-red: {JUMIA_COLORS["jumia_red"]};
            --jumia-dark: {JUMIA_COLORS["dark_gray"]};
        }}
        header[data-testid="stHeader"] {{ background: transparent !important; }}
        div[data-testid="stStatusWidget"] {{ z-index: 9999999 !important; }}
        .stButton > button {{ border-radius: 4px; font-weight: 600; transition: all 0.3s ease; }}
        .stButton > button[kind="primary"] {{ background-color: {JUMIA_COLORS["primary_orange"]} !important; border: none !important; color: white !important; }}
        .stButton > button[kind="primary"]:hover {{ background-color: {JUMIA_COLORS["secondary_orange"]} !important; box-shadow: 0 4px 8px rgba(246, 139, 30, 0.3); transform: translateY(-1px); }}
        .stButton > button[kind="secondary"] {{ background-color: white !important; border: 2px solid {JUMIA_COLORS["primary_orange"]} !important; color: {JUMIA_COLORS["primary_orange"]} !important; }}
        .stButton > button[kind="secondary"]:hover {{ background-color: {JUMIA_COLORS["light_gray"]} !important; }}
        div[data-testid="stMetric"] {{
            background: {JUMIA_COLORS["light_gray"]}; border-radius: 0 0 8px 8px;
            padding: 12px 16px 16px 16px; text-align: center;
        }}
        div[data-testid="stMetricValue"] {{ color: {JUMIA_COLORS["dark_gray"]}; font-weight: 700; font-size: 26px !important; }}
        div[data-testid="stMetricLabel"] {{ color: {JUMIA_COLORS["medium_gray"]}; font-size: 11px; text-transform: uppercase; letter-spacing: 0.6px; font-weight: 600; }}
        ::-webkit-scrollbar {{ width: 18px !important; height: 18px !important; }}
        ::-webkit-scrollbar-track {{ background: {JUMIA_COLORS["light_gray"]}; border-radius: 8px; }}
        ::-webkit-scrollbar-thumb {{ background: {JUMIA_COLORS["medium_gray"]}; border-radius: 8px; border: 3px solid {JUMIA_COLORS["light_gray"]}; }}
        ::-webkit-scrollbar-thumb:hover {{ background: {JUMIA_COLORS["primary_orange"]}; }}
        * {{ scrollbar-width: auto; scrollbar-color: {JUMIA_COLORS["medium_gray"]} {JUMIA_COLORS["light_gray"]}; }}
        div[data-testid="stExpander"] {{ border: 1px solid {JUMIA_COLORS["border_gray"]}; border-radius: 8px; }}
        div[data-testid="stExpander"] summary {{ background-color: {JUMIA_COLORS["light_gray"]}; padding: 12px; border-radius: 8px 8px 0 0; }}
        h1, h2, h3 {{ color: {JUMIA_COLORS["dark_gray"]} !important; }}
        div[data-baseweb="segmented-control"] button {{ border-radius: 4px; }}
        div[data-baseweb="segmented-control"] button[aria-pressed="true"] {{ background-color: {JUMIA_COLORS["primary_orange"]} !important; color: white !important; }}
    </style>
""",
    unsafe_allow_html=True,
)

try:
    support_files = load_support_files_lazy()
    st.session_state.support_files = support_files
    st.session_state["compiled_json_rules"] = support_files.get(
        "compiled_json_rules", {}
    )
except Exception as e:
    st.error(f"Failed to load configs: {e}")
    st.stop()


def get_default_country():
    try:
        lang = st.context.headers.get("Accept-Language", "")
        if "KE" in lang:
            return "Kenya"
        if "UG" in lang:
            return "Uganda"
        if "NG" in lang:
            return "Nigeria"
        if "GH" in lang:
            return "Ghana"
        if "MA" in lang:
            return "Morocco"
    except:
        pass
    return "Kenya"


if "selected_country" not in st.session_state:
    st.session_state.selected_country = get_default_country()

if st.session_state.get("main_toasts"):
    for msg in st.session_state.main_toasts:
        if isinstance(msg, tuple):
            st.toast(msg[0], icon=msg[1])
        else:
            st.toast(msg)
    st.session_state.main_toasts.clear()


def get_image_base64(path):
    if os.path.exists(path):
        try:
            with open(path, "rb") as img_file:
                return base64.b64encode(img_file.read()).decode("utf-8")
        except:
            pass
    return ""


logo_base64 = get_image_base64("jumia logo.png") or get_image_base64("jumia_logo.png")
logo_html = (
    f"<img src='data:image/png;base64,{logo_base64}' style='height: 42px; margin-right: 15px;'>"
    if logo_base64
    else "<span class='material-symbols-outlined' style='font-size: 42px; margin-right: 15px;'>verified_user</span>"
)

st.markdown(
    f"""<div style='background: linear-gradient(135deg, {JUMIA_COLORS["primary_orange"]}, {JUMIA_COLORS["secondary_orange"]}); padding: 25px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 4px 12px rgba(246, 139, 30, 0.3);'><h1 style='color: white; margin: 0; font-size: 36px; display: flex; align-items: center;'>{logo_html}Product Validation Tool</h1></div>""",
    unsafe_allow_html=True,
)

with st.sidebar:
    lang_names = list(LANGUAGES.keys())
    current_lang_code = st.session_state.get("ui_lang", "en")
    current_lang_name = next(
        (k for k, v in LANGUAGES.items() if v == current_lang_code), "English"
    )
    selected_lang_name = st.selectbox(
        "Language / Langue / اللغة",
        lang_names,
        index=lang_names.index(current_lang_name),
    )
    new_lang_code = LANGUAGES[selected_lang_name]
    if new_lang_code != current_lang_code:
        st.session_state.ui_lang = new_lang_code
        st.rerun()
    st.markdown("---")
    st.header(_t("system_status"))
    if st.button(_t("clear_cache"), width='stretch', type="secondary"):
        st.cache_data.clear()
        st.session_state.display_df_cache = {}

        # 🚀 ROBUST CACHE CLEARING (Fixes WinError 32)
        def robust_cleanup(directory):
            if os.path.exists(directory):
                for root, dirs, files in os.walk(directory, topdown=False):
                    for name in files:
                        try:
                            os.remove(os.path.join(root, name))
                        except (PermissionError, OSError):
                            pass  # File locked by another process, skip for now
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except (PermissionError, OSError):
                            pass

        robust_cleanup(PARQUET_CACHE_DIR)
        robust_cleanup(FLAG_CACHE_DIR)
        st.toast("Cache cleared! (Locked files skipped)", icon="🧹")
        st.rerun()
    st.markdown("---")
    st.header(_t("display_settings"))
    new_mode = (
        "wide"
        if "Wide"
        in st.radio(
            "Layout Mode",
            ["Centered", "Wide"],
            index=1 if st.session_state.layout_mode == "wide" else 0,
        )
        else "centered"
    )
    if new_mode != st.session_state.layout_mode:
        st.session_state.layout_mode = new_mode
        st.rerun()

# ==========================================
# SECTION 1: UPLOAD & VALIDATION
# ==========================================
st.header(f":material/upload_file: {_t('upload_files')}", anchor=False)

current_country = st.session_state.get("selected_country", get_default_country())

# ── Flag SVG definitions (inline, no external files needed) ───────────────────
_FLAG_SVGS = {
    "Kenya": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <path fill="#006600" d="M0 0h512v512H0z"/>
  <path fill="#fff" d="M0 170.7h512v170.7H0z"/>
  <path fill="#000" d="M0 192h512v128H0z"/>
  <path fill="#c8102e" d="M224 256 80 160v192zm64 0 144-96v192z"/>
  <ellipse cx="256" cy="256" rx="30" ry="50" fill="#fff" stroke="#c8102e" stroke-width="8"/>
  <ellipse cx="256" cy="256" rx="18" ry="36" fill="#c8102e"/>
</svg>""",
    "Uganda": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <path fill="#000" d="M0 0h512v85.3H0z"/>
  <path fill="#fcdc04" d="M0 85.3h512v85.4H0z"/>
  <path fill="#c8102e" d="M0 170.7h512V256H0z"/>
  <path fill="#000" d="M0 256h512v85.3H0z"/>
  <path fill="#fcdc04" d="M0 341.3h512v85.4H0z"/>
  <path fill="#c8102e" d="M0 426.7h512V512H0z"/>
  <circle cx="256" cy="256" r="72" fill="#fff"/>
  <circle cx="256" cy="256" r="60" fill="#c8102e"/>
  <path fill="#000" d="M256 208c-13 0-22 8-22 18s6 14 14 20c-10 4-20 14-20 30h56c0-16-10-26-20-30 8-6 14-10 14-20s-9-18-22-18z"/>
</svg>""",
    "Nigeria": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <path fill="#008751" d="M0 0h170.7v512H0z"/>
  <path fill="#fff" d="M170.7 0h170.6v512H170.7z"/>
  <path fill="#008751" d="M341.3 0H512v512H341.3z"/>
</svg>""",
    "Ghana": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <path fill="#006b3f" d="M0 0h512v170.7H0z"/>
  <path fill="#fcd116" d="M0 170.7h512v170.6H0z"/>
  <path fill="#ce1126" d="M0 341.3h512V512H0z"/>
  <path fill="#000" d="M256 183l18 55h58l-47 34 18 55-47-34-47 34 18-55-47-34h58z"/>
</svg>""",
    "Morocco": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <path fill="#c1272d" d="M512 0H0v512h512z"/>
  <path fill="none" stroke="#006233" stroke-width="12.5" d="m256 191.4-38 116.8 99.4-72.2H194.6l99.3 72.2z"/>
</svg>""",
}


def _svg_to_b64(svg_str: str) -> str:
    encoded = base64.b64encode(svg_str.strip().encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"


_FLAG_DIR = Path("flags")
_FILE_MAP = {
    "Kenya": "ke",
    "Uganda": "ug",
    "Nigeria": "ng",
    "Ghana": "gh",
    "Morocco": "ma",
}
_flag_b64 = {}
for _cname, _code in _FILE_MAP.items():
    _svg_path = _FLAG_DIR / f"{_code}.svg"
    if _svg_path.exists():
        try:
            content = _svg_path.read_text(encoding="utf-8").strip()
            if content:
                _flag_b64[_cname] = _svg_to_b64(content)
            else:
                _flag_b64[_cname] = _svg_to_b64(_FLAG_SVGS[_cname])
        except Exception:
            _flag_b64[_cname] = _svg_to_b64(_FLAG_SVGS[_cname])
    else:
        _flag_b64[_cname] = _svg_to_b64(_FLAG_SVGS[_cname])

_countries = ["Kenya", "Uganda", "Nigeria", "Ghana", "Morocco"]
_O = JUMIA_COLORS["primary_orange"]

_flag_buttons_html = "".join(
    [
        f"""<button
        onclick="selectCountry('{c}')"
        id="btn-{c}"
        class="flag-btn {"active" if c == current_country else ""}"
        title="{c}">
      <img src="{_flag_b64[c]}" alt="{c} flag" class="flag-img">
      <span class="flag-label">{c}</span>
    </button>"""
        for c in _countries
    ]
)

_flag_selector_html = f"""
<style>
  body {{ margin: 0; padding: 0; background: transparent; }}
  .flag-bar {{
    display: flex; gap: 8px; align-items: center;
    padding: 6px 0; flex-wrap: wrap;
  }}
  .flag-btn {{
    display: flex; align-items: center; gap: 8px;
    padding: 7px 14px 7px 10px;
    border: 2px solid #e0e0e0;
    border-radius: 8px;
    background: #fff;
    cursor: pointer;
    font-family: sans-serif;
    font-size: 13px;
    font-weight: 600;
    color: #444;
    transition: border-color .15s, box-shadow .15s, background .15s;
    outline: none;
  }}
  .flag-btn:hover {{
    border-color: {_O};
    background: #fff8f2;
  }}
  .flag-btn.active {{
    border-color: {_O};
    background: #fff3e6;
    color: {_O};
    box-shadow: 0 0 0 3px rgba(255,136,0,.15);
  }}
  .flag-img {{
    width: 26px; height: 20px;
    border-radius: 3px;
    object-fit: cover;
    box-shadow: 0 1px 3px rgba(0,0,0,.2);
    flex-shrink: 0;
  }}
  .flag-label {{ white-space: nowrap; }}
</style>
<div class="flag-bar" id="flag-bar">
  {_flag_buttons_html}
</div>
<script>
function selectCountry(name) {{
  document.querySelectorAll('.flag-btn').forEach(b => b.classList.remove('active'));
  var btn = document.getElementById('btn-' + name);
  if (btn) btn.classList.add('active');

  try {{
    var par = window.parent;
    var inputs = par.document.querySelectorAll('input[type="text"]');
    var bridge = null;
    for (var i = 0; i < inputs.length; i++) {{
      if (inputs[i].placeholder === 'COUNTRY_BRIDGE_DO_NOT_USE') {{
        bridge = inputs[i]; break;
      }}
    }}
    if (!bridge) return;
    var setter = Object.getOwnPropertyDescriptor(par.HTMLInputElement.prototype, 'value').set;
    setter.call(bridge, name);
    bridge.dispatchEvent(new par.Event('input', {{bubbles: true}}));
    bridge.focus({{preventScroll: true}});
    bridge.dispatchEvent(new par.KeyboardEvent('keydown', {{bubbles:true,cancelable:true,key:'Enter',keyCode:13}}));
    bridge.dispatchEvent(new par.KeyboardEvent('keyup',   {{bubbles:true,cancelable:true,key:'Enter',keyCode:13}}));
    bridge.blur();
  }} catch(e) {{ console.error('country bridge error', e); }}
}}
</script>
"""

st.iframe(_flag_selector_html, height=85)

_country_bridge = st.text_input(
    "country_bridge",
    value="",
    placeholder="COUNTRY_BRIDGE_DO_NOT_USE",
    key=f"country_bridge_{st.session_state.get('country_bridge_counter', 0)}",
    label_visibility="collapsed",
)
if "country_bridge_counter" not in st.session_state:
    st.session_state.country_bridge_counter = 0

country_choice = (
    _country_bridge.strip() if _country_bridge.strip() in _countries else None
)

if country_choice and country_choice != current_country:
    st.session_state.selected_country = country_choice
    st.session_state.last_processed_files = None
    st.session_state.final_report = pd.DataFrame()
    st.session_state.all_data_map = pd.DataFrame()
    st.session_state.all_data_rows = pd.DataFrame()
    st.session_state.exports_cache = {}
    st.session_state.display_df_cache = {}

    st.session_state.ui_lang = "fr" if country_choice == "Morocco" else "en"
    st.session_state.country_bridge_counter += 1
    st.toast(f"Switching to {country_choice}…", icon=":material/public:")
    st.rerun()

country_validator = CountryValidator(st.session_state.selected_country)

_has_files = bool(st.session_state.get("cached_uploaded_files"))
if _has_files:
    if st.button(
        "Force re-validate",
        width='stretch',
        help="Bypass cache and run validation again",
    ):
        for uf in st.session_state.get("cached_uploaded_files", []):
            fhash = hashlib.sha256(uf["bytes"]).hexdigest()[:24]
            invalidate(country_validator.country, fhash)
        st.session_state.last_processed_files = None
        st.rerun()

# ── Clear-all shortcut ────────────────────────────────────────────────────────
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0
if _has_files:
    if st.button(
        "✕ Clear all files",
        key="clear_files_btn",
        type="secondary",
        help="Remove all uploaded files and reset the tool",
    ):
        st.session_state.cached_uploaded_files = []
        st.session_state.final_report = pd.DataFrame()
        st.session_state.all_data_map = pd.DataFrame()
        st.session_state.all_data_rows = pd.DataFrame()
        st.session_state.file_mode = None
        st.session_state.exports_cache = {}
        st.session_state.display_df_cache = {}
        st.session_state.zip_image_store = {}
        st.session_state.zip_image_index = {}
        st.session_state.zip_image_source_bytes = None
        st.session_state.last_processed_files = "empty"
        st.session_state.intersection_sids = set()
        st.session_state.intersection_count = 0
        st.session_state.grid_page = 0
        st.session_state.pop("_grid_page_contexts", None)
        st.session_state.pop("_grid_last_ctx", None)

        st.session_state.pop("_grid_review_data_cache", None)
        st.session_state.pop("_grid_warm_urls", None)
        _dead_keys = [
            k
            for k in st.session_state.keys()
            if k.startswith(("quick_rej_", "grid_chk_", "toast_", "_sf_"))
        ]
        for k in _dead_keys:
            del st.session_state[k]
        st.session_state.uploader_key += 1
        st.rerun()

uploaded_files = st.file_uploader(
    "Upload files",
    type=["csv", "xlsx", "zip"],
    accept_multiple_files=True,
    key=f"daily_files_{st.session_state.uploader_key}",
    label_visibility="collapsed",
)

if uploaded_files:
    st.session_state.cached_uploaded_files = [
        {"name": uf.name, "bytes": uf.read()} for uf in uploaded_files
    ]
elif uploaded_files is not None and len(uploaded_files) == 0:
    st.session_state.cached_uploaded_files = []
    st.session_state.final_report = pd.DataFrame()
    st.session_state.all_data_map = pd.DataFrame()
    st.session_state.all_data_rows = pd.DataFrame()
    st.session_state.file_mode = None
    st.session_state.exports_cache = {}
    st.session_state.display_df_cache = {}
    st.session_state.zip_image_store = {}
    st.session_state.zip_image_index = {}
    st.session_state.zip_image_source_bytes = None
    st.session_state.last_processed_files = "empty"

_large_file_threshold = 5_000
_large_file_skip_validations = [
    "Image Stretched",
    "Image Blurry",
    "Image Mismatch",
    "Image Infringing",
    "Image Too Many things displayed",
]
_total_estimated_rows = 0
for _fc in st.session_state.get("cached_uploaded_files", []):
    try:
        _peek = BytesIO(_fc["bytes"])
        _name_lower = _fc["name"].lower()
        if _name_lower.endswith(".zip"):
            with zipfile.ZipFile(_peek) as _zf:
                _qc_info = next(
                    (
                        info
                        for info in _zf.infolist()
                        if "qc_results" in info.filename.lower()
                        and info.filename.lower().endswith((".xlsx", ".xls", ".csv"))
                    ),
                    None,
                )
                if _qc_info:
                    if _qc_info.filename.lower().endswith(".csv"):
                        _total_estimated_rows += _zf.read(_qc_info).count(b"\n")
                    else:
                        _total_estimated_rows += max(1, _qc_info.file_size // 500)
                else:
                    _total_estimated_rows += max(1, len(_fc["bytes"]) // 500)
        elif _name_lower.endswith(".xlsx"):
            _total_estimated_rows += pd.read_excel(
                _peek, engine="openpyxl", nrows=1, dtype=str
            ).shape[0]
            _total_estimated_rows += max(0, len(_fc["bytes"]) // 500 - 1)
        else:
            _total_estimated_rows += _fc["bytes"].count(b"\n")
    except Exception:
        pass

if _total_estimated_rows > _large_file_threshold:
    st.info(
        f"**Large file detected** (~{_total_estimated_rows:,} rows estimated) — "
        "validation may take 30–60 seconds. Image checks run in parallel to keep things fast.",
        icon=":material/hourglass_top:",
    )

_files_for_processing = st.session_state.get("cached_uploaded_files", [])
process_signature = (
    str(
        sorted(
            [
                f["name"] + hashlib.md5(f["bytes"]).hexdigest()
                for f in _files_for_processing
            ]
        )
    )
    + f"_{country_validator.code}"
    if _files_for_processing
    else "empty"
)

if st.session_state.get("last_processed_files") != process_signature:
    st.session_state.final_report = pd.DataFrame()
    st.session_state.all_data_map = pd.DataFrame()
    st.session_state.all_data_rows = pd.DataFrame()
    st.session_state.file_mode = None
    st.session_state.intersection_sids = set()
    st.session_state.intersection_count = 0
    st.session_state.grid_page = 0
    st.session_state.pop("_grid_page_contexts", None)
    st.session_state.pop("_grid_last_ctx", None)
    st.session_state.exports_cache = {}
    st.session_state.display_df_cache = {}

    st.session_state.pop("_grid_review_data_cache", None)
    st.session_state.pop("_grid_warm_urls", None)
    keys_to_delete = [
        k
        for k in st.session_state.keys()
        if k.startswith(("quick_rej_", "grid_chk_", "toast_"))
    ]
    for k in keys_to_delete:
        del st.session_state[k]

    if process_signature == "empty":
        st.session_state.last_processed_files = "empty"
    else:
        _engine_for_cache = (
            _get_cat_matcher_engine() if _CAT_MATCHER_AVAILABLE else None
        )
        _learning_stamp = (
            str(len(_engine_for_cache.learning_db)) if _engine_for_cache else "0"
        )
        sig_hash = hashlib.md5(
            (process_signature + _learning_stamp + PROCESSING_CACHE_VERSION).encode()
        ).hexdigest()

        cached_data = load_df_parquet(f"{sig_hash}_data.parquet")
        cached_data_rows = load_df_parquet(f"{sig_hash}_data_rows.parquet")
        cached_report = load_df_parquet(f"{sig_hash}_report.parquet")

        if cached_data is not None and cached_report is not None:
            _prepare_lazy_zip_images(_files_for_processing)
            st.session_state.final_report = cached_report
            st.session_state.all_data_map = cached_data
            st.session_state.all_data_rows = (
                cached_data_rows if cached_data_rows is not None else cached_data.copy()
            )
            st.session_state.last_processed_files = process_signature
            st.toast("Loaded from cache", icon=":material/bolt:")
        else:
            try:
                with st.status("Processing files…", expanded=True) as _status:
                    st.write("Reading uploaded file(s)…")

                    # ── 1. Preserve manual approvals across re-validations ─────────
                    _manual_approvals: set = set()
                    if not st.session_state.final_report.empty:
                        _fr0 = st.session_state.final_report
                        if "Is_Manual" in _fr0.columns:
                            _manual_approvals = set(
                                _fr0[
                                    (_fr0["Status"] == "Approved") & (_fr0["Is_Manual"] == True)
                                ]["ProductSetSid"].astype(str).str.strip().unique()
                            )

                    # ── 2. Read ALL uploaded files; detect ZIP/QC sources inline ──
                    all_dfs: list = []
                    file_sids_sets: list = []
                    has_zip_source = False
                    st.session_state.zip_image_store = {}
                    st.session_state.zip_image_index = {}
                    st.session_state.zip_image_source_bytes = None
                    st.session_state.zip_qc_results = pd.DataFrame()
                    st.session_state.pop("_zip_sid_index", None)
                    st.session_state.pop("_zip_status_cols", None)
                    st.session_state.pop("_zip_prefetch_map", None)
                    _sid_col_qc: str | None = None   # SID column name inside the QC file

                    for uf in _files_for_processing:
                        _buf = BytesIO(uf["bytes"])
                        raw_data = pd.DataFrame()

                        if uf["name"].lower().endswith(".zip"):
                            # ZIP: extract embedded qc_results sheet + index images lazily
                            has_zip_source = True
                            with zipfile.ZipFile(_buf) as zf:
                                members = zf.infolist()
                                qc_file = next(
                                    (
                                        info for info in members
                                        if "qc_results" in info.filename.lower()
                                        and info.filename.lower().endswith((".xlsx", ".xls", ".csv"))
                                    ),
                                    None,
                                )
                                if qc_file:
                                    qc_data = zf.read(qc_file)
                                    st.session_state.zip_qc_results = (
                                        pd.read_csv(BytesIO(qc_data), dtype=str)
                                        if qc_file.filename.lower().endswith(".csv")
                                        else pd.read_excel(BytesIO(qc_data), dtype=str)
                                    )
                                    _build_zip_sid_index(st.session_state.zip_qc_results)
                                    raw_data = st.session_state.zip_qc_results.copy()
                                st.session_state.zip_image_index = _index_zip_images(zf)
                                st.session_state.zip_image_source_bytes = uf["bytes"]

                        elif any(k in uf["name"].lower() for k in ("qc_results", "qc_result")):
                            # Standalone QC-results file uploaded alongside product file
                            has_zip_source = True
                            st.session_state.zip_qc_results = (
                                _detect_and_read_csv(_buf)
                                if uf["name"].lower().endswith(".csv")
                                else pd.read_excel(_buf, engine="openpyxl", dtype=str)
                            )
                            _build_zip_sid_index(st.session_state.zip_qc_results)
                            raw_data = st.session_state.zip_qc_results.copy()

                        elif uf["name"].lower().endswith(".xlsx"):
                            raw_data = pd.read_excel(_buf, engine="openpyxl", dtype=str)
                        else:
                            raw_data = _detect_and_read_csv(_buf)

                        if not raw_data.empty:
                            raw_data = _repair_mojibake(raw_data)
                            all_dfs.append(raw_data)

                    st.session_state.no_computation_zip = has_zip_source

                    if not all_dfs:
                        raise ValueError("No data could be read from the uploaded file(s).")

                    # ── 3. Detect file mode (pre_qc vs post_qc) ───────────────────
                    _file_mode = "pre_qc"
                    try:
                        _file_mode = detect_file_type(all_dfs[0]) if "detect_file_type" in dir() or "detect_file_type" in globals() else "pre_qc"
                    except Exception:
                        pass
                    st.session_state.file_mode = _file_mode

                    if _file_mode == "post_qc":
                        _status.update(label="Post-QC file detected", state="complete", expanded=False)
                        st.info("Post-QC file detected. Please use the Post-QC section.", icon=":material/fact_check:")
                        st.session_state.last_processed_files = process_signature
                    else:
                        # ── 4. Merge, standardise, propagate, filter ──────────────
                        st.write("Standardising and merging data…")
                        std_dfs = []
                        for _raw in all_dfs:
                            _std = standardize_input_data(_raw)
                            if "PRODUCT_SET_SID" in _std.columns:
                                _std["PRODUCT_SET_SID"] = _std["PRODUCT_SET_SID"].astype(str).str.strip()
                                file_sids_sets.append(set(_std["PRODUCT_SET_SID"].unique()))
                            # Mark rows that actually came from a file with warranty columns
                            _std["_has_warranty_data"] = (
                                "PRODUCT_WARRANTY" in _std.columns or "WARRANTY_DURATION" in _std.columns
                            )
                            std_dfs.append(_std)

                        merged_data = pd.concat(std_dfs, ignore_index=True)
                        st.session_state.intersection_sids = (
                            set.intersection(*file_sids_sets) if len(file_sids_sets) > 1 else set()
                        )
                        st.session_state.intersection_count = len(st.session_state.intersection_sids)

                        st.write("Validating file schema…")
                        data_prop = propagate_metadata(merged_data)
                        is_valid, errors = validate_input_schema(data_prop)

                        if not is_valid:
                            _status.update(label="Schema validation failed", state="error", expanded=True)
                            for _ve in errors:
                                st.error(_ve)
                            st.session_state.last_processed_files = "error"
                            st.stop()

                        data_filtered, det_names = filter_by_country(data_prop, country_validator)
                        if data_filtered.empty:
                            _status.update(label="No matching products found", state="error", expanded=True)
                            _det_msg = f"No {country_validator.country} products found."
                            if det_names:
                                _det_msg += f" Detected SKUs belong to: **{', '.join(det_names)}**."
                            st.error(_det_msg, icon=":material/error:")
                            if det_names:
                                if st.button(f"Switch to {det_names[0]} and Reprocess", type="primary", icon=":material/swap_horiz:"):
                                    st.session_state.selected_country = det_names[0]
                                    st.session_state.country_bridge_counter += 1
                                    st.rerun()
                            st.stop()

                        if len(det_names) > 1 or (det_names and det_names[0] != country_validator.country):
                            st.toast(f"Multiple countries detected: {', '.join(det_names)}", icon=":material/info:")

                        # Variation counts
                        actual_counts = data_filtered.groupby("PRODUCT_SET_SID")["PRODUCT_SET_SID"].transform("count")
                        if "COUNT_VARIATIONS" in data_filtered.columns:
                            file_counts = pd.to_numeric(data_filtered["COUNT_VARIATIONS"], errors="coerce").fillna(1)
                            data_filtered["COUNT_VARIATIONS"] = actual_counts.combine(file_counts, max)
                        else:
                            data_filtered["COUNT_VARIATIONS"] = actual_counts

                        # Clean columns on the full filtered dataset first
                        for _c in ["NAME", "BRAND", "COLOR", "SELLER_NAME", "CATEGORY_CODE", "LIST_VARIATIONS"]:
                            if _c in data_filtered.columns:
                                data_filtered[_c] = data_filtered[_c].astype(str).fillna("")
                        if "COLOR_FAMILY" not in data_filtered.columns:
                            data_filtered["COLOR_FAMILY"] = ""

                        data = data_filtered.drop_duplicates(subset=["PRODUCT_SET_SID"], keep="first")
                        data_has_warranty = all(c in data.columns for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"])

                        # ── 5. Locate SID column inside the QC file ───────────────
                        qc_zip = st.session_state.zip_qc_results
                        zip_sids: set = set()
                        if has_zip_source and not qc_zip.empty:
                            _sid_col_qc = next(
                                (
                                    c for c in (
                                        "PRODUCT_SET_SID", "ProductSetSid",
                                        "Product Set SID", "cod_productset_sid", "SID",
                                    )
                                    if c in qc_zip.columns
                                ),
                                None,
                            )
                            if _sid_col_qc:
                                zip_sids = set(qc_zip[_sid_col_qc].astype(str).str.strip().unique())

                        all_sids = set(data["PRODUCT_SET_SID"].unique())
                        non_zip_sids = all_sids - zip_sids
                        fast_skip_list = _large_file_skip_validations if _total_estimated_rows > _large_file_threshold else []

                        st.write(f"Running validation on {len(all_sids)} products…")
                        if zip_sids:
                            st.write(f"ZIP/QC data: {len(zip_sids)} prefetched SKUs, {len(non_zip_sids)} additional SKUs.")

                        # ── 6. Validate non-ZIP SKUs (full checks) ────────────────
                        final_report_parts: list = []
                        results_parts: list = []

                        if non_zip_sids:
                            data_non_zip = data[data["PRODUCT_SET_SID"].isin(non_zip_sids)].copy()
                            
                            _prog = st.progress(0, text="Preparing validation...")
                            def _on_flag_done(flag_name: str, i: int, total: int):
                                _prog.progress(int(i / total * 100), text=f"Checking: {flag_name}")

                            fr_non_zip, res_non_zip = cached_validate_products(
                                df_hash(data_non_zip) + country_validator.code,
                                data_non_zip, support_files, country_validator.code,
                                data_has_warranty, skip_validators=fast_skip_list,
                                _on_progress=_on_flag_done
                            )
                            _prog.empty()
                            final_report_parts.append(fr_non_zip)
                            results_parts.append(res_non_zip)

                        # ── 7. Validate ZIP SKUs (skip what's already prefetched) ─
                        if zip_sids:
                            data_zip = data[data["PRODUCT_SET_SID"].isin(zip_sids)].copy()
                            skip_list = sorted(
                                set(_derive_prefetched_skip_list(qc_zip)) | set(fast_skip_list)
                            )
                            _prog_zip = st.progress(0, text="Preparing ZIP validation...")
                            def _on_flag_done_zip(flag_name: str, i: int, total: int):
                                _prog_zip.progress(int(i / total * 100), text=f"Checking (ZIP): {flag_name}")

                            fr_zip, res_zip = cached_validate_products(
                                df_hash(data_zip) + country_validator.code + "_zip_optimized",
                                data_zip, support_files, country_validator.code,
                                data_has_warranty, skip_validators=skip_list,
                                _on_progress=_on_flag_done_zip
                            )
                            _prog_zip.empty()
                            final_report_parts.append(fr_zip)
                            results_parts.append(res_zip)

                        # Merge validator results
                        if final_report_parts:
                            final_report_subset = pd.concat(final_report_parts, ignore_index=True)
                            combined_results: dict = {}
                            for _r_dict in results_parts:
                                for _flag, _df_r in _r_dict.items():
                                    if _flag not in combined_results:
                                        combined_results[_flag] = _df_r
                                    else:
                                        combined_results[_flag] = pd.concat(
                                            [combined_results[_flag], _df_r], ignore_index=True
                                        )
                        else:
                            final_report_subset = pd.DataFrame(
                                columns=["ProductSetSid", "Status", "FLAG", "Comment", "Reason"]
                            )
                            combined_results = {}

                        # ── 8. Build BLANK baseline: every product starts Approved ─
                        final_report = pd.DataFrame({"ProductSetSid": data["PRODUCT_SET_SID"].unique()})
                        final_report["Status"] = "Approved"
                        final_report["FLAG"] = ""
                        final_report["Comment"] = ""
                        final_report["Reason"] = ""
                        final_report["Is_Zip"] = False
                        final_report["Is_Manual"] = False
                        final_report["PRODUCT_SET_SID"] = final_report["ProductSetSid"]

                        # ── 9. Apply ZIP prefetch rejections FIRST (Priority 1) ───
                        if has_zip_source and not qc_zip.empty and _sid_col_qc:
                            # Merge extra context columns from QC file into data for display
                            _extra_ctx = [
                                c for c in qc_zip.columns
                                if c not in data.columns
                                and "status" not in c.lower()
                                and c != _sid_col_qc
                            ]
                            for _ecol in _extra_ctx:
                                _emap = qc_zip.set_index(_sid_col_qc)[_ecol].to_dict()
                                data[_ecol] = data["PRODUCT_SET_SID"].astype(str).str.strip().map(_emap)
                            # Special: image1 → IMAGE1_ZIP fallback
                            if "image1" in qc_zip.columns and "IMAGE1_ZIP" not in data.columns:
                                _img1_map = qc_zip.set_index(_sid_col_qc)["image1"].to_dict()
                                data["IMAGE1_ZIP"] = data["PRODUCT_SET_SID"].astype(str).str.strip().map(_img1_map)

                            status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                            fmap = support_files.get("flags_mapping", {})
                            _fr_sid_to_idx = pd.Series(
                                final_report.index,
                                index=final_report["ProductSetSid"].astype(str).str.strip(),
                            ).to_dict()
                            _data_by_sid = {
                                sid: grp
                                for sid, grp in data.groupby(
                                    data["PRODUCT_SET_SID"].astype(str).str.strip(), sort=False
                                )
                            }
                            _zip_result_rows: dict = {}
                            _rej_in_zip = 0
                            _learned_count = 0
                            engine = _get_cat_matcher_engine()

                            # 🚀 Optimized: Vectorised prefetch mapping
                            status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                            
                            # ── 1. Identify Rejected Entries Efficiently ──
                            melted = qc_zip[[_sid_col_qc] + status_cols].melt(id_vars=_sid_col_qc, var_name="col", value_name="val")
                            melted["val_lower"] = melted["val"].astype(str).str.lower().str.strip()
                            rejected_entries = melted[melted["val_lower"] == "rejected"]
                            rejected_sids = set(rejected_entries[_sid_col_qc])

                            # ── 2. Smart AI Learning (Approved Rows Only) ──
                            if engine:
                                approved_df = qc_zip[~qc_zip[_sid_col_qc].isin(rejected_sids)]
                                for _, _r in approved_df.iterrows():
                                    _name = str(_r.get("NAME", "")).strip()
                                    _cat = str(_r.get("CATEGORY", "")).strip()
                                    if _name and _cat:
                                        engine.apply_learned_correction(_name, _cat, auto_save=False)
                                        _learned_count += 1

                            # ── 3. Process Rejections ──
                            # Index qc_zip for fast row access
                            qc_zip_indexed = qc_zip.set_index(_sid_col_qc)
                            
                            for _, mrow in rejected_entries.iterrows():
                                _sid = str(mrow[_sid_col_qc]).strip()
                                _col = mrow["col"]
                                if _sid not in qc_zip_indexed.index:
                                    continue
                                
                                _r = qc_zip_indexed.loc[_sid]
                                if isinstance(_r, pd.DataFrame):
                                    _r = _r.iloc[0] # handle potential duplicates in SID
                                
                                _base_key = _prefetch_key_from_status_col(_col)
                                _flag = PREFETCH_MAP.get(_base_key, _base_key.replace("_", " ").title())
                                _flag_pf = f"{_flag} (Prefetched)"
                                _comment = _prefetch_reason_from_row(_r, _col, qc_zip.columns)
                                
                                _mapped = fmap.get(_flag, {})
                                _reason_code = _mapped.get("reason", "1000007 - Other Reason")
                                _default_cmt = _mapped.get("comment", "Rejected")
                                _final_cmt = (
                                    _comment if (_comment and _comment.lower() != "rejected")
                                    else _default_cmt
                                )
                                
                                # Extra specificity for known columns
                                if _flag == "Wrong Category" and "Category_Check_Rejection_Reason" in qc_zip.columns:
                                    _cr = str(_r["Category_Check_Rejection_Reason"]).strip()
                                    if _cr and _cr.lower() not in ("nan", "rejected"):
                                        _final_cmt = _cr

                                _fidx = _fr_sid_to_idx.get(_sid)
                                if _fidx is not None:
                                    final_report.at[_fidx, "Status"] = "Rejected"
                                    final_report.at[_fidx, "FLAG"] = _flag_pf
                                    final_report.at[_fidx, "Comment"] = _final_cmt
                                    final_report.at[_fidx, "Reason"] = _reason_code
                                    final_report.at[_fidx, "Is_Zip"] = True
                                    _rej_in_zip += 1

                                    _row_grp = _data_by_sid.get(_sid)
                                    if _row_grp is not None and not _row_grp.empty:
                                        _row_data = _row_grp.copy()
                                        for _zcol in qc_zip.columns:
                                            _zcu = str(_zcol).strip().upper()
                                            if _zcu in ("INITIAL_CATEGORY", "CORRECT_CATEGORY", "SUGGESTED_CATEGORY", "AI_CATEGORY"):
                                                _row_data[_zcol] = _r[_zcol]
                                            elif (
                                                _zcol not in data.columns
                                                and "status" not in str(_zcol).lower()
                                                and "reason" not in str(_zcol).lower()
                                                and _zcol != _sid_col_qc
                                            ):
                                                _row_data[_zcol] = _r[_zcol]
                                        _row_data["Comment_Detail"] = _comment
                                        _zip_result_rows.setdefault(_flag, []).append(_row_data)

                                # Manual review override
                                if str(_r.get("Manual_Review", "")).lower() in ("true", "1", "yes"):
                                    _fidx = _fr_sid_to_idx.get(_sid)
                                    if _fidx is not None:
                                        final_report.at[_fidx, "Status"] = "Approved"
                                        final_report.at[_fidx, "FLAG"] = "Manual review"
                                        final_report.at[_fidx, "Comment"] = "Already Approved"
                                        final_report.at[_fidx, "Is_Zip"] = True

                            for _flag, _rows in _zip_result_rows.items():
                                _combined_r = pd.concat(_rows, ignore_index=True)
                                if _flag in combined_results and not combined_results[_flag].empty:
                                    combined_results[_flag] = pd.concat(
                                        [combined_results[_flag], _combined_r], ignore_index=True
                                    )
                                else:
                                    combined_results[_flag] = _combined_r

                            if _learned_count > 0 and engine:
                                engine.save_learning_db()
                                st.write(f"AI learned {_learned_count} new category mappings from ZIP.")
                            if _rej_in_zip > 0:
                                st.write(f"Successfully mapped {_rej_in_zip} rejections from ZIP/QC file.")
                                st.session_state.pop("_grid_review_data_cache", None)
                                st.session_state.display_df_cache.clear()

                        # ── 10. Apply App rejections (only for still-Approved SKUs) ─
                        if not final_report_subset.empty:
                            fmap = support_files.get("flags_mapping", {})
                            _rej_in_app = 0
                            for _, _row in final_report_subset.iterrows():
                                if _row["Status"] == "Rejected":
                                    _sid = str(_row["ProductSetSid"]).strip()
                                    _mask = final_report["ProductSetSid"].astype(str).str.strip() == _sid
                                    if _mask.any() and (final_report.loc[_mask, "Status"] == "Approved").any():
                                        final_report.loc[_mask, "Status"] = "Rejected"
                                        final_report.loc[_mask, "FLAG"] = _row["FLAG"]
                                        final_report.at[final_report[_mask].index[0], "Comment"] = (
                                            _row.get("Comment", _row.get("Comment_Detail", ""))
                                        )
                                        final_report.loc[_mask, "Reason"] = fmap.get(
                                            str(_row["FLAG"]), {}
                                        ).get("reason", "1000007 - Other Reason")
                                        _rej_in_app += 1
                            if _rej_in_app > 0:
                                st.write(f"App validation found {_rej_in_app} additional rejections.")

                        # ── 11. Finalize report columns ────────────────────────────
                        _parent_map = (
                            data.set_index("PRODUCT_SET_SID")["PARENTSKU"].to_dict()
                            if "PARENTSKU" in data.columns else {}
                        )
                        _seller_map = (
                            data.set_index("PRODUCT_SET_SID")["SELLER_NAME"].to_dict()
                            if "SELLER_NAME" in data.columns else {}
                        )
                        final_report["ParentSKU"] = (
                            final_report["ProductSetSid"].astype(str).str.strip().map(_parent_map).fillna("")
                        )
                        final_report["SellerName"] = (
                            final_report["ProductSetSid"].astype(str).str.strip().map(_seller_map).fillna("")
                        )
                        st.session_state.post_qc_results = combined_results

                        # ── 12. Apply preserved manual approvals (final override) ──
                        if _manual_approvals:
                            _ma_mask = (
                                final_report["ProductSetSid"].astype(str).str.strip().isin(_manual_approvals)
                            )
                            if _ma_mask.any():
                                final_report.loc[
                                    _ma_mask,
                                    ["Status", "Reason", "Comment", "FLAG", "Is_Manual", "Is_Zip"],
                                ] = ["Approved", "", "", "Approved by User", True, False]

                        # ── 13. Sync session state ─────────────────────────────────
                        st.session_state.final_report = final_report
                        st.session_state.all_data_map = data
                        st.session_state.all_data_rows = None  # Lazy
                        st.session_state._data_filtered_ref = data_filtered
                        st.session_state.last_processed_files = process_signature

                        # ── 14. Persistent caching ─────────────────────────────────
                        save_df_parquet(data, f"{sig_hash}_data.parquet")
                        save_df_parquet(data_filtered, f"{sig_hash}_data_rows.parquet")
                        save_df_parquet(final_report, f"{sig_hash}_report.parquet")
                        st.session_state.current_sig_hash = sig_hash
                        _prepare_lazy_zip_images(_files_for_processing)

                        # ── 15. Pre-warm review grid ───────────────────────────────
                        try:
                            from constants import GRID_COLS
                            _available_cols = [c for c in GRID_COLS if c in data.columns]
                            if "CATEGORY_CODE" in data.columns and "CATEGORY_CODE" not in _available_cols:
                                _available_cols.append("CATEGORY_CODE")
                            _valid_df = final_report[final_report["Status"] == "Approved"][["ProductSetSid"]]
                            _review_data = pd.merge(
                                _valid_df, data[_available_cols],
                                left_on="ProductSetSid", right_on="PRODUCT_SET_SID", how="left",
                            )
                            _code_to_path = support_files.get("code_to_path", {})
                            if _code_to_path and "CATEGORY_CODE" in _review_data.columns:
                                _review_data["CATEGORY"] = _review_data["CATEGORY_CODE"].apply(
                                    lambda c: _code_to_path.get(str(c).strip(), str(c)) if pd.notna(c) else ""
                                )
                            st.session_state["_grid_review_data_cache"] = _review_data
                            _warm_urls: set = set()
                            if "MAIN_IMAGE" in _review_data.columns:
                                for _url in _review_data.iloc[: 50 * 2]["MAIN_IMAGE"].astype(str):
                                    _url = _url.strip().replace("http://", "https://", 1)
                                    if _url.startswith("https"):
                                        _warm_urls.add(_url)
                            st.session_state["_grid_warm_urls"] = list(_warm_urls)
                        except Exception as _pw_err:
                            logger.warning("Grid pre-warm failed: %s", _pw_err)

                        _rej_count = int(final_report[final_report["Status"] == "Rejected"].shape[0])
                        _app_count = int(final_report[final_report["Status"] == "Approved"].shape[0])
                        _status.update(
                            label=f"Done — {_app_count:,} approved, {_rej_count:,} rejected",
                            state="complete",
                            expanded=False,
                        )

            except Exception as e:
                st.error(f"Processing error: {e}")
                st.code(traceback.format_exc())
                st.session_state.last_processed_files = "error"
                # do NOT call st.stop() here — let the page continue rendering

   


# -------------------------------------------------
# JTBRIDGE (HTML GRID MESSAGE HANDLER)
# -------------------------------------------------
@st.fragment
def handle_jtbridge():
    _bridge_val = st.text_input(
        "jtbridge",
        value="",
        placeholder="JTBRIDGE_UNIQUE_DO_NOT_USE",
        key=f"main_bridge_{st.session_state.main_bridge_counter}",
        label_visibility="collapsed",
    )

    if _bridge_val:
        try:
            _msg = json.loads(_bridge_val)
            if _msg.get("action") == "reject_comments":
                # Store auto-comments keyed by SID — picked up by the next reject action
                _ac = _msg.get("payload", {})
                if isinstance(_ac, dict):
                    if "pending_auto_comments" not in st.session_state:
                        st.session_state.pending_auto_comments = {}
                    st.session_state.pending_auto_comments.update(_ac)

            elif _msg.get("action") == "reject":
                _payload = _msg.get("payload", {})
                _auto_comments = st.session_state.pop("pending_auto_comments", {})
                if isinstance(_payload, dict) and _payload:
                    _rgroups = {}
                    for _sid, _rkey in _payload.items():
                        _rgroups.setdefault(_rkey, []).append(_sid)
                    _total = 0
                    for _rkey, _sids in _rgroups.items():
                        if _rkey.startswith("Other Reason (Custom): "):
                            _flag = "Other Reason (Custom)"
                            _code = "1000007 - Other Reason"
                            _cmt = _rkey.split(": ", 1)[1]
                        else:
                            _IMAGE_FLAG_FALLBACK = {
                                "REJECT_IMG_STRETCHED": "Image Stretched",
                                "REJECT_IMG_BLURRY": "Image Blurry",
                                "REJECT_IMG_MISMATCH": "Image Mismatch",
                                "REJECT_IMG_INFRINGING": "Image Infringing",
                                "REJECT_IMG_TOO_MANY": "Image Too Many things displayed",
                            }
                            _flag = REASON_MAP.get(_rkey) or _IMAGE_FLAG_FALLBACK.get(
                                _rkey, "Other Reason (Custom)"
                            )
                            _rinfo = support_files["flags_mapping"].get(
                                _flag,
                                {
                                    "reason": "1000007 - Other Reason",
                                    "en": "Manual rejection",
                                },
                            )
                            _code = _rinfo["reason"]
                            _cmt_lang = (
                                "fr"
                                if st.session_state.selected_country == "Morocco"
                                else "en"
                            )
                            _cmt = _rinfo.get(_cmt_lang, _rinfo.get("en"))

                        for _sid in _sids:
                            _sid_cmt = _auto_comments.get(_sid, _cmt)
                            apply_status_change(
                                [_sid],
                                status="Rejected",
                                reason=_code,
                                comment=_sid_cmt,
                                flag=_flag,
                                is_manual=True,
                                is_zip=False,
                            )
                        _total += len(_sids)

                    st.session_state.main_toasts.append(f"Rejected {_total} product(s)")
                    st.session_state.main_bridge_counter += 1
                    st.session_state.do_scroll_top = False
                    st.rerun(scope="fragment")

            elif _msg.get("action") == "undo":
                _payload = _msg.get("payload", {})
                _total_restored = 0
                if isinstance(_payload, dict):
                    for _sid in _payload.keys():
                        restore_single_item(_sid)
                        _total_restored += 1
                if _total_restored > 0:
                    st.session_state.main_bridge_counter += 1
                    st.session_state.do_scroll_top = False
                    st.rerun(scope="fragment")

        except Exception as _e:
            logger.error(f"Bridge parse error: {_e}")


# Call the fragment immediately
handle_jtbridge()


# ==========================================
# RESULTS SECTION
# ==========================================
@st.cache_data(show_spinner=False)
def get_enriched_results(fr_df, data_df):
    if fr_df.empty:
        return pd.DataFrame()
    return pd.merge(
        fr_df,
        data_df[["PRODUCT_SET_SID", "SELLER_NAME", "BRAND"]],
        left_on="ProductSetSid",
        right_on="PRODUCT_SET_SID",
        how="left",
    )


@st.fragment
def render_main_results():
    if (
        not _files_for_processing
        or st.session_state.final_report.empty
        or st.session_state.file_mode == "post_qc"
    ):
        return

    fr = st.session_state.final_report
    data = st.session_state.all_data_map

    # Enrich with metadata (cached)
    fr_meta = get_enriched_results(fr, data)
    if fr_meta.empty:
        return

    app_df = fr_meta[fr_meta["Status"] == "Approved"]
    rej_df = fr_meta[fr_meta["Status"] == "Rejected"]

    st.header(_t("val_results"), anchor=False)
    st.markdown('<div class="dashboard-marker"></div>', unsafe_allow_html=True)

    # 🚀 ENRICHED ANALYTICS DASHBOARD
    with st.expander(_t("dashboard"), expanded=False):
        # ── Calculations ──
        total_count = len(fr)
        auto_count = len(fr[fr["FLAG"] != "Manual review"])
        auto_rate = (auto_count / total_count * 100) if total_count > 0 else 0
        app_rate = (len(app_df) / total_count * 100) if total_count > 0 else 0
        rej_rate = (len(rej_df) / total_count * 100) if total_count > 0 else 0
        manual_hours = (auto_count * 3) / 60
        tool_runtime = 3.7

        # ── KPI Header ──
        render_summary_header(fr)

        # ── Visualization Row ──
        g1, g2 = st.columns(2)
        with g1:
            if not rej_df.empty:
                render_rejection_donut(fr)
            else:
                st.info("No rejections to visualize.")

        with g2:
            if not app_df.empty or not rej_df.empty:
                # Approval Rate over Categories
                cat_stats = (
                    fr_meta.groupby("BRAND")["Status"]
                    .value_counts(normalize=True)
                    .unstack()
                    .fillna(0)
                )
                if "Approved" in cat_stats.columns:
                    top_cats = cat_stats.sort_values("Approved", ascending=False).head(
                        10
                    )
                    fig_bar = px.bar(
                        top_cats,
                        y=top_cats.index,
                        x="Approved",
                        orientation="h",
                        title="Top 10 Brands by Approval Rate",
                        labels={"Approved": "Approval Rate"},
                        color_discrete_sequence=[JUMIA_COLORS["success_green"]],
                    )
                    fig_bar.update_layout(
                        height=300, margin=dict(t=30, l=10, r=10, b=10)
                    )
                    st.plotly_chart(
                        fig_bar,
                        width='stretch',
                        config={"displayModeBar": False},
                    )

        with g1:
            # 1. Validation Mix
            mix_df = pd.DataFrame(
                {
                    "Status": ["Approved", "Rejected"],
                    "Count": [len(app_df), len(rej_df)],
                }
            )
            fig_mix = px.pie(
                mix_df,
                values="Count",
                names="Status",
                hole=0.5,
                color="Status",
                color_discrete_map={"Approved": "#22c55e", "Rejected": "#ef4444"},
                title="Validation Mix",
            )
            fig_mix.update_layout(
                showlegend=False, margin=dict(t=40, b=0, l=0, r=0), height=280
            )
            st.plotly_chart(
                fig_mix, width='stretch', config={"displayModeBar": False}
            )

        with g2:
            # 2. Issues Breakdown
            if not rej_df.empty:
                flag_counts = rej_df["FLAG"].value_counts().head(8).reset_index()
                flag_counts.columns = ["Flag", "Count"]
                fig_flags = px.bar(
                    flag_counts,
                    x="Count",
                    y="Flag",
                    orientation="h",
                    title="Top Issues Breakdown",
                    color="Count",
                    color_continuous_scale="Reds",
                )
                fig_flags.update_layout(
                    showlegend=False,
                    margin=dict(t=40, b=0, l=0, r=0),
                    height=280,
                    yaxis={"categoryorder": "total ascending"},
                    coloraxis_showscale=False,
                )
                st.plotly_chart(
                    fig_flags,
                    width='stretch',
                    config={"displayModeBar": False},
                )
            else:
                st.info("No rejections to visualize.")

        # ── Seller & SKU Analytics ──
        s1, s2 = st.columns(2)

        with s1:
            if not rej_df.empty:
                seller_rej = rej_df["SELLER_NAME"].value_counts().head(10).reset_index()
                seller_rej.columns = ["Seller", "Rejections"]
                fig_seller = px.bar(
                    seller_rej,
                    x="Rejections",
                    y="Seller",
                    orientation="h",
                    title="Sellers at Risk (Rejection Count)",
                    color="Rejections",
                    color_continuous_scale="Oranges",
                )
                fig_seller.update_layout(
                    margin=dict(t=40, b=0, l=0, r=0),
                    height=300,
                    yaxis={"categoryorder": "total ascending"},
                    coloraxis_showscale=False,
                )
                st.plotly_chart(
                    fig_seller,
                    width='stretch',
                    config={"displayModeBar": False},
                )

        with s2:
            # 4. Automation Impact
            fig_savings = go.Figure(
                go.Indicator(
                    mode="gauge+number",
                    value=manual_hours,
                    title={"text": "Hours Saved (Estimate)", "font": {"size": 14}},
                    gauge={
                        "axis": {"range": [None, max(manual_hours * 1.5, 10)]},
                        "bar": {"color": "#00e5b0"},
                        "steps": [
                            {
                                "range": [0, manual_hours * 0.5],
                                "color": "rgba(0,229,176,0.1)",
                            },
                            {
                                "range": [manual_hours * 0.5, manual_hours],
                                "color": "rgba(0,229,176,0.2)",
                            },
                        ],
                    },
                )
            )
            fig_savings.update_layout(height=300, margin=dict(t=60, b=20, l=30, r=30))
            st.plotly_chart(
                fig_savings, width='stretch', config={"displayModeBar": False}
            )

    # 🚀 FLOATING SID LOOKUP & UNDO TOAST
    ut = st.session_state.get("show_undo_toast")
    if ut and (datetime.now() - ut["time"]).seconds < 5:
        with st.container():
            st.markdown(
                f"""<div style='position:fixed; bottom:20px; left:50%; transform:translateX(-50%); background:#333; color:#fff; padding:12px 24px; border-radius:8px; z-index:1000; display:flex; align-items:center; gap:15px; box-shadow:0 4px 12px rgba(0,0,0,0.3);'>
<span>{ut["status"]}d {ut["count"]} items</span>
<button onclick="window.parent.postMessage({{type:'streamlit:set_widget_value', key:'undo_trigger', value:true}}, '*')" style='background:{JUMIA_COLORS["primary_orange"]}; border:none; color:white; padding:4px 12px; border-radius:4px; cursor:pointer; font-weight:700;'>UNDO</button>
</div>""",
                unsafe_allow_html=True,
            )
            if st.button(
                "Internal Undo",
                key="undo_trigger",
                help="Click to undo",
                type="primary",
            ):
                if "undo_snapshot" in st.session_state:
                    st.session_state.final_report = st.session_state.undo_snapshot[
                        "final_report"
                    ]
                    st.session_state.pop("show_undo_toast", None)
                    st.rerun()

    lookup_col1, lookup_col2 = st.columns([2, 1])
    with lookup_col1:
        search_sid = st.text_input(
            "Quick SID Lookup",
            placeholder="Paste SID here to view details...",
            key="global_sid_lookup",
        )

    if search_sid:
        sid_match = data[
            data["PRODUCT_SET_SID"].astype(str).str.strip() == search_sid.strip()
        ]
        if not sid_match.empty:
            with st.expander(f"Quick View: {search_sid}", expanded=True):
                r = sid_match.iloc[0]
                v_cols = st.columns([1, 2])
                with v_cols[0]:
                    img_data = _get_image_from_zip(
                        r.get("NAME", ""), r.get("BRAND", ""), r.get("MAIN_IMAGE", "")
                    )
                    if img_data:
                        st.image(img_data)
                    else:
                        st.warning("No Image")
                with v_cols[1]:
                    st.write(f"**Name:** {r.get('NAME')}")
                    st.write(f"**Brand:** {r.get('BRAND')}")
                    st.write(
                        f"**Current Status:** {fr[fr['ProductSetSid'] == search_sid.strip()]['Status'].iloc[0]}"
                    )
                    if st.button("Approve Now", key="quick_app"):
                        apply_status_change(
                            [search_sid], status="Approved", flag="Manual Quick Approve"
                        )
                        st.rerun()

    st.subheader(_t("flags_breakdown"), anchor=False)
    group_by_seller = st.toggle(
        "Group by Seller",
        help="Toggle to group flagged products by seller instead of flag",
    )

    _blurry_commentary = st.session_state.get("_image_blurry_commentary", {})
    _commentary_in_scope = {
        sid: comment
        for sid, comment in _blurry_commentary.items()
        if fr[fr["ProductSetSid"] == sid]["Status"].eq("Approved").any()
    }
    if _commentary_in_scope:
        with st.expander(
            f"Low Resolution Advisory — {len(_commentary_in_scope)} product(s) (not rejected)",
            expanded=False,
        ):
            st.info(
                "These products have images between 201–299px. They have not been rejected, "
                "but image quality could be improved. Products ≤200px are automatically rejected as Image Blurry."
            )
            _advisory_rows = []
            for _sid, _comment in _commentary_in_scope.items():
                _row = data[data["PRODUCT_SET_SID"] == _sid]
                if not _row.empty:
                    _advisory_rows.append(
                        {
                            "PRODUCT_SET_SID": _sid,
                            "NAME": _row.iloc[0].get("NAME", ""),
                            "SELLER_NAME": _row.iloc[0].get("SELLER_NAME", ""),
                            "Resolution Note": _comment,
                        }
                    )
            if _advisory_rows:
                st.dataframe(
                    pd.DataFrame(_advisory_rows),
                    hide_index=True,
                    width='stretch',
                )
    if not rej_df.empty:
        if group_by_seller:
            for seller in sorted(rej_df["SELLER_NAME"].unique()):
                df_seller = rej_df[rej_df["SELLER_NAME"] == seller]
                seller_flags = df_seller["FLAG"].unique()
                with st.expander(
                    f"Seller: {seller} ({len(df_seller)} items, {len(seller_flags)} flags)"
                ):
                    # Inline stats for seller
                    wrong_cat_count = len(
                        df_seller[df_seller["FLAG"] == "Wrong Category"]
                    )
                    total_seller_items = len(data[data["SELLER_NAME"] == seller])
                    wrong_cat_pct = (
                        (wrong_cat_count / total_seller_items * 100)
                        if total_seller_items > 0
                        else 0
                    )

                    if wrong_cat_pct >= 40:
                        st.warning(
                            f"High Error Rate: {wrong_cat_pct:.1f}% of this seller's products have wrong categories."
                        )
                        sc1, sc2 = st.columns(2)
                        if sc1.button(
                            f"Approve All for {seller[:15]}", key=f"app_sel_{seller}"
                        ):
                            apply_status_change(
                                df_seller["ProductSetSid"].tolist(), status="Approved"
                            )
                            st.rerun()
                        if sc2.button(
                            f"Reject All for {seller[:15]}", key=f"rej_sel_{seller}"
                        ):
                            apply_status_change(
                                df_seller["ProductSetSid"].tolist(),
                                status="Rejected",
                                flag="Bulk Seller Reject",
                            )
                            st.rerun()

                    render_flag_expander(
                        f"Seller: {seller}",
                        df_seller,
                        data,
                        all(
                            c in data.columns
                            for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]
                        ),
                        support_files,
                        country_validator,
                        cached_validate_products,
                    )
        else:

            for title in rej_df["FLAG"].unique():
                df_flagged = rej_df[rej_df["FLAG"] == title]
                is_zip = "(Prefetched)" in title
                exp_label = f"[{len(df_flagged)}] {title}"
                if is_zip:
                    exp_label += " ⚡ ZIP"
                    st.markdown('<div class="dashboard-marker"></div>', unsafe_allow_html=True)
                with st.expander(exp_label, expanded=st.session_state.get(f"exp_{title}", False)):
                    st.html(flag_pill_header(title, len(df_flagged), is_zip=is_zip))
                    render_flag_expander(
                        title,
                        df_flagged,
                        data,
                        all(
                            c in data.columns
                            for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]
                        ),
                        support_files,
                        country_validator,
                        cached_validate_products,
                    )
    else:
        st.success("All products passed validation — no rejections found.")

    # ==========================================
    # CALL EXTERNAL RENDERERS
    # ==========================================
    render_image_grid(support_files)
    render_exports_section(support_files, country_validator)


render_main_results()
