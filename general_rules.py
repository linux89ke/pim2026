"""General QC rules — the file to edit when you meet a new broken pattern.

Each rule is one entry in RULES below. Add one, delete one, or set
``active=False`` to park it without losing the wording. Every rule becomes its
own flag: its own expander, its own line in the export, and it takes part in
the approval re-check and the audit exactly like the built-in checks.

Rules are matched against the CATEGORY the product is filed in, resolved from
the category map, so a rule written against a branch covers everything beneath
it. That matters — a seller who picks the "Shaving Gels" leaf instead of its
parent should not escape a rule written for the parent.

Keyword matching is whole-word by default. Substring matching is available but
has to be asked for, because it is the reliable way to produce nonsense: "ear"
inside search, clear, wear, earth; "air" inside hair, chair, repair. A previous
version of the counterfeit check matched "taser" inside "betaserc" for exactly
this reason.

A rule that raises is caught by the validator dispatch and logged rather than
taking the run down — but it then silently never fires, so check the rules
panel in the sidebar after editing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from data_utils import clean_category_code

logger = logging.getLogger(__name__)

WRONG_CATEGORY_REASON = "1000007 - Wrong Category"


@dataclass
class CategoryRule:
    """A product matching `keyword` is in the wrong place when filed under
    `wrong_in`. `belongs` is documentation for the reviewer — it is written
    into the comment, never used to move anything automatically.

    Leave `keyword` empty to reject *everything* filed under `wrong_in`, for a
    branch that should never receive listings at all. That is a much broader
    rule than a keyword one — it cannot be wrong about which products it hits,
    only about whether the branch really is off-limits — so the category paths
    want checking before it goes live."""

    id: str
    keyword: object                     # str, or a list of them — any one matching fires
    wrong_in: List[str]                 # category paths; a prefix covers the subtree
    comment: str
    flag: str = ""                      # defaults to a title derived from id
    # Matched against BRAND instead of NAME. Either side firing is enough:
    # sellers put the same thing in either field, and a product whose brand is
    # the giveaway often has an innocuous name.
    brand_keyword: object = None
    belongs: str = ""
    match: str = "word"                 # "word" | "substring"
    countries: Optional[List[str]] = None   # None = every market
    reason: str = WRONG_CATEGORY_REASON
    active: bool = True
    columns: List[str] = field(default_factory=lambda: ["NAME", "CATEGORY_CODE"])


@dataclass
class GenericCategoryRule:
    """Reject anything filed on a branch node that has children.

    "Small Appliances" is not a category a blender belongs in — it is the shelf
    the blender categories sit on. Filing on the parent when 87 subcategories
    exist below it is a miscategorisation even though the department is right.

    Only the node itself is matched, never its children: the whole point is
    that the children are the correct destinations."""

    id: str
    parent: str                         # exact category path of the branch node
    comment: str
    flag: str = ""
    countries: Optional[List[str]] = None
    reason: str = WRONG_CATEGORY_REASON
    active: bool = True
    columns: List[str] = field(default_factory=lambda: ["CATEGORY_CODE"])


@dataclass
class FdaRule:
    """Products that need FDA registration wherever they are filed.

    This is the follow-up to a category rule: once a product is somewhere
    plausible, being in the right place is not the end of it. A regulated
    product still has to carry its registration number, and the category rule
    has nothing to say about that.

    `except_in` skips the categories another rule already rejects, so a listing
    is not reported twice for two different reasons."""

    id: str
    keyword: object
    comment: str
    flag: str = ""
    brand_keyword: object = None
    except_in: List[str] = field(default_factory=list)
    match: str = "word"
    countries: Optional[List[str]] = None
    reason: str = "1000007 - Other Reason"
    active: bool = True
    columns: List[str] = field(default_factory=lambda: ["NAME", "CATEGORY_CODE", "FDA"])


# Values that mean "no FDA number", matching check_fda in streamlit_app.py.
_FDA_EMPTY = {"", "nan", "none", "nat", "n/a"}

# One list, used by both the miscategorisation rule and the FDA rule below.
# Kept in one place so adding a term covers both without having to remember
# that there are two.
SEXUAL_WELLNESS_TERMS = [
    "titan gel",
    "mki enlargement",
    "max men",
    "penis enlargement",
    "erectile dysfunction",
    "maxman gold",
    "delay gel",
    "mk improved",
    "libido boost",
    "mk provocative",
    "vagina tightening",
    # Both spellings: the source list has "Perfomance", which is how it is
    # often written on the listing itself, but the correct spelling turns up
    # just as often.
    "maximal perfomance",
    "maximal performance",
]

# Matched against BRAND, not NAME. Nigeria's restricted list already carries
# "Titan" as a brand in its own right, separate from "Titan Gel", so the brand
# field is a real signal and not a guess.
#
# Bare "titan" is deliberately absent from the NAME list above. Word matching
# already stops it hitting "titanium" — the lookahead fails on the following
# "i" — but "Titan" is also a large and entirely legitimate watch brand, and a
# name-wide match would reach far beyond these categories. Confined to BRAND
# and to the shave categories, a watch cannot be caught by it.
SEXUAL_WELLNESS_BRANDS = [
    "titan",
    "titan gel",
]

# The same signal, minus the ambiguous half, for rules that are NOT confined to
# a category. Bare "titan" only means what we want it to mean inside the shave
# branch; applied catalogue-wide it reads a Titan wristwatch as a sexual
# wellness product. Caught in testing: the FDA rule below covers everywhere
# except the shave categories, so with bare "titan" it rejected a Titan watch
# in Watches for having no FDA registration.
#
# "Titan Gel" as a brand has no such second life, so it stays.
SEXUAL_WELLNESS_BRANDS_UNAMBIGUOUS = [
    "titan gel",
]


# ── The rules ──────────────────────────────────────────────────────────────
RULES: List[CategoryRule] = [

    CategoryRule(
        id="iphone-under-android",
        flag="iPhone in Android Phones",
        keyword="iphone",
        wrong_in=["Phones & Tablets / Mobile Phones / Smartphones / Android Phones"],
        belongs="Phones & Tablets / Mobile Phones / Smartphones / iOS Phones",
        comment="iPhone filed under Android Phones — belongs in iOS Phones",
    ),

    # Both category trees, and the whole Shave & Hair Removal branch rather
    # than the single leaf that was reported. The catalogue carries two
    # parallel spellings of this path — one with a "Beauty & Personal Care"
    # level and one without — and the branch has four leaves under Shaving
    # Creams, Lotions & Gels alone. Naming one code would catch one filing and
    # miss the rest.
    CategoryRule(
        id="sexual-wellness-in-shaving",
        flag="Sexual wellness product in shaving category",
        keyword=SEXUAL_WELLNESS_TERMS,
        brand_keyword=SEXUAL_WELLNESS_BRANDS,
        wrong_in=[
            "Health & Beauty / Beauty & Personal Care / Personal Care / Shave & Hair Removal",
            "Health & Beauty / Personal Care / Shave & Hair Removal",
        ],
        comment="Sexual wellness product filed as a shaving product — not a shaving cream, lotion or gel",
    ),

    # Being somewhere plausible is not the end of it. These are regulated
    # products, so once they are out of the shaving branch the question stops
    # being where they are filed and becomes whether they carry a registration
    # number. except_in skips the shave categories the rule above already
    # rejects, so nothing is reported twice for two different reasons.
    FdaRule(
        id="sexual-wellness-needs-fda",
        flag="Sexual wellness product without FDA registration",
        keyword=SEXUAL_WELLNESS_TERMS,
        brand_keyword=SEXUAL_WELLNESS_BRANDS_UNAMBIGUOUS,
        except_in=[
            "Health & Beauty / Beauty & Personal Care / Personal Care / Shave & Hair Removal",
            "Health & Beauty / Personal Care / Shave & Hair Removal",
        ],
        comment="Sexual wellness product with no FDA registration number",
    ),

    # The parent of 87 subcategories. Filing directly on it is a
    # miscategorisation even though the department is right — a blender belongs
    # in Blenders, not on the shelf the blender categories sit on.
    GenericCategoryRule(
        id="small-appliances-parent",
        flag="Filed on Small Appliances instead of a subcategory",
        parent="Home & Office / Home & Kitchen / Kitchen & Dining / Small Appliances",
        comment="Filed on the Small Appliances parent category — a more specific "
                "subcategory exists and should be used",
    ),

    # ── From the weekly search-quality reports ─────────────────────────────
    # Only findings that describe a product-data defect are here. That report
    # also carries search-relevance findings — "ear" returning grooming
    # trimmers ahead of earphones, "air" returning air fresheners — where the
    # products are filed correctly and the complaint is about ranking. A rule
    # for those would reject correct listings, so they are deliberately absent.

    # Parked. Both resolve to 2,112 categories — the whole Tools & Home
    # Improvement branch — which is far more reach than the report's finding
    # justified, and neither has been tried against a real batch. Set
    # active=True to bring either back; nothing else needs changing.
    CategoryRule(
        id="fridge-in-tools",
        flag="Refrigerator in Tools & Home Improvement",
        keyword="fridge",
        wrong_in=["Home & Office / Tools & Home Improvement"],
        belongs="Electronics / Home Appliances",
        comment="Refrigerator filed under Tools & Home Improvement — belongs in Home Appliances",
        active=False,
    ),

    # "water" and "dispenser" were separate rows of the report describing the
    # same products; the report says so itself. One rule, or every dispenser
    # is flagged twice under two names.
    CategoryRule(
        id="dispenser-in-tools",
        flag="Water dispenser in Tools & Home Improvement",
        keyword="dispenser",
        wrong_in=["Home & Office / Tools & Home Improvement"],
        belongs="Electronics / Home Appliances",
        comment="Water dispenser filed under Tools & Home Improvement — belongs in Home Appliances",
        active=False,
    ),

    # No keyword: nothing belongs here. Sellers filing books under the DVD
    # branch is the pattern this catches, so it rejects the whole subtree
    # rather than trying to tell a book from a film by its title — which would
    # be guesswork, and would miss exactly the listings that are dressed up to
    # look like films.
    #
    # Scoped to DVDs alone. Its siblings under Books, Movies and Music —
    # Fiction, Magazines, Stationery, Bestselling Books and the rest — are
    # legitimate destinations and are untouched.
    CategoryRule(
        id="anything-in-dvds",
        flag="Listed under DVDs",
        keyword="",                       # every product in the branch
        wrong_in=["Books, Movies and Music / DVDs"],
        comment="Filed under DVDs — books and other products do not belong in the DVD categories",
        columns=["CATEGORY_CODE"],        # no NAME dependency: the category is the offence
    ),

    # Parked. A lavalier microphone is a legitimate Musical Instruments
    # accessory as often as it is a phone accessory, so the report's finding —
    # three listings — is thin ground for a rule that judges every one of them.
    # active=True brings it back.
    CategoryRule(
        id="phone-mic-in-instruments",
        flag="Phone microphone in Musical Instruments",
        keyword="lavalier",
        wrong_in=["Musical Instruments / Microphones & Accessories"],
        belongs="Phones & Tablets / Accessories",
        comment="Phone-accessory microphone filed under Musical Instruments",
        active=False,
    ),
]


# ── Engine ─────────────────────────────────────────────────────────────────

def _keyword_pattern(keyword, match: str) -> Optional[re.Pattern]:
    """One pattern for a keyword or a list of them. Any one matching fires."""
    words = [keyword] if isinstance(keyword, str) else list(keyword or [])
    parts = []
    for w in words:
        kw = str(w).strip()
        if not kw:
            continue
        kw = re.escape(kw)
        # Multi-word keywords tolerate any run of whitespace between the words,
        # so "titan  gel" and "titan gel" both match.
        kw = kw.replace(r"\ ", r"\s+")
        parts.append(kw)
    if not parts:
        return None
    body = "|".join(parts)
    if match == "substring":
        return re.compile(f"(?:{body})", re.IGNORECASE)
    # Lookarounds rather than \b: \b treats "-" as a boundary, so "titan-gel"
    # would match on \b and not here. Both readings are defensible; this one
    # is consistent with the rest of the app.
    return re.compile(rf"(?<!\w)(?:{body})(?!\w)", re.IGNORECASE)


def _codes_under(paths: List[str], code_to_path: Dict) -> set:
    """Every category code at or beneath any of `paths`."""
    out = set()
    for code, path in (code_to_path or {}).items():
        p = str(path).strip()
        for want in paths:
            w = str(want).strip()
            if p == w or p.startswith(w + " / "):
                out.add(clean_category_code(str(code)))
                break
    return out


def _cat_series(data):
    """Cleaned category codes, reusing _cat_clean when the pipeline made it."""
    if "_cat_clean" in data.columns:
        return data["_cat_clean"]
    return data["CATEGORY_CODE"].fillna("").astype(str).map(clean_category_code)


def _emit(data, hit, comment: str, reason: str):
    if not hit.any():
        return pd.DataFrame(columns=data.columns)
    flagged = data[hit].copy()
    flagged["Comment_Detail"] = comment
    flagged["Reason"] = reason
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


def _text_hit(data, pat, brand_pat):
    """NAME or BRAND matching. Either firing is enough — sellers put the
    same thing in either field, and a product whose brand gives it away
    often has a deliberately bland name."""
    hit = None
    if pat is not None and "NAME" in data.columns:
        hit = data["NAME"].fillna("").astype(str).str.contains(pat, na=False)
    if brand_pat is not None and "BRAND" in data.columns:
        b = data["BRAND"].fillna("").astype(str).str.contains(brand_pat, na=False)
        hit = b if hit is None else (hit | b)
    return hit


def _make_category_check(rule, codes: set):
    pat = _keyword_pattern(rule.keyword, rule.match)
    brand_pat = _keyword_pattern(getattr(rule, "brand_keyword", None), rule.match)

    def _check(data, **_kwargs):
        if data is None or data.empty or not codes:
            return pd.DataFrame(columns=getattr(data, "columns", []))
        if "CATEGORY_CODE" not in data.columns:
            return pd.DataFrame(columns=data.columns)
        in_scope = _cat_series(data).isin(codes)
        if not in_scope.any():
            return pd.DataFrame(columns=data.columns)
        if pat is None and brand_pat is None:
            hit = in_scope                      # no keyword: the category is the offence
        else:
            text = _text_hit(data, pat, brand_pat)
            if text is None:
                return pd.DataFrame(columns=data.columns)
            hit = in_scope & text
        detail = rule.comment
        if getattr(rule, "belongs", ""):
            detail = f"{detail} (should be: {rule.belongs})"
        return _emit(data, hit, detail, rule.reason)

    return _check


def _make_generic_check(rule, codes: set):
    def _check(data, **_kwargs):
        if data is None or data.empty or not codes:
            return pd.DataFrame(columns=getattr(data, "columns", []))
        if "CATEGORY_CODE" not in data.columns:
            return pd.DataFrame(columns=data.columns)
        return _emit(data, _cat_series(data).isin(codes), rule.comment, rule.reason)

    return _check


def _fda_missing(data):
    if "FDA" not in data.columns:
        # No FDA column at all: nothing carries a number, so every match counts.
        return pd.Series(True, index=data.index)
    v = data["FDA"].fillna("").astype(str).str.strip().str.lower()
    return v.isin(_FDA_EMPTY)


def _make_fda_check(rule, except_codes: set):
    pat = _keyword_pattern(rule.keyword, rule.match)
    brand_pat = _keyword_pattern(getattr(rule, "brand_keyword", None), rule.match)

    def _check(data, **_kwargs):
        if data is None or data.empty or (pat is None and brand_pat is None):
            return pd.DataFrame(columns=getattr(data, "columns", []))
        hit = _text_hit(data, pat, brand_pat)
        if hit is None:
            return pd.DataFrame(columns=data.columns)
        if except_codes and "CATEGORY_CODE" in data.columns:
            hit &= ~_cat_series(data).isin(except_codes)
        hit &= _fda_missing(data)
        return _emit(data, hit, rule.comment, rule.reason)

    return _check


def _flag_name(rule) -> str:
    return rule.flag or rule.id.replace("-", " ").title()


def _active(rule, country_code: str) -> bool:
    if not rule.active:
        return False
    if rule.countries and country_code and country_code not in rule.countries:
        return False
    return True


def _exact_codes(path: str, code_to_path: Dict) -> set:
    return {clean_category_code(str(c)) for c, p in (code_to_path or {}).items()
            if str(p).strip() == str(path).strip()}


def build_validators(support_files: Dict, country_code: str = "") -> List[tuple]:
    """Return (flag_name, fn, kwargs) tuples to append to the validator list."""
    code_to_path = (support_files or {}).get("code_to_path") or {}
    out = []
    for rule in RULES:
        if not _active(rule, country_code):
            continue

        if isinstance(rule, GenericCategoryRule):
            # Exact node only. Its children are the correct destinations, so
            # matching the subtree would reject the very filings this wants.
            codes = _exact_codes(rule.parent, code_to_path)
            if not codes:
                logger.warning("general_rules: %s did not resolve %s", rule.id, rule.parent)
                continue
            out.append((_flag_name(rule), _make_generic_check(rule, codes), {}))
            continue

        if isinstance(rule, FdaRule):
            # except_in may legitimately be empty — the rule then applies
            # everywhere, so an empty scope is not a failure here.
            out.append((_flag_name(rule),
                        _make_fda_check(rule, _codes_under(rule.except_in, code_to_path)), {}))
            continue

        codes = _codes_under(rule.wrong_in, code_to_path)
        if not codes:
            # The category map did not resolve any of the paths. Skipping is
            # right — a rule with no scope would match nothing anyway — but it
            # is reported in the panel so a typo in a path is visible rather
            # than looking like a rule that simply never fires.
            logger.warning("general_rules: %s matched no categories for %s",
                           rule.id, rule.wrong_in)
            continue
        out.append((_flag_name(rule), _make_category_check(rule, codes), {}))
    return out


def build_scopes(code_to_path: Dict, country_code: str = "") -> Dict:
    """Resolve every active rule's category paths to codes, once.

    audit_record() is called per product and there are thousands per batch,
    while resolving a path walks the whole category map. Doing it inside the
    loop would be tens of millions of comparisons per audit.
    """
    scopes = {}
    for rule in RULES:
        if not _active(rule, country_code):
            continue
        if isinstance(rule, GenericCategoryRule):
            wrong = _exact_codes(rule.parent, code_to_path)
        elif isinstance(rule, FdaRule):
            wrong = _codes_under(rule.except_in, code_to_path)
        else:
            wrong = _codes_under(rule.wrong_in, code_to_path)
        scopes[rule.id] = {
            "rule": rule,
            "wrong": wrong,
            "belongs": (_codes_under([rule.belongs], code_to_path)
                        if isinstance(rule, CategoryRule) and rule.belongs else set()),
            "pattern": (None if isinstance(rule, GenericCategoryRule)
                        else _keyword_pattern(rule.keyword, rule.match)),
            "brand_pattern": (None if isinstance(rule, GenericCategoryRule)
                              else _keyword_pattern(getattr(rule, "brand_keyword", None), rule.match)),
        }
    return scopes


def audit_record(rec: Dict, scopes: Dict, country_code: str = "") -> List[Dict]:
    """Judge a single ZIP record against the rules, for the targeted audit.

    Each entry carries `against`, naming which of the file's own checks the
    verdict should be compared with — a category rule disagrees with the file's
    category decision, an FDA rule with its FDA decision. Comparing an FDA
    finding against the category column would report a false approval every
    time the file happened to reject the product for something else.

    `kind` is one of:

      violation         — the rule fires. If the file did not reject, it missed it.
      correct_placement — the keyword matches and the product sits where the
                          rule says it belongs. If the file rejected it on
                          category, the file is wrong the other way.

    Only rules carrying `belongs` produce the second kind. A rule asserts where
    something must NOT be; without `belongs` it never asserts a placement is
    right, so reading a false rejection out of its silence would be unfounded.
    """
    out: List[Dict] = []
    if not rec or not scopes:
        return out
    code = clean_category_code(str(rec.get("CATEGORY_CODE", "") or ""))
    name = str(rec.get("NAME", "") or "")
    brand = str(rec.get("BRAND", "") or "")

    for scope in scopes.values():
        rule = scope["rule"]
        pat, brand_pat = scope["pattern"], scope.get("brand_pattern")
        if pat is not None or brand_pat is not None:
            if not ((pat is not None and pat.search(name))
                    or (brand_pat is not None and brand_pat.search(brand))):
                continue

        if isinstance(rule, FdaRule):
            if code and code in scope["wrong"]:
                continue                       # the category rule owns this one
            fda = str(rec.get("FDA", "") or "").strip().lower()
            if fda not in _FDA_EMPTY:
                continue
            out.append({"rule": _flag_name(rule), "kind": "violation", "against": "fda",
                        "reason_type": _flag_name(rule), "detail": rule.comment})
            continue

        if not code:
            continue
        if code in scope["wrong"]:
            detail = rule.comment
            if isinstance(rule, CategoryRule) and rule.belongs:
                detail = f"{detail} (should be: {rule.belongs})"
            out.append({"rule": _flag_name(rule), "kind": "violation", "against": "category",
                        "reason_type": _flag_name(rule), "detail": detail})
        elif scope["belongs"] and code in scope["belongs"]:
            out.append({"rule": _flag_name(rule), "kind": "correct_placement", "against": "category",
                        "reason_type": f"{_flag_name(rule)} — correctly placed",
                        "detail": f"Already filed under {rule.belongs}, which is where this "
                                  f"product belongs."})
    return out


def relevant_columns() -> Dict[str, List[str]]:
    """Column dependencies, for the flag cache. Without these an unmapped flag
    falls back to hashing the whole frame on every run — and a column a rule
    actually reads but does not declare is worse than that, because the cache
    would then serve a stale verdict after someone edited it."""
    out = {}
    for r in RULES:
        if not r.active:
            continue
        cols = list(r.columns)
        if getattr(r, "brand_keyword", None) and "BRAND" not in cols:
            cols.append("BRAND")
        out[_flag_name(r)] = cols
    return out


def rule_health(support_files: Dict) -> pd.DataFrame:
    """One row per rule, for the sidebar panel."""
    code_to_path = (support_files or {}).get("code_to_path") or {}
    rows = []
    for rule in RULES:
        if isinstance(rule, GenericCategoryRule):
            kind, keyword = "generic category", "(any product)"
            codes = _exact_codes(rule.parent, code_to_path) if rule.active else set()
            # An unresolved parent means the rule can never fire.
            status = "off" if not rule.active else ("path not found" if not codes else "ok")
        elif isinstance(rule, FdaRule):
            kind = "fda"
            keyword = _kw_label(rule.keyword)
            codes = _codes_under(rule.except_in, code_to_path) if rule.active else set()
            # except_in is an exclusion list; empty just means "applies
            # everywhere", so it is never a failure for this type.
            status = "off" if not rule.active else "ok"
        else:
            kind = "category"
            keyword = _kw_label(rule.keyword)
            codes = _codes_under(rule.wrong_in, code_to_path) if rule.active else set()
            status = ("off" if not rule.active
                      else "no categories matched" if not codes else "ok")
        rows.append({
            "Rule": _flag_name(rule),
            "Type": kind,
            "Keyword": keyword,
            "Categories": len(codes),
            "Active": rule.active,
            "Status": status,
        })
    return pd.DataFrame(rows)


def _kw_label(keyword) -> str:
    if isinstance(keyword, str):
        return keyword or "(any product)"
    words = list(keyword or [])
    if not words:
        return "(any product)"
    return f"{words[0]} +{len(words) - 1} more" if len(words) > 1 else words[0]
