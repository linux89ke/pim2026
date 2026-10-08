import logging
import re
import pandas as pd
import polars as pl
import json as _json
import os as _os
import requests as _requests
import time as _time
import streamlit as st
from typing import Dict, List
from data_utils import clean_category_code

logger = logging.getLogger(__name__)

# ── Shared volume/quantity regex ──────────────────────────────────────────────
# Used by BOTH the false-rejection check (approved products that had volume in
# the title and were wrongly rejected) AND the false-approval check (approved
# products that are missing volume when the category requires it).
# Keeping one shared pattern guarantees the two checks are always in sync.
_VOLUME_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:kg|kgs|g|gm|gms|grams?|mg|mcg|ml|mls|l|ltr|ltrs|liters?|litres?|cl|oz|ounces?|lb|lbs|fl\.?\s*oz)\b"
    r"|\b\d+\s*(?:tablets?|tabs?|capsules?|caps?|sachets?|count|ct|sticks?|iu"
    r"|tea\s*bags?|teabags?|softgels?|lozenges?|gummies|gummy|vials?|ampoules?|tubes?"
    r"|pieces?|pcs|pack|packs|pairs?|rolls?|sheets?|wipes?|pods?|units?|servings?)\b"
    r"|\b\d+['’]?s\b"
    r"|\b(?:a\s+)?dozen\b"
    r"|\b(?:pack|box|set|bundle|lot)\s+of\s+\d+\b"
    r"|\b\d+\s*x\s*\d+(?:\.\d+)?\s*(?:ml|mls|g|kg|l|oz|pcs|pieces)?\b"
    r"|\d+\s*(?:\xc2\xb5g|\xce\xbcg|\xb5g|\u00b5g|\u03bcg|mcg)",
    re.IGNORECASE,
)

# Every check is read directly from the file's own Status/Reason column pair.
# "Prohibited" is no longer its own check — the file itself reports it as a
# reason *type* under Category (or under the pre-QC Skip step), so it lives
# there instead of being guessed at via a keyword list.
CHECK_ORDER = [
    "skip", "duplicate", "restricted_keyword", "category", "color", "warranty", "variation", "fda",
    "title_weight", "title_english", "name_brand", "image_quality", "image_extraction",
    "ai_caption", "brand_image", "apple_acc_overturned",
    # Appended, not slotted next to "category" where it belongs thematically,
    # so the order of every existing section in the generated report is
    # unchanged. Both the audit tables and the .docx iterate this list, so a
    # check missing from it is silently absent from both.
    "general_rule",
    "refurbished",
    "suspected_fake",
    "generic_brand_evasion",
    # Pipeline overturn cases: ZIP rejections the app's own checks overturned.
    "pipeline_overturned",
]

CHECK_LABELS = {
    "skip": "Pre-QC Skip Reasons",
    "duplicate": "Duplicate Products",
    "restricted_keyword": "Restricted Keywords / Prohibited Content",
    "category": "Category Match",
    "color": "Color",
    "warranty": "Warranty",
    "variation": "Variation",
    "fda": "FDA / Regulatory Documents",
    "title_weight": "Title Missing Weight/Volume",
    "title_english": "Title Not In English",
    "name_brand": "Product Name ↔ Brand Name",
    "image_quality": "Image Quality",
    "image_extraction": "Image Extraction Errors",
    "ai_caption": "AI Product Caption Errors",
    "brand_image": "Brand Detected On Image",
    "apple_acc_overturned": "Apple Brand on Accessories (Wrong Rejection - Overturned)",
    "general_rule": "General Rule Violations",
    "refurbished": "Refurbished Products",
    "suspected_fake": "Suspected Fake / Counterfeit",
    "generic_brand_evasion": "Generic Brand Evasion / Brand Hijacking",
    "specs_inconsistency": "Smartphone Name & Specs Inconsistency",
    "pipeline_overturned": "ZIP Rejections Overturned by App (False Rejections)",
}

try:
    from general_rules import audit_record as _general_audit_record
except Exception:  # a broken rules file must not take the audit down with it
    def _general_audit_record(*args, **kwargs): return []

try:
    from refurbished_rules import audit_refurbished_record as _audit_refurb_record
except Exception:
    def _audit_refurb_record(*args, **kwargs):
        return {"is_refurb": False, "is_compliant": False, "violation": None}

try:
    from extended_audit_rules import (
        audit_suspected_fake_products as _audit_suspected_fake,
        audit_generic_brand_evasion as _audit_brand_evasion,
        audit_specs_inconsistency as _audit_specs,
    )
except Exception:
    def _audit_suspected_fake(*args, **kwargs): return {}
    def _audit_brand_evasion(*args, **kwargs): return {}
    def _audit_specs(*args, **kwargs): return {}

# (status_column, reason_column) as they actually appear in the file, per check.
_CHECK_COLUMNS = {
    "restricted_keyword": ("Restricted_Keyword_Status", "Restricted_Keyword_Reason"),
    "category": ("Category_Check_Status", "Category_Check_Rejection_Reason"),
    "color": ("Color_Check_Status", "Color_Rejection_Reason"),
    "warranty": ("Warranty_Check_Status", "Warranty_Rejection_Reason"),
    "variation": ("Variation_Check_Status", "Variation_Rejection_Reason"),
    "fda": ("FDA_Check_Status", "FDA_Rejection_Reason"),
    "title_weight": ("Title_Language_Check_Status", "Title_Language_Check_Reason"),
    "title_english": ("Title_Language_Check_Status", "Title_Language_Check_Reason"),
    "name_brand": ("Product Name_Brand Name_Status", "Product name_Brand name_rejection reason"),
    "image_quality": ("Image_Quality_Check_Status", "Image_Quality_Check_Reason"),
    "brand_image": ("Brand_Image_Check_Status", "Brand_Image_Check_Reason"),
}


def diagnose_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a table showing, for every check: whether its expected
    Status/Reason columns exist in `data`, AND what actual status values are
    present in that column. A column can exist and still produce zero
    results if the status text doesn't match what the evaluator expects
    (e.g. 'Fail' instead of 'Rejected') — this makes that visible directly
    instead of it silently looking like the check has no findings at all."""
    rows = []
    for check_key, (status_col, reason_col) in _CHECK_COLUMNS.items():
        status_found = status_col in data.columns
        reason_found = reason_col in data.columns
        if status_found:
            vals = data[status_col].dropna().astype(str).str.strip()
            vals = vals[vals != ""]
            sample = ", ".join(f"{v} ({c})" for v, c in vals.value_counts().head(6).items())
        else:
            sample = ""
        rows.append({
            "Check": CHECK_LABELS[check_key],
            "Status Column": status_col,
            "Column Found?": "✅" if status_found else "❌ MISSING",
            "Actual Status Values Present": sample or "(no non-empty values found)",
            "Reason Column": reason_col,
            "Reason Col Found?": "✅" if reason_found else "❌ MISSING",
        })
    for key, col in (("skip", "QC_Skip_Reason"), ("duplicate", "Duplicate_Flag")):
        found = col in data.columns
        rows.append({
            "Check": CHECK_LABELS[key], "Status Column": col,
            "Column Found?": "✅" if found else "❌ MISSING",
            "Actual Status Values Present": "", "Reason Column": "", "Reason Col Found?": "",
        })
    return pd.DataFrame(rows)

# Substring → canonical reason-type label, checked in order, per check.
# Every distinct wording the file actually produces gets its own label —
# nothing gets merged into a generic bucket unless it truly matches nothing.
_REASON_PATTERNS = {
    "category": [
        ("prohibited", "Prohibited Category"),
        ("inactive", "Inactive Category"),
        ("manual review", "Flagged For Manual Review"),
        ("suggests sibling category", "AI Suggests Sibling Category"),
        ("rejects with", "Overlapping Category Path"),
        ("suggests a different category", "AI Suggests Different Category"),
        ("suggests a better category", "AI Suggests Different Category"),
        # Sub-validation labels (match against the AI's own rejection reason text)
        ("replica jersey", "Replica Jersey / IP Violation"),
        ("sexual wellness", "Sexual Wellness Miscategory"),
        ("intimate product", "Sexual Wellness Miscategory"),
        ("pet product", "Pet Product Listed Under Non-Pet Category"),
        ("non-baby", "Baby/Toddler Listed Under Non-Baby Category"),
        ("adult footwear", "Adult Product Listed Under Baby Category"),
        ("adult hosiery", "Adult Product Listed Under Baby Category"),
        ("baby category", "Adult Product Listed Under Baby Category"),
        ("fragrance", "Fragrance / Perfume Mismatch"),
        ("perfume", "Fragrance / Perfume Mismatch"),
        ("book", "Books – Wrong Subcategory"),
        ("hair clipper", "Hair / Grooming Appliance Mismatch"),
        ("hair dryer", "Hair / Grooming Appliance Mismatch"),
        ("hair trimmer", "Hair / Grooming Appliance Mismatch"),
        ("earphone", "Electronics / Accessories Mismatch"),
        ("headphone", "Electronics / Accessories Mismatch"),
    ],
    "fda": [
        ("therapeutic/medical device", "Medical Device Requires FDA Registration"),
        ("infant oral-contact", "Infant Feeding Tool Requires FDA Registration"),
        ("regulation registration number", "FDA Documentation Missing"),
    ],
    "color": [
        ("color not filled in", "Color Missing But Inferable From Title"),
        ("color is required", "Color Missing"),
    ],
    "warranty": [
        ("confirm the product warranty", "Warranty Field Empty"),
    ],
    "variation": [
        ("variation is mandatory", "Variation Field Empty"),
    ],
    "name_brand": [
        ("high-end brand", "High-End Brand Counterfeit Suspected"),
        ("generic brand is not allowed", "Generic Brand Not Allowed (Fashion)"),
        ("repeated in product name", "Brand Name Repeated In Title"),
        ("inspired/alternative perfume", "Inspired/Alternative Perfume Brand Not Accepted"),
    ],
    "title_weight": [
        ("sold by weight", "Missing Quantity/Weight In Title"),
        ("weight/volume", "Missing Quantity/Weight In Title"),
        ("quantity/size", "Missing Quantity/Weight In Title"),
        ("must include quantity", "Missing Quantity/Weight In Title"),
    ],
    "restricted_keyword": [
        ("block", "Restricted Keyword Block"),
        ("restricted", "Restricted Keyword Block"),
        ("prohibited", "Prohibited Content"),
    ],
    "title_english": [
        ("not in english", "Title Not In English"),
    ],
    "brand_image": [
        ("brand mismatch", "Brand Mismatch (Image vs Listing)"),
        ("misspelling", "Brand Misspelling (Needs Correction)"),
    ],
    "apple_acc_overturned": [
        ("overturned", "Apple Accessory - Wrong Rejection (Overturned)"),
        ("apple brand detected", "Apple Accessory - Wrong Rejection (Overturned)"),
    ],
}

_SKIP_PATTERNS = [
    ("prohibited", "Prohibited Category"),
    ("inactive", "Inactive Category"),
    ("sex/adult toy", "Adult/Sex Toy Product"),
    ("tester products", "Tester Product"),
    ("manual review", "High-End Brand Manual Review"),
]

# Default verdict per reason type. None means "run the file's own mandatory-
# attribute rule to confirm/deny it" rather than assuming — these are the
# checks we can actually re-verify from the data (color/warranty/variation/
# FDA/weight all have a deterministic mandatory-or-not answer per category).
_REASON_VERDICT = {
    "Prohibited Category": "True Rejection",
    "Inactive Category": "True Rejection",
    "Flagged For Manual Review": "Needs Manual Review",
    "AI Suggests Sibling Category": "Needs Manual Review",
    "Overlapping Category Path": "Needs Manual Review",
    "AI Suggests Different Category": "Needs Manual Review",
    "Medical Device Requires FDA Registration": "True Rejection",
    "Infant Feeding Tool Requires FDA Registration": "True Rejection",
    "FDA Documentation Missing": None,
    "Color Missing But Inferable From Title": None,
    "Color Missing": None,
    "Warranty Field Empty": None,
    "Variation Field Empty": None,
    "High-End Brand Counterfeit Suspected": "True Rejection",
    "Generic Brand Not Allowed (Fashion)": "True Rejection",
    "Brand Name Repeated In Title": "True Rejection",
    "Inspired/Alternative Perfume Brand Not Accepted": "True Rejection",
    "Restricted Keyword Block": "True Rejection",
    "Prohibited Content": "True Rejection",
    "Missing Quantity/Weight In Title": None,
    "Title Not In English": "True Rejection",
    "Brand Mismatch (Image vs Listing)": "Needs Manual Review",
    "Brand Misspelling (Needs Correction)": "Needs Manual Review",
    "Apple Accessory - Wrong Rejection (Overturned)": "False Rejection - Overturned",
}

_STATUS_TO_VERDICT_BASE = {
    "rejected": "True Rejection",
    "block": "True Rejection",
    "review": "Needs Manual Review",
    "manual review": "Needs Manual Review",
}


def _clean(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none") else s


def _match(check_key: str, reason_text: str):
    """Returns (reason_type_label, is_error). Any reason mentioning 'error'
    is pulled out as an AI Error rather than treated as a genuine finding —
    it means the check itself failed, not that it found something.

    For the category check, API error strings are split into specific
    sub-types (quota-429, connection, timeout) so they appear as separate
    DataFrames in the Targeted Audit instead of one generic bucket.
    """
    r = _clean(reason_text)
    if not r:
        return "Reason Not Provided", False

    # ── Category check: detect specific API error sub-types first ────────────
    if check_key == "category":
        r_low = r.lower()
        if ("ai error" in r_low or "error code" in r_low or "is_gateway_error" in r_low
                or "insufficient_quota" in r_low or "openai.com" in r_low):
            if "429" in r or "insufficient_quota" in r_low or "quota" in r_low:
                return "API Error – Rate-limit / Quota Exceeded (429)", True
            if "connection error" in r_low:
                return "API Error – Connection Error", True
            if "timed out" in r_low or "timeout" in r_low or "readtimeout" in r_low:
                return "API Error – Request Timed Out", True
            if "400" in r or "failed to make http" in r_low:
                return "API Error – Provider HTTP Error (400)", True
            return "API Error – Other", True

    if "error" in r.lower():
        return "AI Error", True
    for substr, label in _REASON_PATTERNS.get(check_key, []):
        if substr in r.lower():
            return label, False
    return "Other / Unclassified Reason", False


def _match_skip(reason_text: str):
    r = _clean(reason_text)
    if not r:
        return "Other Skip Reason", False
    if "error" in r.lower():
        return "AI Error", True
    for substr, label in _SKIP_PATTERNS:
        if substr in r.lower():
            return label, False
    return "Other Skip Reason", False


def _match_extraction_error(status_text: str) -> str:
    """Classifies the raw Image_Extraction_Status error text into a
    canonical reason type, so 'IncompleteRead' failures and outright
    'Connection failed' failures show up as separate rows rather than one
    generic bucket."""
    s = status_text.lower()
    if "incompleteread" in s or "connection broken" in s:
        return "Incomplete Read (Connection Broken)"
    if "connection failed" in s:
        return "Connection Failed"
    if "timeout" in s or "timed out" in s:
        return "Timeout"
    return "Other Extraction Error"


def _match_caption_error(caption_text: str) -> str:
    """AI_Product_Caption can contain an embedded error (the image-captioning
    step failed) instead of an actual caption — classifies which kind of
    failure it was, same idea as the extraction-error parser above."""
    s = caption_text.lower()
    if "invalid_image_url" in s or "timeout while downloading" in s:
        return "Invalid/Timeout Image URL"
    if "status_code': 500" in s or "internal" in s:
        return "Server Error"
    if "status_code': 429" in s or "rate limit" in s:
        return "Rate Limited"
    return "Other Caption Error"


# ── Cached file loaders ───────────────────────────────────────────────────────
@st.cache_data
def load_weight_categories():
    try:
        with open("weight.txt", "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()


@st.cache_data
def load_colors():
    try:
        with open("colors.txt", "r", encoding="utf-8") as f:
            colors = [line.strip().lower() for line in f if line.strip()]
            return sorted(colors, key=len, reverse=True)
    except Exception:
        return []


@st.cache_data
def load_color_set() -> set:
    """Returns colors.txt as a lowercase set for O(1) membership checks."""
    return set(load_colors())


_MULTICOLOR_VARIANTS_AUDIT = {
    "multicolor", "multicolour", "multicolored", "multicoloured",
    "multi colour", "multi color", "multi-colour", "multi-color",
    "multicolors", "multicolours",
}


def _is_placeholder_color(color_val: str) -> bool:
    """True for a COLOR value that is punctuation standing in for nothing —
    '-', '--', '..', '***' — same guard streamlit_app.py's check_missing_color
    already applies (re.match(r"^[._*-]{1,5}$")). Without it here, a seller
    writing a bare dash could get AI-rescued by Color_AI_Normalized guessing a
    color from the title text, and the audit would call that a False
    Rejection — a dash is not a color declaration, whatever the AI infers.
    """
    return bool(re.match(r"^[._*-]{1,5}$", color_val.strip()))


def _color_recognised(color_val: str, valid_set: set) -> bool:
    """Return True if color_val is a recognised color according to colors.txt.

    Supports composite values like 'Red/Blue', 'Black & Gold', 'Dark Navy'.
    At least one split-part (or token within a part) must match the valid set.
    Multicolor variants (e.g. 'Multicolour') are always accepted.
    If valid_set is empty (colors.txt missing), returns False to be safe.
    """
    c = color_val.strip().lower()
    if not c:
        return False
    if c in _MULTICOLOR_VARIANTS_AUDIT or re.match(r"^multi", c):
        return True
    if not valid_set:
        return False
    # Split composite: 'Red/Blue', 'Black & Gold', 'Dark Red, White'
    parts = re.split(r"[,/&|]|\s+and\s+|\s+or\s+|\s+with\s+", c)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part in valid_set or part in _MULTICOLOR_VARIANTS_AUDIT:
            return True
        # Allow modifier + base: 'dark blue' -> token 'blue'
        for token in part.split():
            if token in valid_set:
                return True
    return False


@st.cache_data
def get_color_regex():
    colors = load_colors()
    if not colors:
        return None
    pattern = r"\b(" + "|".join(re.escape(c) for c in colors) + r")\b"
    return re.compile(pattern, re.IGNORECASE)


QC_RULES_FILE = "QC Check Validaton  (3).xlsx"


def _normalise_sheet_name(name) -> str:
    """Collapse whitespace and case so sheet lookup tolerates hand-edited tabs.

    The workbook really does contain ' Mandatory Attributes - NG' (leading
    space) and 'Mandatory Attributes - French' (title case). Matching exactly
    and case-sensitively meant Senegal and Ivory Coast — which both map to the
    French sheet — silently loaded ZERO rules, so every rule-based
    verification for those two countries quietly fell back to "no rule".
    """
    return re.sub(r"\s+", " ", str(name)).strip().lower()


@st.cache_data(show_spinner=False)
def load_qc_excel(country_code: str):
    if not country_code:
        country_code = "UG"
    cc = country_code.upper().strip()
    # Senegal and Ivory Coast share one French-language sheet.
    if cc in ("SN", "CI"):
        cc = "FRENCH"
    try:
        import os
        from data_utils import PARQUET_CACHE_DIR
        from loaders import _atomic_to_parquet
        os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
        pq_path = os.path.join(PARQUET_CACHE_DIR, f"qc_rules_{cc}.parquet")

        df = None
        if os.path.exists(pq_path) and os.path.exists(QC_RULES_FILE) and os.path.getmtime(pq_path) > os.path.getmtime(QC_RULES_FILE):
            try:
                df = pd.read_parquet(pq_path)
            except Exception:
                df = None

        if df is None:
            xl = pd.ExcelFile(QC_RULES_FILE)
            target = _normalise_sheet_name(f"Mandatory Attributes - {cc}")
            found_sheet = next(
                (n for n in xl.sheet_names if _normalise_sheet_name(n) == target), None
            )
            if not found_sheet:
                logger.error(
                    "[QC rules] No sheet for %s in %s. Looked for %r; available: %s",
                    country_code, QC_RULES_FILE, target, list(xl.sheet_names),
                )
                return {}
            df = xl.parse(found_sheet)
            if "ID" in df.columns:
                df["ID"] = df["ID"].astype(str).str.strip()
                df = df[df["ID"].notna() & (df["ID"] != "") & (df["ID"] != "nan")]
                try:
                    _atomic_to_parquet(df.astype(str), pq_path)
                except Exception as _w_err:
                    logger.warning("Could not write qc_rules parquet: %s", _w_err)

        if df is None or "ID" not in df.columns:
            logger.error(
                "[QC rules] Sheet for %s has no 'ID' column", country_code
            )
            return {}

        df["ID"] = df["ID"].astype(str).str.strip()
        df = df[df["ID"].notna() & (df["ID"] != "") & (df["ID"] != "nan")]
        rules = df.set_index("ID").to_dict(orient="index")
        logger.info("[QC rules] %s -> %d rules", country_code, len(rules))
        return rules
    except Exception as e:
        logger.error("[QC rules] Failed loading %s for %s: %s", QC_RULES_FILE, country_code, e)
        return {}


# ── Rule-based re-verification for the checks that have a deterministic answer ─
def _verify(check_key: str, rec: dict, rule: dict, weights: set, color_re) -> str:
    """Only called for reason types marked None in _REASON_VERDICT — actually
    re-derives whether the file's own mandatory-attribute rule agrees."""
    cat_code = _clean(rec.get("CATEGORY_CODE"))
    name = _clean(rec.get("NAME"))

    if check_key == "fda":
        req = _clean(rule.get("FDA Documents", "Mandatory")).lower()
        return "True Rejection" if req != "no need" else "False Rejection"

    if check_key == "color":
        req = _clean(rule.get("Color", "Mandatory")).lower()
        color_val = _clean(rec.get("COLOR"))
        if req == "no need":
            return "False Rejection"
        # Any value that starts with 'multi' is a valid multicolor declaration
        if re.match(r"^multi", color_val.lower()):
            return "False Rejection"
            
        ai_color = _clean(rec.get("Color_AI_Normalized"))
        valid_colors = load_color_set()
        
        def _ai_rescued():
            return ai_color and ai_color.lower() not in ("nan", "none", "not found") and _color_recognised(ai_color, valid_colors)
            
        # Column must be filled AND the value must be a recognised color in colors.txt.
        # A blank COLOR field is always a True Rejection — the AI being able to guess
        # a color from the title text doesn't excuse the seller leaving it blank.
        # A punctuation placeholder ('-', '..') counts as blank for this too —
        # never AI-rescued, since it isn't a color declaration to rescue.
        if not color_val or _is_placeholder_color(color_val):
            return "True Rejection"

        if not _color_recognised(color_val, valid_colors):
            return "False Rejection"
        return "False Rejection"

    if check_key == "warranty":
        req = _clean(rule.get("Warranty", "Mandatory")).lower()
        return "True Rejection" if req != "no need" else "False Rejection"

    if check_key == "variation":
        req = _clean(rule.get("Variation", "Mandatory")).lower()
        var_val = _clean(rec.get("VARIATION")) or _clean(rec.get("LIST_VARIATIONS"))
        var_count = _clean(rec.get("COUNT_OF_EXISTING_VARIATIONS")) or _clean(rec.get("COUNT_VARIATIONS"))
        has_var = bool(var_val or (var_count and var_count not in ("0",)))
        if has_var:
            return "False Rejection"
        return "True Rejection" if req != "no need" else "False Rejection"

    if check_key == "title_weight":
        return "True Rejection" if cat_code in weights else "False Rejection"
    
    if check_key == "title_english":
        return "Needs Manual Review"

    return "Needs Manual Review"


def _verify_false_approval(check_key: str, rec: dict, rule: dict, weights: set, color_re) -> str:
    """Returns a reason-type label if an approved row actually violates the
    file's own mandatory rule, else '' (nothing wrong)."""
    cat_code = _clean(rec.get("CATEGORY_CODE"))
    name = _clean(rec.get("NAME"))

    if check_key == "fda":
        if _clean(rule.get("FDA Documents", "")).lower() == "mandatory" and not _clean(rec.get("FDA")):
            return "FDA Documentation Missing"
    elif check_key == "color":
        if _clean(rule.get("Color", "")).lower() == "mandatory":
            color_val = _clean(rec.get("COLOR"))
            ai_color = _clean(rec.get("Color_AI_Normalized"))
            valid_colors = load_color_set()
            
            # Check if AI rescued it
            def _ai_rescued():
                return ai_color and ai_color.lower() not in ("nan", "none", "not found") and _color_recognised(ai_color, valid_colors)

            if _is_placeholder_color(color_val):
                return "Color Missing"

            if not color_val:
                if _ai_rescued():
                    return ""
                return "Color Missing"

            if not _color_recognised(color_val, valid_colors):
                if _ai_rescued():
                    return ""
                # Plain wording: the reader does not care which file the
                # list of valid colors lives in.
                return "Color Not Recognised"
    elif check_key == "warranty":
        if _clean(rule.get("Warranty", "")).lower() == "mandatory" and not _clean(rec.get("PRODUCT_WARRANTY")):
            return "Warranty Field Empty"
    elif check_key == "variation":
        if _clean(rule.get("Variation", "")).lower() == "mandatory":
            var_val = _clean(rec.get("VARIATION")) or _clean(rec.get("LIST_VARIATIONS"))
            var_count = _clean(rec.get("COUNT_OF_EXISTING_VARIATIONS")) or _clean(rec.get("COUNT_VARIATIONS"))
            if not var_val and (not var_count or var_count == "0"):
                return "Variation Field Empty"
    elif check_key == "title_weight":
        # Refurbished/renewed products are exempt from the weight-in-title rule —
        # their naming convention is governed by the refurbished validation, not
        # by the weight/volume category rule.  Do not flag them here.
        _name_low = name.lower() if name else ""
        if "refurb" in _name_low or "renew" in _name_low:
            return ""
        if cat_code in weights and not _VOLUME_RE.search(name):
            return "Missing Quantity/Weight In Title"
    elif check_key == "title_english":
        pass
    return ""


# ── Row/context builder ───────────────────────────────────────────────────────
# Every field here is something a reviewer would actually want to look at to
# double-check the specific finding — not just the bare minimum. Pulled
# straight from columns that exist in the file (including the AI-derived
# ones like Color_AI_Normalized, Category_Match_Score, Brand_Detected_On_Product).
def _context_columns(check_key: str, rec: dict) -> dict:
    ctx = {}
    # Rule of thumb for what belongs here: if a reviewer would have to look at
    # it to decide approve-or-reject, it goes in. That means the picture for
    # any check judged by eye — you cannot tell whether a colour is really
    # missing, a category is really wrong, or a duplicate is really a
    # duplicate, from text alone.
    if check_key == "category":
        ctx["Initial Category Path"] = _clean(rec.get("Initial_Category_Path"))
        ctx["Suggested Categories"] = _clean(rec.get("Suggested_Categories"))
        ctx["Top1 Category"] = _clean(rec.get("Top1_Category"))
        ctx["Category Match Score"] = _clean(rec.get("Category_Match_Score"))
        ctx["Top1 Score"] = _clean(rec.get("Top1_Score"))
        ctx["AI Product Caption"] = _clean(rec.get("AI_Product_Caption"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "color":
        ctx["Color"] = _clean(rec.get("COLOR"))
        ctx["Color Family"] = _clean(rec.get("COLOR_FAMILY"))
        ctx["Color (AI Normalized)"] = _clean(rec.get("Color_AI_Normalized"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "warranty":
        ctx["Warranty"] = _clean(rec.get("PRODUCT_WARRANTY"))
        ctx["Warranty Type"] = _clean(rec.get("WARRANTY_TYPE"))
        ctx["Warranty Duration"] = _clean(rec.get("WARRANTY_DURATION"))
        ctx["Warranty Address"] = _clean(rec.get("WARRANTY_ADDRESS"))
    elif check_key == "variation":
        ctx["Variation"] = _clean(rec.get("VARIATION")) or _clean(rec.get("LIST_VARIATIONS"))
        ctx["Existing Variation Count"] = _clean(rec.get("COUNT_OF_EXISTING_VARIATIONS")) or _clean(rec.get("COUNT_VARIATIONS"))
    elif check_key == "fda":
        ctx["FDA"] = _clean(rec.get("FDA"))
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "title_weight":
        # Weight/volume is usually printed on the pack, so the picture often
        # settles it faster than the title does.
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "title_english":
        pass  # Product Name (already in base row) is the whole subject of this check
    elif check_key == "name_brand":
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Brand Detected On Product"] = _clean(rec.get("Brand_Detected_On_Product"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "image_quality":
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
        ctx["Image Filename"] = _clean(rec.get("Image_Filename"))
    elif check_key == "brand_image":
        ctx["Listed Brand"] = _clean(rec.get("BRAND"))
        ctx["Brand Detected On Product"] = _clean(rec.get("Brand_Detected_On_Product"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "image_extraction":
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "ai_caption":
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "duplicate":
        # Had no context at all, which made it the hardest group to act on:
        # "this is a duplicate" with nothing to compare against. The flag says
        # what matched, the image is how a human confirms it, and the brand
        # distinguishes a genuine repeat from two similar products.
        ctx["Duplicate Flag"] = _clean(rec.get("Duplicate_Flag"))
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "refurbished":
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "suspected_fake":
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Price"] = _clean(rec.get("GLOBAL_SALE_PRICE") or rec.get("GLOBAL_PRICE") or rec.get("PRICE"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "generic_brand_evasion":
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    elif check_key == "specs_inconsistency":
        ctx["Brand"] = _clean(rec.get("BRAND"))
        ctx["Image"] = _clean(rec.get("MAIN_IMAGE"))
    return ctx


def _base_row(sid: str, check_key: str, rec: dict) -> dict:
    row = {
        "ProductSetSid": sid,
        "Check": check_key,
        "Product Name": _clean(rec.get("NAME")),
        "Seller": _clean(rec.get("SELLER_NAME")),
        "Category": _clean(rec.get("CATEGORY")),
    }
    row.update(_context_columns(check_key, rec))
    return row


_NEEDED = [
    "PRODUCT_SET_SID", "CATEGORY_CODE", "NAME", "BRAND", "COLOR", "CATEGORY",
    "COLOR_FAMILY", "Color_AI_Normalized",
    "PRODUCT_WARRANTY", "WARRANTY_TYPE", "WARRANTY_DURATION", "WARRANTY_ADDRESS",
    "COUNT_VARIATIONS", "LIST_VARIATIONS", "VARIATION", "COUNT_OF_EXISTING_VARIATIONS",
    "FDA", "MAIN_IMAGE", "Image_Filename",
    "Initial_Category_Path", "Suggested_Categories", "Top1_Category",
    "Category_Match_Score", "Top1_Score", "AI_Product_Caption",
    "Brand_Detected_On_Product",
    "Image_Extraction_Status", "QC_Skip_Reason", "Duplicate_Flag", "SELLER_NAME",
    # Approves the product outright regardless of the per-check columns, so the
    # audit has to see it or it reads a rejected check as agreement on a
    # product that actually shipped approved.
    "Manual_Review",
] + [col for pair in _CHECK_COLUMNS.values() for col in pair]


# ── Image extraction errors (pipeline-level, independent of QC decisions) ────
def get_image_extraction_errors(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty or "Image_Extraction_Status" not in data.columns:
        return pd.DataFrame()
    status = data["Image_Extraction_Status"].astype(str)
    mask = status.str.strip().str.lower().ne("successful") & status.str.strip().ne("")
    cols = [c for c in ["PRODUCT_SET_SID", "NAME", "MAIN_IMAGE", "Image_Extraction_Status"] if c in data.columns]
    return data.loc[mask, cols].copy()


# ── The unified evaluator ──────────────────────────────────────────────────────
def _append_approved_findings(rows: list, sid: str, check_key: str, rec: dict, rule: dict,
                               weights: set, color_re, reason: str) -> None:
    """Shared logic for an item that has no rejection recorded for this check
    (either status == 'Approved', or no status column exists and the reason
    field is blank). Checks for a missed mandatory-attribute violation, a
    brand-misspelling note, or an embedded error — used whether or not a
    status column is present."""
    if reason and "error" in reason.lower():
        rows.append({**_base_row(sid, check_key, rec), "Reason Type": "AI Error",
                     "Verdict": "AI Error", "Detail": reason})
        return
    if reason and (reason.lower().startswith("approved (spelling note)") or "misspelling" in reason.lower()):
        rows.append({**_base_row(sid, check_key, rec),
                     "Reason Type": "Brand Misspelling (Needs Correction)",
                     "Verdict": "Needs Manual Review", "Detail": reason})
        return
    fa_label = _verify_false_approval(check_key, rec, rule, weights, color_re)
    if fa_label == "Color Not Recognised":
        rows.append({**_base_row(sid, check_key, rec), "Reason Type": "Color Not Recognised - Overturned",
                     "Verdict": "False Rejection",
                     "Detail": "Rejection overturned: Unrecognised color accepted -- product approved."})
    elif fa_label:
        rows.append({**_base_row(sid, check_key, rec), "Reason Type": fa_label,
                     "Verdict": "False Approval",
                     "Detail": "Approved, but the file's own mandatory-attribute "
                               "rule for this category says it shouldn't have been."})


def evaluate_all_checks(data: pd.DataFrame, country_code: str) -> pd.DataFrame:
    """
    Walks every product in `data` and reads each of the file's own nine
    per-check Status/Reason column pairs directly — no guessing from a
    summarized flag. Every distinct reason string is split into its own
    labeled Reason Type (e.g. 'Title Not In English' is never merged with
    'Missing Quantity/Weight In Title'), Duplicate_Flag and QC_Skip_Reason
    become their own top-level checks, and any reason mentioning 'error'
    is broken out as a separate 'AI Error' flag rather than a real finding.

    Returns one row per (product, check, reason) with columns:
    ProductSetSid, Check, Product Name, Category, [check-specific fields],
    Reason Type, Verdict, Detail.

    Verdict is one of: True Rejection, False Rejection, False Approval,
    Needs Manual Review, AI Error, Skipped, Duplicate.
    """
    if data.empty:
        return pd.DataFrame(columns=["ProductSetSid", "Check", "Product Name", "Category",
                                      "Reason Type", "Verdict", "Detail"])

    weights = load_weight_categories()
    color_re = get_color_regex()
    qc_rules = load_qc_excel(country_code)
    # The product's FINAL verdict, from PIM_QC_Result.xlsx.
    #
    # A per-check column says what one check thought; it does not say what
    # happened to the product. "Review" especially: it means the AI was unsure
    # and passed it to a human, not that the product was held. In KE 805, 838
    # of the 1,385 products whose category check said Review shipped Approved.
    #
    # Reading the column alone, the audit counted all 1,385 as "the file caught
    # it" and reported nothing — so a rule firing on any of those 838 was
    # silently dropped. That is what hid the Titan Gel product: its category
    # check said Review, the reason text was correct, and it shipped Approved.
    _platform_verdict = {}
    try:
        _pv = st.session_state.get("_platform_verdict") or {}
        _platform_verdict = {
            str(k).strip(): str(v).strip().lower() for k, v in dict(_pv).items()
        }
    except Exception:
        logger.exception("could not read the platform verdict map; "
                         "falling back to the per-check columns")
    # Deduplicate BOTH sides. The frame's own labels were already handled, but
    # `cols` itself repeats Title_Language_Check_Status/Reason — title_weight
    # and title_english share those two columns — so the selection rebuilt a
    # duplicate-labelled frame and .to_dict() dropped keys again, warning
    # "columns are not unique, some columns will be omitted" on every run.
    cols = list(dict.fromkeys(c for c in _NEEDED if c in data.columns))
    _dedup_data = data.loc[:, ~data.columns.duplicated()]
    records = _dedup_data[cols].to_dict("records")
    status_cols_present = {status_col for status_col, _ in _CHECK_COLUMNS.values() if status_col in data.columns}

    # Resolved once, not per record: general_rules walks the whole category map
    # to turn a path into codes, and there are thousands of records here.
    _gr_scopes = {}
    _refurb_c2p = {}
    _refurb_data = {}
    try:
        import streamlit as _st
        from general_rules import build_scopes as _gr_build_scopes
        _supp = (_st.session_state.get("support_files") or {})
        _gr_c2p = _supp.get("code_to_path") or {}
        _gr_scopes = _gr_build_scopes(_gr_c2p, country_code) if _gr_c2p else {}
        _refurb_c2p = _gr_c2p
        _refurb_data = _supp.get("refurb_data") or {}
    except Exception:
        logger.exception("general_rules / refurb: could not resolve rule scopes for the audit")

    if not _refurb_data:
        try:
            from loaders import load_refurb_data_from_local
            _refurb_data = load_refurb_data_from_local()
        except Exception:
            pass

    # ── Batch-evaluate the three new extended checks once on the whole frame ──
    # Each returns a dict: ProductSetSid -> {'reason_type': ..., 'detail': ...}
    # Doing this outside the per-record loop is O(n) instead of O(n²).
    _fake_violations: Dict[str, dict] = {}
    _evasion_violations: Dict[str, dict] = {}
    _specs_violations: Dict[str, dict] = {}
    try:
        _supp2 = {}
        try:
            import streamlit as _st2
            _supp2 = _st2.session_state.get("support_files") or {}
        except Exception:
            pass

        # suspected_fake
        _sf_df = (_supp2.get("suspected_fake") or {}).get(country_code)
        if _sf_df is None:
            try:
                from loaders import load_suspected_fake_from_local
                _sf_df = load_suspected_fake_from_local().get(country_code)
            except Exception:
                _sf_df = None
        _sneaker_cats = _supp2.get("sneaker_category_codes") or []
        if _sf_df is not None:
            _fake_violations = _audit_suspected_fake(data, _sf_df, _sneaker_cats)

        # generic_brand_evasion
        _brands_list = _supp2.get("known_brands") or []
        if not _brands_list:
            try:
                from loaders import load_txt_file
                import os as _os2
                if _os2.path.exists("brands.txt"):
                    _brands_list = load_txt_file("brands.txt")
            except Exception:
                pass
        if _brands_list:
            _evasion_c2p = _supp2.get("code_to_path") or _gr_c2p or {}
            _evasion_violations = _audit_brand_evasion(data, _brands_list, _evasion_c2p)

        # specs_inconsistency
        _phone_cats = _supp2.get("smartphone_category_codes") or []
        _spec_cats = _supp2.get("spec_category_codes") or []
        _specs_violations = _audit_specs(data, _spec_cats, _phone_cats)
    except Exception:
        logger.exception("extended audit rules (fake/evasion/specs) batch failed")

    rows = []
    for rec in records:
        sid = _clean(rec.get("PRODUCT_SET_SID"))
        cat_code = _clean(rec.get("CATEGORY_CODE"))
        rule = qc_rules.get(cat_code, {})

        # ── Pre-QC skip ───────────────────────────────────────────────────
        skip_reason = _clean(rec.get("QC_Skip_Reason"))
        if skip_reason:
            label, is_error = _match_skip(skip_reason)
            rows.append({**_base_row(sid, "skip", rec), "Reason Type": label,
                         "Verdict": "AI Error" if is_error else "Skipped", "Detail": skip_reason})
            continue  # skipped products never went through the 9 checks

        # ── Duplicate ────────────────────────────────────────────────────
        dup_flag = _clean(rec.get("Duplicate_Flag"))
        if dup_flag:
            rows.append({**_base_row(sid, "duplicate", rec),
                         "Reason Type": "Duplicate Product (Same Seller + Name)",
                         "Verdict": "Duplicate", "Detail": dup_flag})

        # ── Image extraction failures (infrastructure, not a QC decision,
        # but still surfaced as its own check so it's counted alongside
        # every other AI Error and shown consistently in the same tables/
        # report instead of a disconnected side-list) ────────────────────
        extraction_status = _clean(rec.get("Image_Extraction_Status"))
        if extraction_status and extraction_status.lower() != "successful":
            label = _match_extraction_error(extraction_status)
            rows.append({**_base_row(sid, "image_extraction", rec), "Reason Type": label,
                         "Verdict": "AI Error", "Detail": extraction_status})

        # ── AI_Product_Caption can contain an embedded error instead of an
        # actual caption (the captioning step itself failed, e.g. it
        # couldn't download the image) — surfaced the same way.
        caption_text = _clean(rec.get("AI_Product_Caption"))
        if caption_text and caption_text.lower().startswith("ai error"):
            label = _match_caption_error(caption_text)
            rows.append({**_base_row(sid, "ai_caption", rec), "Reason Type": label,
                         "Verdict": "AI Error", "Detail": caption_text})

        # ── General rules vs the file's category verdict ──────────────────
        # The rules in general_rules.py are an independent opinion about where
        # a product belongs, so where they disagree with the ZIP's category
        # decision, one of the two is wrong and it is worth naming.
        #
        # Both directions are reported, but they are not symmetric. A rule
        # firing on a product the file approved is a clear miss. The reverse —
        # a rejection on a product the rule says is correctly placed — is only
        # claimed for rules that carry `belongs`, because a rule states where
        # something must NOT be; absence of a rule is not a statement that the
        # listing is fine, and the file may well have rejected it for a reason
        # no rule here covers.
        # Each finding says which of the file's own checks it argues with, so
        # an FDA finding is compared against the FDA verdict and not the
        # category one — otherwise every product the file happened to reject
        # for the wrong thing would read as a false approval.
        # Manual_Review approves the product outright, whatever the individual
        # check columns say, so a row can read "Category: Rejected" and still
        # ship approved. Treated as agreement, that hid the case worth
        # reporting most: a rule fired, the file's own category check agreed,
        # and the product was approved anyway.
        _manual_ok = _clean(rec.get("Manual_Review")).lower() in ("true", "1", "yes")

        # What actually happened to the product, where the file tells us.
        # Approved here means it shipped, whatever any single check column said,
        # so a rule firing on it is a miss worth reporting. Rejected likewise
        # settles it the other way. Only when there is no final verdict to
        # consult do we fall back to reading the per-check columns.
        _final = _platform_verdict.get(str(sid).strip(), "")

        def _file_verdict(_key):
            if _manual_ok:
                # Approved regardless of this check's own column.
                return False, "marked Already Approved (Manual_Review)", False
            _sc, _rc = _CHECK_COLUMNS[_key]
            _st = _clean(rec.get(_sc)).lower() if _sc in status_cols_present else ""
            _rn = _clean(rec.get(_rc))
            _check_rej = (
                (_st in ("rejected", "review", "manual review"))
                or (not _st and _rn and "error" not in _rn.lower())
            )
            if _final == "approved":
                return False, "the file approved this product", False
            if _final == "rejected":
                if _check_rej:
                    return True, _rn or "(rejected by check)", True
                return True, _clean(rec.get(_rc)) or "(product rejected at overall file level)", False
            return _check_rej, _rn, _check_rej

        try:
            for _gr in _general_audit_record(rec, _gr_scopes, country_code):
                _against = _gr.get("against", "category")
                _rejected, _reason_txt, _check_specifically_rejected = _file_verdict(_against)
                _what = "on category" if _against == "category" else "for FDA"
                if _gr.get("kind") == "overturned" or _gr.get("reason_type", "").endswith("- Overturned"):
                    rows.append({**_base_row(sid, "general_rule", rec),
                                 "Reason Type": _gr["reason_type"],
                                 "Verdict": "False Rejection",
                                 "Detail": _gr["detail"]})
                elif _gr["kind"] == "violation" and not _rejected:
                    rows.append({**_base_row(sid, "general_rule", rec),
                                 "Reason Type": _gr["reason_type"],
                                 "Verdict": "False Approval",
                                 "Detail": f"{_gr['detail']} The file did not reject this "
                                           f"product {_what}."})
                elif _gr["kind"] == "correct_placement" and _check_specifically_rejected:
                    _cat_reason = (
                        _reason_txt
                        if _reason_txt
                        else "no category-specific rejection reason was recorded"
                    )
                    rows.append({**_base_row(sid, "general_rule", rec),
                                 "Reason Type": _gr["reason_type"],
                                 "Verdict": "False Rejection",
                                 "Detail": f"{_gr['detail']} The file rejected it {_what}: "
                                           f"{_cat_reason}. "
                                           "This rejection should be overturned — "
                                           "the product is correctly categorised."})
        except Exception:
            logger.exception("general_rules audit failed for %s", sid)

        # ── Refurbished products validation ──────────────────────────────
        try:
            refurb_res = _audit_refurb_record(rec, _refurb_c2p, _refurb_data, country_code)
            _file_refurb_rej = ""
            for _c_key in rec:
                _c_low = str(_c_key).lower()
                if "reason" in _c_low or "status" in _c_low:
                    _val = _clean(rec.get(_c_key))
                    _val_l = _val.lower()
                    if re.search(r"\b(?:refurbished|refurb|renewed)\b", _val_l) or "1000028" in _val_l:
                        if "skin renewal" in _val_l or "renewing" in _val_l or "anti-aging" in _val_l:
                            continue
                        _file_refurb_rej = _val
                        break

            if _manual_ok:
                _is_approved = True
            elif _final == "approved":
                _is_approved = True
            elif _final == "rejected":
                _is_approved = False
            else:
                _has_rej = any(_clean(rec.get(_sc)).lower() in ("rejected", "block") for _sc in status_cols_present)
                _is_approved = not _has_rej

            if refurb_res.get("violation"):
                v = refurb_res["violation"]
                # Unapproved Refurbished Seller is suppressed: the seller
                # approval list is country-specific and often incomplete,
                # causing false positives whenever a seller is not yet
                # loaded into the support files.
                if v.get("reason_type") == "AI Missed Refurbished - Unapproved Refurbished Seller":
                    pass
                elif v.get("is_overturned") or v.get("reason_type", "").endswith("- Overturned"):
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": v["reason_type"],
                        "Verdict": "False Rejection",
                        "Detail": v["detail"],
                    })
                elif _is_approved:
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": v["reason_type"],
                        "Verdict": "False Approval",
                        "Detail": f"{v['detail']} The AI pipeline approved this product without catching the refurbished violation.",
                    })
                elif _file_refurb_rej:
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": v["reason_type"],
                        "Verdict": "True Rejection",
                        "Detail": f"Correctly rejected by pipeline: {v['detail']}",
                    })
                else:
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": v["reason_type"],
                        "Verdict": "False Approval",
                        "Detail": f"{v['detail']} The AI pipeline did not catch the refurbished violation (product was rejected for an unrelated issue: {_final or 'other check'}).",
                    })

            elif refurb_res.get("is_compliant"):
                if _file_refurb_rej:
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": "Refurbished Listing Compliant But Rejected",
                        "Verdict": "False Rejection",
                        "Detail": f"Pipeline rejected with reason '{_file_refurb_rej}', but this listing complies with all refurbished guidelines.",
                    })
            elif not refurb_res.get("is_refurb") and _file_refurb_rej:
                _cat_c_clean = clean_category_code(str(rec.get("CATEGORY_CODE", "")))
                _is_refurb_cat = False
                try:
                    from refurbished_rules import REFURB_ELIGIBLE_CATEGORY_CODES
                    _is_refurb_cat = _cat_c_clean in REFURB_ELIGIBLE_CATEGORY_CODES
                except Exception:
                    pass
                if _is_refurb_cat or "1000028" in _file_refurb_rej or re.search(r"\b(?:phone|laptop|tablet|desktop|device|macbook|ipad)\b", str(rec.get("NAME", "")).lower()):
                    rows.append({
                        **_base_row(sid, "refurbished", rec),
                        "Reason Type": "Non-Refurbished Product Rejected as Refurbished",
                        "Verdict": "False Rejection",
                        "Detail": f"Pipeline rejected with reason '{_file_refurb_rej}', but this product is not a refurbished item.",
                    })
        except Exception:
            logger.exception("refurbished audit failed for %s", sid)

        # ── Suspected Fake / Counterfeit ──────────────────────────────────
        try:
            if sid in _fake_violations:
                vf = _fake_violations[sid]
                if vf.get("is_overturned") or vf.get("reason_type", "").endswith("- Overturned"):
                    rows.append({
                        **_base_row(sid, "suspected_fake", rec),
                        "Reason Type": vf["reason_type"],
                        "Verdict": "False Rejection",
                        "Detail": vf["detail"],
                    })
                elif _is_approved:
                    rows.append({
                        **_base_row(sid, "suspected_fake", rec),
                        "Reason Type": vf["reason_type"],
                        "Verdict": "False Approval",
                        "Detail": f"{vf['detail']} The AI approved this product despite the price being at or below the counterfeit ceiling.",
                    })
                else:
                    _rej_reason = ""
                    for _c_key2 in rec:
                        _c_low2 = str(_c_key2).lower()
                        if "reason" in _c_low2:
                            _rv2 = _clean(rec.get(_c_key2))
                            if any(t in _rv2.lower() for t in ("counterfeit", "fake", "1000023", "not authoriz")):
                                _rej_reason = _rv2
                                break
                    if _rej_reason:
                        rows.append({
                            **_base_row(sid, "suspected_fake", rec),
                            "Reason Type": vf["reason_type"],
                            "Verdict": "True Rejection",
                            "Detail": f"Correctly rejected by pipeline: {vf['detail']}",
                        })
                    else:
                        rows.append({
                            **_base_row(sid, "suspected_fake", rec),
                            "Reason Type": vf["reason_type"],
                            "Verdict": "False Approval",
                            "Detail": f"{vf['detail']} Product was rejected for an unrelated reason; the counterfeit price violation was not caught.",
                        })
        except Exception:
            logger.exception("suspected_fake audit failed for %s", sid)

        # ── Generic Brand Evasion / Brand Hijacking ───────────────────────
        try:
            if sid in _evasion_violations:
                ve = _evasion_violations[sid]
                if _is_approved:
                    rows.append({
                        **_base_row(sid, "generic_brand_evasion", rec),
                        "Reason Type": ve["reason_type"],
                        "Verdict": "False Approval",
                        "Detail": f"{ve['detail']} The AI approved this product without catching the brand evasion.",
                    })
                else:
                    _rej_reason_b = ""
                    for _c_key3 in rec:
                        _c_low3 = str(_c_key3).lower()
                        if "reason" in _c_low3 or "status" in _c_low3 or "flag" in _c_low3:
                            _rv3 = _clean(rec.get(_c_key3))
                            if any(t in _rv3.lower() for t in ("brand", "hijack", "generic", "unbranded", "mismatch", "fake", "trademark", "counterfeit", "1000007")):
                                _rej_reason_b = _rv3
                                break
                    if _rej_reason_b:
                        rows.append({
                            **_base_row(sid, "generic_brand_evasion", rec),
                            "Reason Type": ve["reason_type"],
                            "Verdict": "True Rejection",
                            "Detail": f"Correctly rejected by pipeline: {ve['detail']}",
                        })
                    else:
                        rows.append({
                            **_base_row(sid, "generic_brand_evasion", rec),
                            "Reason Type": ve["reason_type"],
                            "Verdict": "True Rejection",
                            "Detail": f"Rejected by pipeline: {ve['detail']}",
                        })
        except Exception:
            logger.exception("generic_brand_evasion audit failed for %s", sid)

        # ── Smartphone Name & Specs Inconsistency (Omitted per request) ───
        # Removed from targeted audit report; handled by main pipeline checks.

        # ── The file's own nine checks ───────────────────────────────────
        for check_key, (status_col, reason_col) in _CHECK_COLUMNS.items():
            has_status_col = status_col in status_cols_present
            status = _clean(rec.get(status_col)).lower() if has_status_col else ""
            reason = _clean(rec.get(reason_col))
            
            # The Kenya book rule used to sit here, deciding whether a product
            # was in a book category with `"book" in CATEGORY`. That reads the
            # leaf the record carries, not the path, so a book correctly filed
            # in "Books, Movies and Music / Business & Finance / Business &
            # Economics" arrived as "Business & Economics", failed the
            # substring test, and was reported as a false approval. It also
            # failed the other way: "Nursery Decor / Bookends" contains "book"
            # and is not a book category.
            #
            # Books now go through the same category verification as everything
            # else. The Books-branch exemption still exists for validation, in
            # custom_country_rules.is_kenya_books_exempt, which splits the path
            # properly and keeps the DVDs sub-tree in scope.

            # ── Warranty contradiction check ──────────────────────────────
            # If PRODUCT_WARRANTY actually has a value, a rejection for this
            # check is wrong no matter what the reason text says or whether
            # the mandatory-rule sheet loaded — the file's own data
            # contradicts its own decision. Always worth flagging as its
            # own distinct issue rather than folding into "Warranty Field
            # Empty" (which would be misleading here).
            if check_key == "warranty":
                warranty_val = _clean(rec.get("PRODUCT_WARRANTY"))
                is_rejection_like = (
                    (has_status_col and status in ("rejected", "review", "manual review"))
                    or (not has_status_col and reason and "error" not in reason.lower())
                )
                if warranty_val and is_rejection_like:
                    rows.append({**_base_row(sid, "warranty", rec),
                                 "Reason Type": "Warranty Present But Rejected",
                                 "Verdict": "False Rejection",
                                 "Detail": f"Rejected for missing warranty, but PRODUCT_WARRANTY "
                                           f"= '{warranty_val}'. Original reason: "
                                           f"{reason or '(none provided)'}"})
                    continue

            # ── Brand Image Check / Apple accessory contradiction check ─────────
            if check_key == "brand_image":
                is_rejection_like = (
                    (has_status_col and status in ("rejected", "review", "manual review"))
                    or (not has_status_col and reason and "error" not in reason.lower())
                )
                if is_rejection_like:
                    det_brand_val = _clean(rec.get("Brand_Detected_On_Product")).lower()
                    cat_val = _clean(rec.get("CATEGORY")).lower()
                    name_val = _clean(rec.get("NAME")).lower()
                    reason_val = _clean(reason).lower()
                    _APPLE_ACC_RE = re.compile(
                        r"\b(?:case|cases|cover|covers|sleeve|sleeves|pouch|pouches|screen.?protector|housing|skin)\b",
                        re.IGNORECASE,
                    )
                    _is_apple_detected = (
                        det_brand_val == "apple"
                        or "visible on the product is 'apple'" in reason_val
                        or "visible on the product is \"apple\"" in reason_val
                        or "brand detected on product: apple" in reason_val
                    )
                    _is_acc = bool(_APPLE_ACC_RE.search(cat_val) or _APPLE_ACC_RE.search(name_val))
                    if _is_apple_detected and _is_acc:
                        rows.append({
                            **_base_row(sid, "brand_image", rec),
                            "Reason Type": "Apple Accessory - Wrong Rejection (Overturned)",
                            "Verdict": "False Rejection",
                            "Detail": f"Wrong rejection overturned: Apple brand detected on accessory image ('{rec.get('NAME')}') for Apple devices. Original: {reason or '(none provided)'}"
                        })
                        continue

            # ── Title Language / Volume contradiction check ──────────────────
            # If the product was rejected for missing volume but the sophisticated
            # regex extracts volume from the NAME successfully, it's a False Rejection.
            if check_key in ("title_weight", "title_english"):
                _r_low = (reason or "").lower()
                _name_low_ref = _clean(rec.get("NAME", "")).lower()
                # Skip entirely for refurbished/renewed products: their naming
                # is governed by the refurbished validation, not weight/volume.
                # Check BOTH the reason text AND the product name, because
                # approved refurb items may have an empty reason.
                if (re.search(r"\b(?:refurbished|refurb|renewed)\b", _r_low)
                        or re.search(r"\b(?:refurbished|refurb|renewed)\b", _name_low_ref)):
                    continue
                # Skip phone/tablet/laptop specs rejections: they are governed by specs checks, not weight/volume
                if any(k in _r_low for k in ("listing is incomplete", "key specs", "specifications to the title", "incomplete smartphone")):
                    continue

            if check_key == "title_weight":
                _r_low = (reason or "").lower()
                is_rejection_like = (
                    (has_status_col and status in ("rejected", "review", "manual review"))
                    or (not has_status_col and reason and "error" not in _r_low)
                )
                is_volume_rejection = any(term in _r_low for term in ("sold by weight", "weight/volume", "quantity/size", "must include quantity", "weight", "volume", "quantity"))
                if is_rejection_like and is_volume_rejection:
                    name_val = _clean(rec.get("NAME"))
                    if name_val and _VOLUME_RE.search(name_val):
                        rows.append({**_base_row(sid, "title_weight", rec),
                                     "Reason Type": "Volume Present But Rejected",
                                     "Verdict": "False Rejection",
                                     "Detail": f"Rejected for missing volume/weight in title, but valid volume format detected in NAME: '{name_val}'. Original reason: {reason or '(none provided)'}"})
                        continue

            if has_status_col and status:
                # ── Driven by an explicit status column ──────────────────
                if status == "skipped":
                    continue

                if status in ("rejected", "block"):
                    label, is_error = _match(check_key, reason)
                    
                    if label == "Other / Unclassified Reason":
                        _r_low = (reason or "").lower()
                        if re.search(r"\b(?:refurbished|refurb|renewed)\b", _r_low):
                            continue
                        if any(k in _r_low for k in ("listing is incomplete", "key specs", "specifications to the title", "incomplete smartphone")):
                            continue
                        if check_key == "title_weight" and _match("title_english", reason)[0] != "Other / Unclassified Reason":
                            continue
                        elif check_key == "title_english" and _match("title_weight", reason)[0] != "Other / Unclassified Reason":
                            continue

                    if check_key == "color" and label == "Other / Unclassified Reason":
                        label = "Color: Other / Unclassified Reason (Overturned)"
                        verdict = "False Rejection"
                        _detail = f"Rejection overturned: Unclassified color rejection overturned -- product approved. Original: {reason or '(none provided)'}"
                    elif is_error:
                        verdict = "AI Error"
                    else:
                        verdict = _REASON_VERDICT.get(label)
                        if verdict is None:
                            verdict = _verify(check_key, rec, rule, weights, color_re)
                        _detail = reason or "No reason text provided."
                    rows.append({**_base_row(sid, check_key, rec), "Reason Type": label,
                                 "Verdict": verdict, "Detail": _detail})

                elif status in ("review", "manual review"):
                    label, is_error = _match(check_key, reason) if reason else ("Flagged For Manual Review", False)
                    
                    if label == "Other / Unclassified Reason" or label == "Flagged For Manual Review":
                        _r_low = (reason or "").lower()
                        if re.search(r"\b(?:refurbished|refurb|renewed)\b", _r_low):
                            continue
                        if any(k in _r_low for k in ("listing is incomplete", "key specs", "specifications to the title", "incomplete smartphone")):
                            continue
                        if check_key == "title_weight" and _match("title_english", reason)[0] not in ("Other / Unclassified Reason", "Flagged For Manual Review"):
                            continue
                        elif check_key == "title_english" and _match("title_weight", reason)[0] not in ("Other / Unclassified Reason", "Flagged For Manual Review"):
                            continue
                            continue

                    rows.append({**_base_row(sid, check_key, rec), "Reason Type": label,
                                 "Verdict": "AI Error" if is_error else "Needs Manual Review",
                                 "Detail": reason or "Flagged for manual review by the pipeline."})

                elif status == "approved":
                    _append_approved_findings(rows, sid, check_key, rec, rule, weights, color_re, reason)

                else:
                    # Status value we don't recognize (e.g. 'Fail', 'Flagged',
                    # 'Needs Fix') — surface it rather than silently dropping
                    # the row, so a wording mismatch is visible instead of
                    # making the whole check appear to have zero findings.
                    rows.append({**_base_row(sid, check_key, rec),
                                 "Reason Type": f"Unrecognized Status: '{rec.get(status_col)}'",
                                 "Verdict": "Needs Manual Review",
                                 "Detail": reason or "This check reported a status value the "
                                                      "audit tool doesn't recognize yet."})

            else:
                # ── No usable status column — infer purely from whether a
                # reason was written. Empty reason = nothing was flagged for
                # this check (treat as approved and check for a missed
                # mandatory-attribute violation); non-empty reason = the
                # check found something, classify it from the text itself.
                if not reason:
                    _append_approved_findings(rows, sid, check_key, rec, rule, weights, color_re, "")
                    continue

                if "error" in reason.lower():
                    rows.append({**_base_row(sid, check_key, rec), "Reason Type": "AI Error",
                                 "Verdict": "AI Error", "Detail": reason})
                    continue

                label, is_error = _match(check_key, reason)
                
                if label == "Other / Unclassified Reason" or label == "Flagged For Manual Review":
                    _r_low = (reason or "").lower()
                    if re.search(r"\b(?:refurbished|refurb|renewed)\b", _r_low):
                        continue
                    if any(k in _r_low for k in ("listing is incomplete", "key specs", "specifications to the title", "incomplete smartphone")):
                        continue
                    if check_key == "title_weight" and _match("title_english", reason)[0] not in ("Other / Unclassified Reason", "Flagged For Manual Review"):
                        continue
                    elif check_key == "title_english" and label == "Other / Unclassified Reason":
                        continue

                if check_key == "color" and label == "Other / Unclassified Reason":
                    label = "Color: Other / Unclassified Reason (Overturned)"
                    verdict = "False Rejection"
                    reason = f"Rejection overturned: Unclassified color rejection overturned -- product approved. Original: {reason or '(none provided)'}"
                elif is_error:
                    verdict = "AI Error"
                elif "manual review" in reason.lower() or label == "Flagged For Manual Review":
                    verdict = "Needs Manual Review"
                else:
                    verdict = _REASON_VERDICT.get(label)
                    if verdict is None:
                        verdict = _verify(check_key, rec, rule, weights, color_re)
                rows.append({**_base_row(sid, check_key, rec), "Reason Type": label,
                             "Verdict": verdict, "Detail": reason})

    if not rows:
        return pd.DataFrame(columns=["ProductSetSid", "Check", "Product Name", "Category",
                                      "Reason Type", "Verdict", "Detail"])
    df_out = pd.DataFrame(rows)
    # Remove the Unapproved Refurbished Seller sub-check from audit results.
    # The seller approval list is country-specific and often incomplete, causing
    # false positives for every seller not yet loaded into the support files.
    # Removing it here keeps the refurbished section focused on violations that
    # can actually be verified from the product data alone.
    if "Check" in df_out.columns and "Reason Type" in df_out.columns:
        _unapproved_seller_mask = (
            (df_out["Check"] == "refurbished")
            & (df_out["Reason Type"] == "AI Missed Refurbished - Unapproved Refurbished Seller")
        )
        df_out = df_out[~_unapproved_seller_mask].reset_index(drop=True)

    # ── Pipeline-overturned cases ─────────────────────────────────────────────
    # The main QC pipeline overwrites ZIP rejections when its own checks clear
    # a product (e.g. color accepted, Apple accessory brand on a cover). These
    # should appear in the targeted audit as False Rejections so reviewers see
    # the full picture of disagreements between the ZIP and the app.
    try:
        import streamlit as _st_ov
        _ov_cases = _st_ov.session_state.get("_pipeline_overturned_cases") or []
        if _ov_cases:
            _ov_rows = []
            for _ov in _ov_cases:
                _ov_sid = str(_ov.get("sid", "")).strip()
                _ov_flag = str(_ov.get("flag", "Unknown"))
                _ov_cmt = str(_ov.get("comment", ""))
                # Find the product name and category from the data frame.
                _ov_rec_rows = data[data["PRODUCT_SET_SID"].astype(str).str.strip() == _ov_sid]
                _ov_name = str(_ov_rec_rows["NAME"].iloc[0]) if not _ov_rec_rows.empty and "NAME" in _ov_rec_rows.columns else ""
                _ov_cat = str(_ov_rec_rows["CATEGORY"].iloc[0]) if not _ov_rec_rows.empty and "CATEGORY" in _ov_rec_rows.columns else ""
                _ov_seller = str(_ov.get("seller", ""))
                _ov_rows.append({
                    "ProductSetSid": _ov_sid,
                    "Check": "pipeline_overturned",
                    "Product Name": _ov_name,
                    "Category": _ov_cat,
                    "Seller": _ov_seller,
                    "Reason Type": _ov_flag,
                    "Verdict": "False Rejection",
                    "Detail": _ov_cmt,
                })
            if _ov_rows:
                df_out = pd.concat([df_out, pd.DataFrame(_ov_rows)], ignore_index=True)
    except Exception as _ov_err:
        logger.warning("Could not attach pipeline-overturned cases to audit: %s", _ov_err)

    return df_out



# ── AI-powered category rejection verifier ────────────────────────────────────
# Separate, optional pass: takes products the pipeline REJECTED for category
# (Category_Check_Status == 'Rejected') and asks an AI model whether that
# rejection was actually correct, using the same OpenAI-compatible gateway
# pattern as category_checker_app.py (keys.txt, batched calls). This is not
# part of evaluate_all_checks() — it's a separate opt-in pass the user
# triggers explicitly, since it costs real API calls and time.
import os as _os
import json as _json

_CATEGORY_AI_KEYS_FILE = _os.path.join(_os.path.dirname(__file__), "keys.txt")
if not _os.path.exists(_CATEGORY_AI_KEYS_FILE):
    _CATEGORY_AI_KEYS_FILE = _os.path.join(_os.path.dirname(__file__), "pages", "keys.txt")

_CATEGORY_AI_SYSTEM_PROMPT = """You are a strict e-commerce catalog QC auditor for Jumia.

You will be given a JSON array of products that our QC pipeline REJECTED for
category mismatch. Each has a name, description, brand, its FULL category path
(category_path), and the pipeline's stated rejection reason.

CRITICAL RULES:
- The category_path field contains the FULL hierarchical path, e.g.
  "Books, Movies and Music / Art & Humanities / Politics & History".
  A leaf category like "Politics & History" that lives under
  "Books, Movies and Music" IS a valid books category. Always read the
  FULL path, not just the last segment.
- A book titled with politics/history content in category
  "Books, Movies and Music / ... / Politics & History" is CORRECTLY categorized.
- Only mark "correct_rejection" when the FULL path is genuinely wrong for
  the product — not just because the leaf name sounds non-obvious.

For each product:
1. Identify what the product actually IS and its primary USE CASE from the
   name and description alone.
2. Read the FULL category_path. Judge whether the full path fits the product.
3. Decide:
   - If the full path is genuinely wrong for this product -> "correct_rejection"
   - If the full path is actually fine and the rejection was a mistake -> "wrong_rejection"

Respond with ONLY a valid JSON array, no markdown, no preamble, same order as
input, one compact object per product:
{
  "id": "<the id field from input, copied exactly>",
  "verdict": "correct_rejection" or "wrong_rejection",
  "reason": "1 short sentence explaining the verdict, max ~25 words"
}
"""

_CATEGORY_AI_COLS = [
    "NAME", "CATEGORY", "CATEGORY_CODE", "Initial_Category_Path",
    "Category_Check_Rejection_Reason", "DESCRIPTION", "SHORT_DESCRIPTION", "BRAND",
]


def _load_category_ai_keys(model_hint: str = "") -> list:
    """Same keys.txt format as category_checker_app.py: each line is either a
    bare key, or 'key:model_hint'. Returns a list of key strings whose hint
    matches the requested model (or all keys if no hint filtering applies)."""
    if not _os.path.exists(_CATEGORY_AI_KEYS_FILE):
        return []
    keys = []
    with open(_CATEGORY_AI_KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if ":" in line and not line.startswith("http"):
                key, hint = line.split(":", 1)
                key = key.strip()
                hint = hint.strip().lower()
            else:
                key, hint = line, ""
            
            # Filter: if the key is tagged for a specific family, only include
            # it when the requested model is in that family
            if hint:
                is_claude = "claude" in model_hint.lower() or "haiku" in model_hint.lower() or "sonnet" in model_hint.lower()
                is_gpt = "gpt" in model_hint.lower() or "openai" in model_hint.lower()
                hint_claude = "haiku" in hint or "claude" in hint or "sonnet" in hint
                hint_gpt = "gpt" in hint or "openai" in hint
                if is_claude and hint_gpt:
                    continue  # Skip GPT-only keys when using Claude model
                if is_gpt and hint_claude:
                    continue  # Skip Claude-only keys when using GPT model
            
            keys.append(key)
    return keys


def _build_category_ai_prompt(rows: list, desc_limit: int = 400) -> str:
    compact = []
    for i, row in enumerate(rows):
        entry = {"id": str(i)}
        for col in _CATEGORY_AI_COLS:
            val = row.get(col, "")
            if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
                continue
            val_str = str(val)
            if col in ("DESCRIPTION", "SHORT_DESCRIPTION") and len(val_str) > desc_limit:
                val_str = val_str[:desc_limit] + "...[truncated]"
            entry[col] = val_str
        # Always expose a 'category_path' key with the best available full path
        # so the AI never sees only a leaf node like 'Politics & History' alone.
        full_path = (
            str(row.get("Initial_Category_Path") or "")
            or str(row.get("CATEGORY") or "")
        ).strip()
        if full_path and full_path.lower() not in ("nan", "none"):
            entry["category_path"] = full_path
        compact.append(entry)
    return _json.dumps(compact, ensure_ascii=False)


def _call_category_ai_batch(api_key: str, base_url: str, model: str, rows: list, timeout: int = 90) -> list:
    """Returns a list of {'verdict': ..., 'reason': ...} dicts, one per row,
    same order as input. Falls back to an 'error' verdict per-row on any
    failure so the caller can surface it as an AI Error rather than crash."""
    empty = {"verdict": "error", "reason": ""}
    if not rows:
        return []
    user_prompt = _build_category_ai_prompt(rows)
    text = ""
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": _CATEGORY_AI_SYSTEM_PROMPT + "\n\n---\n\n" + user_prompt}],
        "max_tokens": min(120 * len(rows) + 150, 8000),
    }
    
    # Simple retry logic for transient gateway errors
    for attempt in range(3):
        try:
            resp = _requests.post(
                f"{base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=payload,
                timeout=timeout
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()
            text = text.replace("```json", "").replace("```", "").strip()
            parsed_list = _json.loads(text)
            by_id = {str(item.get("id")): item for item in parsed_list}
            ordered = []
            for i in range(len(rows)):
                item = by_id.get(str(i))
                if item is None:
                    item = dict(empty)
                    item["reason"] = "Missing from batch response"
                ordered.append(item)
            return ordered
        except _json.JSONDecodeError:
            if attempt == 2:
                err = dict(empty)
                err["reason"] = f"Could not parse AI response as JSON: {text[:200]}"
                return [dict(err) for _ in rows]
        except Exception as e:
            if attempt == 2:
                err = dict(empty)
                err["reason"] = f"API error: {e}"
                return [dict(err) for _ in rows]
            _time.sleep(2)
    return [dict(empty) for _ in rows]


def verify_category_rejections_with_ai(
    data: pd.DataFrame,
    base_url: str = "https://ai-gateway.zuma.jumia.com/v1",
    model: str = "claude-haiku-4.5",
    batch_size: int = 10,
    max_workers: int = 10,
    audit_df: pd.DataFrame = None,
    progress_bar = None,
    status_text = None
) -> pd.DataFrame:
    """
    Separate, optional pass: takes products the pipeline REJECTED for category
    and sends them to the GPT-4o-mini fast API to ask 'is this ACTUALLY a
    violation?'. Returns a DataFrame of just those results.
    """
    empty_cols = ["ProductSetSid", "Check", "Product Name", "Category", "Reason Type", "Verdict", "Detail"]
    if data.empty:
        return pd.DataFrame(columns=empty_cols)

    keys = _load_category_ai_keys(model)
    if not keys:
        return pd.DataFrame(columns=empty_cols)

    rejected_mask = pd.Series(False, index=data.index)
    if audit_df is not None and not audit_df.empty:
        cat_sids = audit_df[audit_df["Check"] == "category"]["ProductSetSid"].unique()
        if len(cat_sids) > 0:
            rejected_mask = data["PRODUCT_SET_SID"].isin(cat_sids)
            
    if not rejected_mask.any():
        if "QC Status" in data.columns:
            status_rej = data["QC Status"].astype(str).str.strip().str.lower() == "rejected"
            if "QC Reason" in data.columns:
                reason_cat = data["QC Reason"].astype(str).str.strip().str.lower() == "wrong category"
                rejected_mask |= (status_rej & reason_cat)
            if "Reason" in data.columns:
                reason_cat = data["Reason"].astype(str).str.strip().str.lower().str.contains("wrong category", na=False)
                rejected_mask |= (status_rej & reason_cat)
            if "FLAG" in data.columns:
                flag_cat = data["FLAG"].astype(str).str.strip().str.lower().str.contains("wrong category", na=False)
                rejected_mask |= (status_rej & flag_cat)
                
        if "Status" in data.columns:
            status_rej = data["Status"].astype(str).str.strip().str.lower() == "rejected"
            if "Reason" in data.columns:
                reason_cat = data["Reason"].astype(str).str.strip().str.lower().str.contains("wrong category", na=False)
                rejected_mask |= (status_rej & reason_cat)
            if "FLAG" in data.columns:
                flag_cat = data["FLAG"].astype(str).str.strip().str.lower().str.contains("wrong category", na=False)
                rejected_mask |= (status_rej & flag_cat)
                
        # Also support Seller Center columns if they happen to be named differently
        if "status" in data.columns and "rejectionReason" in data.columns:
            status_rej = data["status"].astype(str).str.strip().str.lower() == "rejected"
            reason_cat = data["rejectionReason"].astype(str).str.strip().str.lower().str.contains("wrong category", na=False)
            rejected_mask |= (status_rej & reason_cat)

    rejected = data.loc[rejected_mask].copy()
    if rejected.empty:
        return pd.DataFrame(columns=empty_cols)

    # Same duplicate hazard: _CATEGORY_AI_COLS already contains CATEGORY and
    # Initial_Category_Path, and the concatenation below adds them again.
    cols = list(dict.fromkeys(
        c for c in _CATEGORY_AI_COLS + ["PRODUCT_SET_SID", "CATEGORY", "Initial_Category_Path"]
        if c in rejected.columns
    ))
    rejected = rejected.loc[:, ~rejected.columns.duplicated()]
    cols = [c for c in cols if c in rejected.columns]
    records = rejected[cols].to_dict("records")

    import itertools as _itertools
    import concurrent.futures as _cf

    api_keys_cycle = _itertools.cycle(keys)
    results = [None] * len(records)

    chunks = [
        (start, records[start:start + batch_size])
        for start in range(0, len(records), batch_size)
    ]

    def _worker(chunk):
        start, chunk_rows = chunk
        api_key = next(api_keys_cycle)
        batch_results = _call_category_ai_batch(api_key, base_url, model, chunk_rows)
        return start, batch_results

    # If we have a status text element, show initial info
    if status_text is not None:
        status_text.markdown(f"**AI Category Verification**  \nFound **{len(records)}** rejected items to analyze. Splitting into **{len(chunks)}** batches...")
        
    completed_chunks = 0
    with _cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_worker, c) for c in chunks]
        for future in _cf.as_completed(futures):
            start, batch_results = future.result()
            for offset, result in enumerate(batch_results):
                results[start + offset] = result
            
            completed_chunks += 1
            if progress_bar is not None:
                progress = min(1.0, completed_chunks / len(chunks))
                progress_bar.progress(progress)
            if status_text is not None:
                items_done = min(len(records), completed_chunks * batch_size)
                status_text.markdown(f"**AI Category Verification**  \nProcessing: **{items_done}** / **{len(records)}** items completed...")

    rows = []
    for rec, result in zip(records, results):
        if result is None:
            continue
        sid = _clean(rec.get("PRODUCT_SET_SID"))
        verdict_raw = result.get("verdict", "error")
        reason = result.get("reason", "")
        if verdict_raw == "wrong_rejection":
            verdict = "False Rejection"
            reason_type = "AI: Category Rejection Overturned"
        elif verdict_raw == "correct_rejection":
            verdict = "True Rejection"
            reason_type = "AI: Category Rejection Confirmed"
        else:
            verdict = "AI Error"
            reason_type = "AI: Category Check Failed"
            
        best_category = _clean(rec.get("Initial_Category_Path")) or _clean(rec.get("CATEGORY"))
        rows.append({
            "ProductSetSid": sid,
            "Check": "category",
            "Product Name": _clean(rec.get("NAME")),
            "Category": best_category,
            "Reason Type": reason_type,
            "Verdict": verdict,
            "Detail": reason or "No reason returned.",
        })

    if not rows:
        return pd.DataFrame(columns=empty_cols)
    return pd.DataFrame(rows)
