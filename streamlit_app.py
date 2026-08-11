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

import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import requests
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
    TV_COLOR_EXEMPT_NODE, TV_COLOR_EXEMPT_PREFIX,
    ASPECT_ADVISORY_TALL, ASPECT_ADVISORY_WIDE,
    ASPECT_REJECT_TALL, ASPECT_REJECT_WIDE,
    COUNTRY_VALIDATOR_CONFIG,
    FLAG_CACHE_DIR,
    GRID_COLS,
    JUMIA_COLORS,
    PARQUET_CACHE_DIR,
    REASON_MAP,
    SNEAKER_BRAND_ALIASES,
)
from data_utils import (
    _detect_and_read_csv,
    _get_image_from_zip,
    _normalize_series,
    _repair_mojibake,
    MANUAL_DECISION_PREFIX,
    apply_manual_decisions,
    clean_category_code,
    create_match_key,
    create_match_key_vectorized,
    df_hash,
    filter_by_country,
    find_predecessor_decisions,
    load_df_parquet,
    load_manual_decisions,
    preview_decision_merge,
    propagate_metadata,
    save_df_parquet,
    standardize_input_data,
    validate_input_schema,
    normalize_text,
)
from custom_country_rules import check_kebs_banned_products, load_kebs_hb_codes, check_kebs_fda
from ghana_rules import check_ghana_smart_glasses, load_ghana_qc_rules
from loaders import compile_regex_patterns, load_support_files_lazy
from morocco_rules import check_morocco_prohibited_brands, load_morocco_qc_rules
from nigeria_rules import (
    check_nigeria_apple,
    check_nigeria_books,
    check_nigeria_gift_card,
    check_nigeria_hp_toners,
    check_nigeria_powerbanks,
    check_generic_powerbanks,
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
    checkpoint_final_report,
    flag_pill_header,
    render_context_rail,
    render_exports_section,
    render_flag_expander,
    render_image_grid,
    render_sibling_prompt,
    render_manual_review_buttons,
    render_rejection_donut,
    render_severity_group_header,
    render_summary_header,
    render_override_history,
)

# A JS injector used to live here. It reached into the parent document on
# every DOM mutation and repainted any expander whose label mentioned "ZIP"
# with a hardcoded tan (rgb(244,210,159)) set via !important — a fifth palette
# that overrode the stylesheet and could not be themed.
#
# It is gone for two reasons. Design: ZIP provenance is a footnote, not a
# whole background colour, and it now reads as a "from ZIP" badge inside the
# flag header where the severity ramp owns the colour. Cost: a MutationObserver
# on document.body ran colorExpander() for every mutation the app made, for
# the lifetime of the session, to style a handful of summaries.
# ──────────────────────────────────────────────────────────────────────────────

PREFETCH_MAP = {
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

# The single "Product Name Brand Name" validation is split into these
# sub-validations based on the reason text (see _classify_name_brand_sub_bucket).
# "Other" exists so any future/unrecognized reason still gets its own flag
# instead of being silently dropped or lumped into an existing bucket.
NAME_BRAND_SUB_FLAGS = [
    "Product Name Brand Name \u2013 Brand Repeated In Title",
    "Product Name Brand Name \u2013 Inspired/Alternative Perfume Brand",
    "Product Name Brand Name \u2013 Generic/Placeholder Brand",
    "Product Name Brand Name \u2013 High-End Brand Counterfeit Suspected",
    "Product Name Brand Name \u2013 Other",
]

TITLE_LANGUAGE_SUB_FLAGS = [
    "Title Language Check \u2013 Not In English",
    "Title Language Check \u2013 Other",
]

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
PROCESSING_CACHE_VERSION = "prefetch_context_v5"  # bumped: fake-perfume color-word fix + new validators
PREFETCH_VALIDATOR_SKIP_MAP = {
    "category_check": ["Wrong Category", "Category Check"],
    "warranty_check": ["Product Warranty", "Warranty Check"],
    "fda_check": ["FDA"],
    "color_check": ["Missing COLOR", "Color Check"],
    "variation_check": ["Wrong Variation", "Variation Check"],
    "product_name_brand_name": [
        "BRAND name repeated in NAME", "Product Name Brand Name",
        *NAME_BRAND_SUB_FLAGS,
    ],
    "brand_image_check": ["Brand Image Check"],
    "title_language_check": [
        "Missing Weight/Volume", "Title Language Check",
        *TITLE_LANGUAGE_SUB_FLAGS,
    ],
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



import re as _re_cat

# ── Category-Check reason → sub-bucket key ───────────────────────────────────
_CAT_API_ERROR_RE = _re_cat.compile(
    r'AI error\s*:|Error code\s*:\s*\d+|insufficient_quota|Connection error|'
    r'Request timed out|timed_out|rate.?limit|api.?error|openai\.com|'
    r'is_gateway_error|ReadTimeoutError|ConnectTimeout|ServiceUnavailable',
    _re_cat.IGNORECASE,
)

def _classify_name_brand_sub_bucket(reason: str) -> str:
    """Map Product name_Brand name_rejection reason to its sub-validation flag.

    Splits the single 'Product Name Brand Name' validation into its distinct
    underlying issue types, plus an 'Other' bucket so any future/unrecognized
    reason text still gets surfaced under its own flag instead of silently
    lumping into the parent flag or getting dropped.
    """
    r = str(reason).strip()
    low = r.lower()
    if not r or low == "nan":
        return "Product Name Brand Name \u2013 Other"

    if "repeated in product name" in low or "brand name is not repeated" in low:
        return "Product Name Brand Name \u2013 Brand Repeated In Title"

    if "inspired" in low and "perfume" in low:
        return "Product Name Brand Name \u2013 Inspired/Alternative Perfume Brand"

    if ("placeholder" in low and "brand" in low) or "brand field is 'generic'" in low or "generic brand is not allowed" in low:
        return "Product Name Brand Name \u2013 Generic/Placeholder Brand"

    if "high-end brand" in low or "counterfeit" in low:
        return "Product Name Brand Name \u2013 High-End Brand Counterfeit Suspected"

    return "Product Name Brand Name \u2013 Other"


def _classify_title_language_sub_bucket(reason: str) -> str:
    r = str(reason).strip()
    low = r.lower()
    if not r or low == "nan":
        return "Title Language Check \u2013 Other"

    if "not in english" in low:
        return "Title Language Check \u2013 Not In English"

    return "Title Language Check \u2013 Other"


_RE_JERSEY_1 = re.compile(r'replica jersey')
_RE_JERSEY_2 = re.compile(r'jersey')
_RE_JERSEY_3 = re.compile(r'licensed|protected|brand|team')
_RE_BABY_1 = re.compile(r'baby')
_RE_BABY_2 = re.compile(r'adult|women|women\'s|men|hosiery|socks|footwear|eu 3|eu 4')
_RE_BABY_3 = re.compile(r'baby|toddler')
_RE_BABY_4 = re.compile(r'non-baby|non baby')
_RE_FRAGRANCE_1 = re.compile(r'fragrance|perfume|deodorant body spray')
_RE_FRAGRANCE_2 = re.compile(r'unisex|men\'s|women\'s|decorative|grooming|oral care|lotion|skin care')
_RE_CLOTHING = re.compile(r't-shirt|jeans|trousers|vest|thobe|satin shirt|button-down|boot|oxford|derby|outerwear|climbing gear|hosiery|socks')
_RE_HAIR = re.compile(r'hair clipper|hair dryer|hair trimmer|hair darkening|hair coloring|hair cut|balding')
_RE_ELECTRONICS = re.compile(r'earphone|headphone|keyboard|mouse combo|laptop|dome camera|bullet camera|sports camera')
_RE_KITCHEN = re.compile(r'toaster|kitchen appliance|electric coil cooker|hotplate|insect killer')
_RE_HEALTH = re.compile(r'dietary supplement|weight management|energy chew|ashwagandha|herbal supplement')
_RE_FOOD = re.compile(r'wine|stout beer|tonic water|cocktail mixer')
_RE_SKINCARE = re.compile(r'face cream|facial|serum|kaolin clay|body moisturizer')
_RE_LIGHTING = re.compile(r'emergency lamp|outdoor light|garden light|heat bulb|solar light|specialty bulb|agricultural machinery')
_RE_BEDDING = re.compile(r'duvet|comforter|mosquito net')
_RE_TOOLS = re.compile(r'hacksaw|router bit|woodworking|agricultural')
_RE_MEDICAL = re.compile(r'diagnostic medical|support hose|compression category')

def _classify_category_check_sub_bucket(reason: str) -> str:
    """Map Category_Check_Rejection_Reason to its sub-bucket key."""
    r = str(reason).strip()
    if _CAT_API_ERROR_RE.search(r):
        return "Category Check – AI API Errors"

    low = r.lower()
    if not r or low == 'nan':
        return "Category Check – Other Mismatch"

    if 'prohibited' in low: return "Category Check – Prohibited Category"
    if 'inactive' in low: return "Category Check – Inactive Category"
    if _RE_JERSEY_1.search(low) or (_RE_JERSEY_2.search(low) and _RE_JERSEY_3.search(low)): return "Category Check – Replica Jersey / IP Violation"
    if 'sexual wellness' in low or 'intimate product' in low: return "Category Check – Sexual Wellness Miscategory"
    if 'pet product' in low: return "Category Check – Pet product listed under non-pet category"
    if _RE_BABY_1.search(low) and _RE_BABY_2.search(low): return "Category Check – Adult product listed under Baby category"
    if _RE_BABY_3.search(low) and _RE_BABY_4.search(low): return "Category Check – Baby/toddler listed under non-baby category"
    if _RE_FRAGRANCE_1.search(low) and _RE_FRAGRANCE_2.search(low): return "Category Check – Fragrance/Perfume Mismatch"
    if 'book' in low: return "Category Check – Books Wrong Subcategory"
    if _RE_CLOTHING.search(low): return "Category Check – Clothing Subcategory Mismatch"
    if _RE_HAIR.search(low): return "Category Check – Hair / Grooming Appliance Mismatch"
    if _RE_ELECTRONICS.search(low): return "Category Check – Electronics / Accessories Mismatch"
    if _RE_KITCHEN.search(low): return "Category Check – Kitchen / Home Appliance Mismatch"
    if _RE_HEALTH.search(low): return "Category Check – Health / Supplement Mismatch"
    if _RE_FOOD.search(low): return "Category Check – Food / Beverage Mismatch"
    if _RE_SKINCARE.search(low): return "Category Check – Skincare Subcategory Mismatch"
    if _RE_LIGHTING.search(low): return "Category Check – Lighting Mismatch"
    if _RE_BEDDING.search(low): return "Category Check – Bedding / Linen Mismatch"
    if _RE_TOOLS.search(low): return "Category Check – Tools / Hardware Mismatch"
    if _RE_MEDICAL.search(low): return "Category Check – Medical Device Mismatch"

    return "Category Check – Other Mismatch"


def _derive_prefetched_skip_list(qc_df: pd.DataFrame) -> List[str]:
    skip = set()
    if qc_df.empty:
        return []
    status_cols = [c for c in qc_df.columns if "status" in str(c).lower()]
    for col in status_cols:
        skip.update(
            PREFETCH_VALIDATOR_SKIP_MAP.get(_prefetch_key_from_status_col(col), [])
        )
    # Duplicate_Flag deliberately does NOT skip our own duplicate check.
    #
    # It used to, on the reasonable-sounding grounds that the ZIP had already
    # looked. It hasn't: Duplicate_Flag is an observation with a single value
    # ("Duplicate — same seller + product name") and no verdict — there is no
    # Duplicate_Check_Status beside the other eight *_Check_Status columns, and
    # of 1,747 duplicate-flagged products the platform rejected in one KE
    # batch, not one was rejected for being a duplicate.
    #
    # So the skip handed the job to something that never does it, and on any
    # batch with a ZIP duplicates were rejected by nobody while still showing a
    # badge on the card and filling the audit — handled everywhere except where
    # it counts.
    return sorted(skip)


def restore_single_item(sid):
    fr = st.session_state.final_report
    sid_str = str(sid).strip()
    mask = fr["ProductSetSid"].astype(str).str.strip() == sid_str
    if not mask.any():
        return

    if "manual_undone_tracker" not in st.session_state:
        st.session_state.manual_undone_tracker = {}

    if len(st.session_state.manual_undone_tracker) > 200:
        keys = list(st.session_state.manual_undone_tracker.keys())
        for k in keys[:100]:
            del st.session_state.manual_undone_tracker[k]

    current_flag = fr.loc[mask, "FLAG"].iloc[0]
    st.session_state.manual_undone_tracker.setdefault(sid_str, set()).add(current_flag)

    qc_zip = st.session_state.get("zip_qc_results", pd.DataFrame())
    if not qc_zip.empty:
        sid_col = None
        for possible in ["PRODUCT_SET_SID", "ProductSetSid", "Product Set SID", "cod_productset_sid", "SID"]:
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
                        flag = PREFETCH_MAP.get(col_key, col_key.replace("_", " ").title())
                        flag_prefetched = f"{flag} (Prefetched)"

                        if flag_prefetched not in st.session_state.manual_undone_tracker[sid_str]:
                            mapped_info = fmap.get(flag, {})
                            reason_code = mapped_info.get("reason", "1000007 - Other Reason")
                            default_cmt = mapped_info.get("comment", "Rejected")

                            zip_cmt = _prefetch_reason_from_row(r, col, qc_zip.columns)
                            final_comment = zip_cmt if (zip_cmt and zip_cmt.lower() not in ("rejected", "nan")) else default_cmt

                            apply_status_change(
                                [sid_str],
                                status="Rejected",
                                reason=reason_code,
                                comment=final_comment,
                                flag=flag_prefetched,
                                is_manual=True,
                                is_zip=True,
                            )
                            st.session_state.main_toasts.append(f"Product still rejected for: {flag}")
                            return

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

_SCRAPER_AVAILABLE = False

try:
    from category_matcher_engine import (
        CategoryMatcherEngine,
        check_wrong_category,
        get_engine,
    )
    _CAT_MATCHER_AVAILABLE = True
except ImportError:
    _CAT_MATCHER_AVAILABLE = False
    def check_wrong_category(data, categories_list=None, cat_path_to_code=None, code_to_path=None, confidence_threshold=0.0):
        if "CATEGORY" not in data.columns:
            return pd.DataFrame(columns=data.columns)
        flagged = data[data["CATEGORY"].astype(str).str.contains("miscellaneous", case=False, na=False)].copy()
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
    now = time.time()
    try:
        patterns = ["*.pkl", "*.parquet"]
        files = []
        for p in patterns:
            files.extend(list(Path(directory).glob(p)))
        # Manual decision journals are never pruned: they hold the only copy of
        # work a human did by hand, which no amount of recomputation can rebuild.
        # Everything else here is a derived cache and is safe to discard.
        files = [f for f in files if not f.name.startswith(MANUAL_DECISION_PREFIX)]

        for f in files:
            if (now - f.stat().st_mtime) > max_age_days * 86400:
                f.unlink(missing_ok=True)

        remaining = []
        for p in patterns:
            remaining.extend(list(Path(directory).glob(p)))
        remaining = [f for f in remaining if not f.name.startswith(MANUAL_DECISION_PREFIX)]
        remaining.sort(key=os.path.getmtime)

        for f in remaining[:-max_files]:
            f.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Cache pruning failed for {directory}: {e}")


@st.cache_resource(show_spinner=False)
def _prune_caches_once() -> bool:
    """Prune the on-disk caches once per process, not once per rerun.

    Streamlit re-executes this whole script on every interaction, and pruning
    globs + stats + sorts every file in both cache dirs (measured at ~68ms with
    ~600 files) — pure overhead on every click. Housekeeping at startup is
    enough; nothing here needs to react to files written mid-session.
    """
    prune_cache_dir(FLAG_CACHE_DIR)
    prune_cache_dir(PARQUET_CACHE_DIR)
    return True


_prune_caches_once()


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


FLAG_RELEVANT_COLS = {
    "Wrong Category": ["NAME", "CATEGORY", "CATEGORY_CODE"],
    # DESCRIPTION/SHORT_DESCRIPTION are listed because both checks now read
    # them. This map also scopes the per-flag cache digest, so leaving them out
    # would mean an edited description never invalidated the cached result.
    # "Matched In" says which of the four the brand was actually found in.
    "Restricted brands": ["NAME", "BRAND", "SELLER_NAME", "CATEGORY_CODE", "CATEGORY",
                          "DESCRIPTION", "SHORT_DESCRIPTION", "Matched In"],
    # NAME and the descriptions are listed because the price ceiling now looks
    # for the brand claim there too, not just in the BRAND field. This map also
    # scopes the per-flag cache digest.
    "Suspected Fake product": ["CATEGORY_CODE", "BRAND", "NAME", "GLOBAL_SALE_PRICE",
                               "GLOBAL_PRICE", "DESCRIPTION", "SHORT_DESCRIPTION"],
    "Seller Not approved to sell Refurb": ["PRODUCT_SET_SID", "CATEGORY_CODE", "SELLER_NAME", "NAME"],
    "Product Warranty": ["PRODUCT_WARRANTY", "WARRANTY_DURATION", "CATEGORY_CODE"],
    "Seller Approve to sell books": ["CATEGORY_CODE", "SELLER_NAME"],
    "Seller Approved to Sell Perfume": ["CATEGORY_CODE", "SELLER_NAME", "BRAND", "NAME"],
    "Counterfeit Sneakers": ["CATEGORY_CODE", "NAME", "BRAND",
                             "DESCRIPTION", "SHORT_DESCRIPTION", "Matched In",
                             "Brand Claim"],
    "Suspected counterfeit Jerseys": ["CATEGORY_CODE", "NAME", "SELLER_NAME"],
    "Suspected Fake Perfume": ["CATEGORY_CODE", "NAME", "BRAND"],
    "Unnecessary words in NAME": ["NAME"],
    "Single-word NAME": ["CATEGORY_CODE", "NAME"],
    "Generic BRAND Issues": ["CATEGORY_CODE", "BRAND"],
    "Fashion brand issues": ["CATEGORY_CODE", "BRAND"],
    "BRAND name repeated in NAME": ["BRAND", "NAME"],
    "Brand Image Check": ["BRAND", "NAME", "Brand_Image_Check_Reason", "Brand_Detected_On_Product"],
    "Product Name Brand Name – Brand Repeated In Title": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Product Name Brand Name – Inspired/Alternative Perfume Brand": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Product Name Brand Name – Generic/Placeholder Brand": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Product Name Brand Name – High-End Brand Counterfeit Suspected": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Product Name Brand Name – Other": ["BRAND", "NAME", "Product name_Brand name_rejection reason"],
    "Wrong Variation": ["COUNT_VARIATIONS", "CATEGORY_CODE"],
    "Generic branded products with genuine brands": ["NAME", "BRAND", "CATEGORY"],
    "Missing COLOR": ["CATEGORY_CODE", "NAME", "COLOR"],
    "Missing Weight/Volume": ["CATEGORY_CODE", "NAME"],
    "Incomplete Smartphone Name": ["CATEGORY_CODE", "NAME"],
    "Specs Inconsistency": ["CATEGORY_CODE", "NAME", "DESCRIPTION", "SHORT_DESCRIPTION", "CATEGORY"],
    "Brand Image Mismatch": ["BRAND", "NAME", "Brand_Detected_On_Product", "SELLER_NAME"],
    # The localised columns carry real content in the French and Arabic
    # markets — in one Uganda batch alone, 233 French and 225 Arabic
    # descriptions were populated. Scanning only the base columns meant a
    # phone number sitting in DESCRIPTION_AR was invisible to this check.
    "Off-Platform Contact": [
        "NAME", "NAME_FR", "NAME_AR",
        "DESCRIPTION", "DESCRIPTION_FR", "DESCRIPTION_AR",
        "SHORT_DESCRIPTION", "SHORT_DESCRIPTION_FR", "SHORT_DESCRIPTION_AR",
        "SELLER_NAME",
    ],
    "Duplicate product": ["NAME", "SELLER_NAME", "BRAND", "CATEGORY_CODE", "COLOR", "COLOR_FAMILY", "MAIN_IMAGE"],
    "Perfume Tester": ["CATEGORY_CODE", "NAME"],
    "Discount too high": ["GLOBAL_PRICE", "GLOBAL_SALE_PRICE"],
    "Suspicious Discount": ["GLOBAL_PRICE", "GLOBAL_SALE_PRICE"],
    "Poor images": ["MAIN_IMAGE"],
    "Image Stretched": ["MAIN_IMAGE", "NAME", "BRAND"],
    "Image Blurry": ["MAIN_IMAGE", "NAME", "BRAND"],
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
    "ALL CAPS Product Name": ["NAME"],
    "Product Name Too Short": ["NAME"],
    "Variation Name Mismatch": ["NAME"],
    "Prohibited products": ["NAME", "CATEGORY_CODE"],
    "FDA": ["CATEGORY_CODE"],
    # Deliberately NOT listed, so they fall back to hashing the whole frame:
    #   "KEBS Banned Products", "KEBS FDA", "MA - Marque Interdite"
    # Their checks live in other modules and were not audited here; guessing
    # a narrow column set for them would risk serving stale QC verdicts.
}

# Rules from general_rules.py declare their own columns. Without this every one
# of them would be unmapped and fall back to hashing the whole frame on each
# run, so editing any unrelated column would invalidate their cache.
try:
    from general_rules import relevant_columns as _general_relevant_columns

    # Union, not replace. A rule can be filed under an existing flag —
    # "Wrong Category" has a built-in check of its own reading NAME and
    # CATEGORY — and update() would have narrowed that entry to whatever
    # columns the rule declares. The cache would then miss an edit to a column
    # the built-in check reads and serve its previous verdict.
    for _flag, _cols in _general_relevant_columns().items():
        _existing = FLAG_RELEVANT_COLS.get(_flag) or []
        FLAG_RELEVANT_COLS[_flag] = sorted(set(_existing) | set(_cols))
except Exception:  # a broken rules file must not stop the app importing
    logging.getLogger(__name__).exception("general_rules: could not read column map")


# Bump when the cache-key scheme changes, so pickles written by an older scheme
# can never be read back under a key that now means something different.
# fk3: entries written while several checks shared one cache file are wrong and
# have to be discarded rather than left to expire. Any pickle from that period
# holds whichever sibling finished first, and the built-in Wrong Category key
# is unchanged by the check_id fix — so without this bump it would keep serving
# a rule's result, or an empty one, as the whole flag's verdict.
FLAG_CACHE_KEY_VERSION = "fk3"

# Columns every check implicitly depends on: results are keyed by SID, and the
# row set itself is part of a check's input.
_ALWAYS_RELEVANT_COLS = ("PRODUCT_SET_SID",)


class _ColumnDigests:
    """Per-column content digests for one DataFrame, computed at most once each.

    Hashing each flag's column subset independently would re-hash shared columns
    dozens of times (NAME alone is used by ~20 checks). Hashing each column once
    and composing per-flag keys from those digests costs about the same as the
    single whole-frame hash it replaces.
    """

    def __init__(self, df: pd.DataFrame):
        self._df = df
        self._cols: Dict[str, str] = {}
        self._whole: Optional[str] = None

    def _column(self, col: str) -> str:
        if col not in self._cols:
            if col in self._df.columns:
                try:
                    self._cols[col] = hashlib.md5(
                        pd.util.hash_pandas_object(self._df[col], index=False).values.tobytes()
                    ).hexdigest()
                except Exception:
                    # Unhashable dtype (object columns holding dicts/lists) —
                    # fall back to something conservative but still content-based.
                    self._cols[col] = hashlib.md5(
                        self._df[col].astype(str).str.cat(sep="\x1f").encode("utf-8", "replace")
                    ).hexdigest()
            else:
                # Absent is itself meaningful: a check behaves differently when a
                # column is missing, so it must not collide with "present".
                self._cols[col] = "\x00absent"
        return self._cols[col]

    def signature(self, cols: Optional[List[str]]) -> str:
        if cols is None:
            # Whole-frame fallback for unmapped flags. Composed from the same
            # per-column digests rather than df_hash(): df_hash memoises into
            # df.attrs, and DataFrame.copy() carries attrs across, so a frame
            # copied from an already-hashed one and then modified reports the
            # ORIGINAL hash. Composing here is both correct and free for
            # columns another flag already hashed.
            if self._whole is None:
                self._whole = hashlib.md5(
                    "|".join(
                        f"{c}={self._column(c)}" for c in sorted(self._df.columns)
                    ).encode()
                ).hexdigest()
            return "ALL:" + self._whole
        wanted = sorted(set(cols) | set(_ALWAYS_RELEVANT_COLS))
        return "COLS:" + "|".join(f"{c}={self._column(c)}" for c in wanted)


def flag_cache_path(
    name: str,
    digests: "_ColumnDigests",
    country_code: str,
    rules_sig: str,
    check_id: str = "",
) -> str:
    """Where this flag's result for this exact input is cached.

    The key covers only the columns the check actually reads (per
    FLAG_RELEVANT_COLS), so editing an unrelated column no longer invalidates
    every flag. A flag with no mapping falls back to the whole-frame hash.

    country_code is part of the key because several checks resolve
    country-specific rule sets; without it, the same rows validated for a
    different country could be served another country's verdicts.
    """
    # check_id separates checks that share a flag name. Several general rules
    # are filed under "Wrong Category" alongside the built-in check of that
    # name, and keying on the name alone gave all of them ONE cache file: they
    # run concurrently, the first to finish writes it, and the rest read that
    # sibling's result instead of computing their own. On the next run every
    # one of them reads the same pickle, so whichever check happened to write
    # it — an empty result from any one of them — became the verdict for the
    # entire flag. That is a flag that silently finds nothing.
    key = "\x1e".join((
        FLAG_CACHE_KEY_VERSION,
        name,
        check_id,
        country_code,
        rules_sig,
        digests.signature(FLAG_RELEVANT_COLS.get(name)),
    ))
    return os.path.join(FLAG_CACHE_DIR, f"{hashlib.md5(key.encode()).hexdigest()}.pkl")


def run_cached_check(func, cache_path, ckwargs):
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            try:
                os.unlink(cache_path) 
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


_IMAGE_DIM_CACHE = _BoundedDict(maxsize=5000)
_IMAGE_HASH_CACHE = _BoundedDict(maxsize=5000)
_IMAGE_DIM_LOCK = threading.Lock()
# Staging area for the low-resolution advisory produced by check_image_blurry.
# That check runs on a worker thread, where st.session_state writes are silently
# dropped; validate_products drains this into session_state on the main thread.
_IMAGE_BLURRY_COMMENTARY: dict = {}


def _compute_phash(img_bytes: bytes) -> str:
    try:
        import imagehash
        img = Image.open(BytesIO(img_bytes)).convert("RGB")
        return str(imagehash.phash(img))
    except Exception:
        return ""


def _size_and_phash(img_bytes: bytes):
    """Dimensions and perceptual hash from a single decode.

    This used to be two calls that each opened the same bytes: one for .size,
    one inside _compute_phash that fully decoded the image to hash it. Both the
    duplicate check and the aspect-ratio check depend on the results, and every
    unique image in a batch goes through here.

    Two things make it ~19x faster per image, measured on a 2000x1500 JPEG:
    120.6ms -> 6.5ms.

      • One open instead of two.
      • draft() before hashing. It is a JPEG-only, decoder-level downscale, so
        the file is decoded at 1/2, 1/4 or 1/8 scale rather than in full and
        then resized. phash reduces to 32x32 internally, so 256px carries far
        more detail than it can use — verified over 30 generated photos at
        assorted sizes that the resulting hash is byte-identical to hashing at
        full resolution, which it has to be: duplicate detection compares
        hashes with ==, so a hash that merely came close would stop matching
        the same product hashed the other way.

    size is read BEFORE draft(). draft() mutates the image and .size then
    reports the reduced dimensions, which would feed the aspect-ratio rule
    wrong numbers.
    """
    try:
        img = Image.open(BytesIO(img_bytes))
        size = img.size
    except Exception:
        return None, ""
    ph = ""
    try:
        import imagehash
        try:
            img.draft("RGB", (256, 256))   # no-op for non-JPEG formats
        except Exception:
            pass
        ph = str(imagehash.phash(img.convert("RGB")))
    except Exception:
        ph = ""
    return size, ph


if "zip_image_store" not in st.session_state:
    st.session_state.zip_image_store = {}
if "zip_image_index" not in st.session_state:
    st.session_state.zip_image_index = {}
if "zip_image_source_bytes" not in st.session_state:
    st.session_state.zip_image_source_bytes = None

# TV colour exemption — ON by default, temporarily, until the seven television
# categories are set to "No Need" in QC Check Validaton.xlsx. That sheet is
# what actually asserts televisions need a colour; this is an override sitting
# on top of it, which is why it is meant to be removed rather than kept.
#
# Seeded here rather than left to the sidebar toggle's default. Three of the
# four places that read this use a bare .get(), which returns None when the key
# is absent — so on the first run of a session, before the sidebar has
# rendered, the exemption would silently be off for the batch being processed.
if "tv_color_exempt" not in st.session_state:
    st.session_state.tv_color_exempt = True

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
SID_COLUMN_CANDIDATES = ["PRODUCT_SET_SID", "ProductSetSid", "Product Set SID", "cod_productset_sid", "SID"]


def _find_sid_col(df: pd.DataFrame) -> Optional[str]:
    return next((c for c in SID_COLUMN_CANDIDATES if c in df.columns), None)


def _basename_lower(value) -> str:
    name = str(value).strip().replace("\\", "/").split("/")[-1].lower()
    return name if name and name != "nan" else ""


def _index_zip_images(zf: zipfile.ZipFile) -> Dict[str, str]:
    """Index every image sitting in an images/ folder, at any depth.

    This required the folder to be at the top level. Real archives from the QC
    pipeline put it one level down — "output/images/..." — so the index came
    back empty and the grid never had a single picture from them, silently.
    Verified against a real 1,929-image batch: 0 indexed before, all of them
    after.

    Still anchored to a folder named "images" rather than taking any image
    anywhere in the archive, so a logo or a thumbnail dropped beside the data
    files is not mistaken for product photography.
    """
    return {
        _basename_lower(info.filename): info.filename
        for info in zf.infolist()
        if ("images/" in info.filename.lower().replace("\\", "/"))
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


# 🚀 OPTIMIZED: Double Download Eliminated
def _fetch_all_image_dimensions(data: pd.DataFrame) -> dict:
    """
    Download all unique images ONCE and cache. Both caches are filled 
    in a single network pass using session pooling. Thread-safe.
    """
    if "MAIN_IMAGE" not in data.columns:
        return {}
    _all_urls = data["MAIN_IMAGE"].astype(str)
    urls = _all_urls[_all_urls.str.strip().str.startswith("http")].unique()
    with _IMAGE_DIM_LOCK:
        new_urls = list(dict.fromkeys(u for u in urls if u and u not in _IMAGE_DIM_CACHE))

    # Descriptors only — three short strings per image, not the image itself.
    #
    # This used to collect the decoded payload for every ZIP image up front and
    # hold them all until the last one had been measured. _get_image_from_zip
    # returns a base64 data URI, roughly 1.37x the raw file, so a batch of
    # 3,000 images at 150KB each parked about 0.6GB in one list before any work
    # started; 6,000 images, or larger photos, doubled that. The store those
    # payloads pass through is capped at 300 entries, which looks like a bound
    # and is not one: the list held its own reference to every payload, so
    # eviction from the store freed nothing.
    #
    # Each image is needed for as long as it takes to read its size and hash.
    # Holding it after that was the whole cost.
    zip_descriptors = []
    store = st.session_state.get("zip_image_store", {})
    if store and "MAIN_IMAGE" in data.columns:
        _zip_mask = (
            data["MAIN_IMAGE"].astype(str).str.strip().ne("")
            & ~data["MAIN_IMAGE"].astype(str).str.startswith("http")
        )
        if _zip_mask.any():
            _zip_subset = data.loc[_zip_mask, ["MAIN_IMAGE"]].copy()
            for c in ["NAME", "BRAND"]:
                if c in data.columns:
                    _zip_subset[c] = data.loc[_zip_mask, c]
            _zip_subset = _zip_subset.drop_duplicates(subset=["MAIN_IMAGE"])
            for _zrow in _zip_subset.itertuples():
                img_val = str(_zrow.MAIN_IMAGE)
                if img_val in _IMAGE_DIM_CACHE:
                    continue
                zip_descriptors.append((
                    img_val,
                    str(getattr(_zrow, "NAME", "")),
                    str(getattr(_zrow, "BRAND", "")),
                ))

    if not new_urls and not zip_descriptors:
        return _IMAGE_DIM_CACHE

    def fetch(url):
        session = get_image_session()
        try:
            # OPTIMIZED: Removed duplicate bare requests.get call
            r = session.get(url.replace("http://", "https://"), timeout=6)
            if r.status_code == 200:
                raw = r.content
                size, ph = _size_and_phash(raw)
                return url, size, ph
        except Exception:
            pass
        return url, None, ""

    def process_zip_img(tup):
        key, payload = tup
        try:
            if isinstance(payload, str) and payload.startswith("data:"):
                _, encoded = payload.split(",", 1)
                raw = base64.b64decode(encoded)
            else:
                raw = payload if isinstance(payload, (bytes, bytearray)) else b""
            size, ph = _size_and_phash(raw)
            return key, size, ph
        except Exception:
            pass
        return key, None, ""

    results = []
    # Lowered thread concurrency limit to prevent socket exhaustion
    _img_workers = min(16, max(4, (os.cpu_count() or 4) * 2))
    if new_urls:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_img_workers) as executor:
            results.extend(list(executor.map(fetch, new_urls)))

    if zip_descriptors:
        # Peak memory is now this many images rather than the whole batch, so
        # it stays flat as batches grow. The results kept afterwards are a
        # size tuple and a hash string per image — tens of bytes — so those can
        # safely be held for all of them.
        _ZIP_DECODE_CHUNK = 200
        _zip_workers = min(8, _img_workers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=_zip_workers) as executor:
            for _start in range(0, len(zip_descriptors), _ZIP_DECODE_CHUNK):
                _slice = zip_descriptors[_start:_start + _ZIP_DECODE_CHUNK]
                # Read on this thread, not in the workers. _get_image_from_zip
                # goes through st.session_state, and calling that from a worker
                # is what the "missing ScriptRunContext" warnings are about — it
                # happens to work today and is not worth leaning on harder.
                _payloads = []
                for _img_val, _name, _brand in _slice:
                    _payload = _get_image_from_zip(_name, _brand, _img_val)
                    if _payload:
                        _payloads.append((_img_val, _payload))
                if _payloads:
                    results.extend(list(executor.map(process_zip_img, _payloads)))
                # Dropped before the next chunk is built, so two chunks are
                # never alive at once.
                _payloads = None

    with _IMAGE_DIM_LOCK:
        for key, size, ph in results:
            # Record failures too. Without this, a URL that 404s or times out is
            # absent from the cache, so it lands in `new_urls` again on the next
            # run and is re-fetched with the full 6s timeout + 2 retries — every
            # single validation, forever.
            _IMAGE_DIM_CACHE[key] = size if size else None
            if ph:
                _IMAGE_HASH_CACHE[key] = ph

    # Publish a snapshot for ui_components, which needs the hashes to find the
    # same photo listed by another seller but cannot import this module (it is
    # the entry script, and streamlit_app already imports ui_components).
    # A dict copy of at most a few thousand short strings, once per validation.
    try:
        st.session_state["_image_phash_by_url"] = dict(_IMAGE_HASH_CACHE)
    except Exception:
        pass
    return _IMAGE_DIM_CACHE


def check_image_stretched(data: pd.DataFrame, _image_cache: dict = None) -> pd.DataFrame:
    if "MAIN_IMAGE" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    target = data[data["MAIN_IMAGE"].astype(str).str.strip() != ""].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    url_data = _image_cache if _image_cache else _fetch_all_image_dimensions(target)

    url_issues = {}
    # Only this dataset's URLs — _IMAGE_DIM_CACHE is process-wide and holds up
    # to 5000 entries from earlier runs. A None value marks a fetch that failed.
    for url in target["MAIN_IMAGE"].astype(str).unique():
        dims = url_data.get(url)
        if not dims:
            continue
        w, h = dims
        if w > 0:
            ratio = h / w
            # Only the extreme tier rejects. Between the two bounds the image
            # is merely unusual, and that is left to the grid to show as
            # commentary — see ASPECT_ADVISORY_* below.
            if ratio > ASPECT_REJECT_TALL:
                url_issues[url] = f"Image Stretched - Tall Aspect Ratio ({w}x{h})"
            elif ratio < ASPECT_REJECT_WIDE:
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

    url_data = _image_cache if _image_cache else _fetch_all_image_dimensions(target)

    reject_map = {}
    commentary_map = {}
    # Only this dataset's URLs (see check_image_stretched); None = failed fetch.
    for url in target["MAIN_IMAGE"].astype(str).unique():
        dims = url_data.get(url)
        if not dims:
            continue
        w, h = dims
        if w <= 200 and h <= 200:
            reject_map[url] = f"Image too small/blurry ({w}x{h}px) — below 200x200"
        elif w < 250 and h < 250:
            commentary_map[url] = (
                f"Image resolution low ({w}x{h}px) — consider upgrading"
            )

    # This runs on a validator worker thread, which has no Streamlit script run
    # context — st.session_state there reads as empty and silently DISCARDS
    # writes, so the low-resolution advisory was never populated. Stage it in a
    # module-level dict; validate_products drains it on the main thread.
    if commentary_map:
        sid_to_comment = {}
        for row in target.itertuples():
            url = str(getattr(row, "MAIN_IMAGE", ""))
            if url in commentary_map:
                sid_to_comment[str(getattr(row, "PRODUCT_SET_SID", ""))] = commentary_map[url]
        if sid_to_comment:
            with _IMAGE_DIM_LOCK:
                _IMAGE_BLURRY_COMMENTARY.update(sid_to_comment)

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
    return check_image_stretched(data)


def check_miscellaneous_category(
    data: pd.DataFrame,
    categories_list: list = None,
    compiled_rules: dict = None,
    cat_path_to_code: dict = None,
    code_to_path: dict = None,
    country_code: str = None,
) -> pd.DataFrame:
    if not categories_list or not code_to_path:
        try:
            _sf = st.session_state.get("support_files", {})
            categories_list = categories_list or _sf.get("categories_names_list", [])
            cat_path_to_code = cat_path_to_code or _sf.get("cat_path_to_code", {})
            code_to_path = code_to_path or _sf.get("code_to_path", {})
        except:
            pass

    # check_kenya_book_category ran here for Kenya. It decided "is this a
    # book category?" with `"book" in CATEGORY`, and CATEGORY carries the
    # leaf — so a book correctly filed in "Books, Movies and Music /
    # Business & Finance / Business & Economics" arrived as "Business &
    # Economics", failed the test and was rejected. Its findings also went
    # straight into the result, bypassing the Books exemption applied to the
    # matcher output below, so nothing downstream could undo it.
    #
    # Books are judged by the same category matcher as everything else now,
    # and drop_kenya_books_false_positives protects the ones already filed
    # correctly — splitting the path properly and keeping DVDs in scope.

    if _CAT_MATCHER_AVAILABLE:
        try:
            _engine = _get_cat_matcher_engine()
            if _engine is not None:
                if categories_list and not _engine._tfidf_built:
                    _engine.build_tfidf_index(categories_list)
                base_flagged = check_wrong_category(
                    data,
                    categories_list=categories_list,
                    cat_path_to_code=cat_path_to_code,
                    code_to_path=code_to_path,
                )
                # Kenya: the matcher predicts arbitrary categories for book
                # titles and rejects books that are already filed correctly.
                # Everything under "Books, Movies and Music" is exempt except
                # the DVDs sub-tree.
                if str(country_code or "").upper() == "KE":
                    try:
                        from custom_country_rules import drop_kenya_books_false_positives
                        _before = len(base_flagged)
                        base_flagged = drop_kenya_books_false_positives(
                            base_flagged, code_to_path
                        )
                        _dropped = _before - len(base_flagged)
                        if _dropped:
                            logger.info(
                                "Kenya books exemption: dropped %s Wrong Category "
                                "false positive(s)", _dropped,
                            )
                    except Exception as _e:
                        logger.warning("Kenya books exemption failed: %s", _e)
                return base_flagged
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


# Restricted brands that are also ordinary English words.
#
# These are still matched in NAME and BRAND exactly as before — putting
# "Simple" in the brand field is a brand claim. They are NOT matched in
# DESCRIPTION or SHORT_DESCRIPTION, because there the same letters are usually
# just an adjective.
#
# Measured on a real 8,677-product KE batch: scanning descriptions for every
# restricted brand flagged 205 extra products, and 198 of them were the word
# "simple" in ordinary prose — "a slim and simple design", "makes everyday
# cooking simple", "the simple rules make it easy for children". Excluding
# these words takes the same scan to 7, which is the evasion this is for.
#
# Add a word here if a restricted brand starts producing prose false
# positives; remove one if a brand is genuinely being hidden in descriptions
# and the noise is worth it.
RESTRICTED_BRANDS_NOT_SCANNED_IN_PROSE = {
    "simple", "classic", "original", "premium", "fashion", "generic", "nature",
}


def check_restricted_brands(
    data: pd.DataFrame, country_rules: List[Dict]
) -> pd.DataFrame:
    if data.empty or not country_rules:
        return pd.DataFrame(columns=data.columns)

    d = data.copy()

    # Exclude categories that should never be flagged for restricted brands.
    # PS5 / PlayStation game titles legitimately contain brand-like keywords.
    _EXCLUDED_CATEGORY_FRAGMENTS = (
        "ps 5 games",
        "ps5 games",
        "playstation 5",
        "playstation5",
    )
    if "CATEGORY" in d.columns:
        _cat_lower = d["CATEGORY"].astype(str).str.lower()
        _cat_excl_mask = _cat_lower.apply(
            lambda c: any(f in c for f in _EXCLUDED_CATEGORY_FRAGMENTS)
        )
        d = d[~_cat_excl_mask].copy()
    if "CATEGORY_CODE" in d.columns and "code_to_path" in (data.attrs or {}):
        pass  # code_to_path lookup not available here; CATEGORY column is the source of truth

    if d.empty:
        return pd.DataFrame(columns=data.columns)

    if "_brand_norm" not in d.columns:
        d["_brand_norm"] = _normalize_series(d.get("BRAND", pd.Series("", index=d.index)))
    if "_name_norm" not in d.columns:
        d["_name_norm"] = _normalize_series(d.get("NAME", pd.Series("", index=d.index)))
    if "_seller_norm" not in d.columns:
        d["_seller_norm"] = _normalize_series(d.get("SELLER_NAME", pd.Series("", index=d.index)))

    if "_name_lower" not in d.columns:
        d["_name_lower"] = d.get("NAME", pd.Series("", index=d.index)).astype(str).str.lower()

    # Free-text surfaces, kept apart from each other so a finding can say which
    # one the brand was hidden in rather than just "somewhere in the copy".
    #
    # Lowercased only at this point. Tag stripping is deferred until after the
    # pre-filter below, because it is a regex rewrite of every description in
    # the batch — thousands of long HTML strings — to serve the handful of rows
    # that survive filtering. Doing it here roughly doubled the cost of the
    # whole check on an 8,677-product batch.
    d["_desc_lower"] = (
        d.get("DESCRIPTION", pd.Series("", index=d.index)).astype(str).str.lower()
    )
    d["_sdesc_lower"] = (
        d.get("SHORT_DESCRIPTION", pd.Series("", index=d.index)).astype(str).str.lower()
    )

    all_keywords = set()
    brand_names_only = set()
    brand_raw_lower_only = set()
    for rule in country_rules:
        all_keywords.add(rule["brand"])
        valid_vars = [v for v in rule.get("variations", []) if str(v).strip()]
        all_keywords.update(valid_vars)
        brand_names_only.add(rule["brand"])
        if rule.get("brand_raw"):
            brand_raw_lower_only.add(rule["brand_raw"].lower())

    _name_pattern = "(?i)" + "|".join(r"\b" + re.escape(k) + r"\b" for k in brand_names_only if k)
    _name_raw_pattern = "(?i)" + "|".join(r"\b" + re.escape(k) + r"\b" for k in brand_raw_lower_only if k)
    _name_norm_pattern = "(?i)" + "|".join(re.escape(k) for k in brand_names_only if k)
    _brand_substr_pattern = "(?i)" + "|".join(r"\b" + re.escape(k) + r"\b" for k in brand_names_only if k)
    
    # Prose surfaces use their own alternation: the ordinary-word brands are
    # dropped from it, so "simple" in a description costs nothing here.
    _prose_names = [
        k for k in brand_raw_lower_only
        if k and k not in RESTRICTED_BRANDS_NOT_SCANNED_IN_PROSE
    ]
    _prose_pattern = (
        "(?i)" + "|".join(r"\b" + re.escape(k) + r"\b" for k in _prose_names)
        if _prose_names else None
    )

    mask = (
        d["_brand_norm"].isin(all_keywords)
        | d["_brand_norm"].str.contains(_brand_substr_pattern, na=False)
        | d["_name_norm"].str.contains(_name_norm_pattern, na=False)
        | d["_name_lower"].str.contains(_name_raw_pattern, na=False)
    )
    if _prose_pattern:
        mask = (
            mask
            | d["_desc_lower"].str.contains(_prose_pattern, na=False)
            | d["_sdesc_lower"].str.contains(_prose_pattern, na=False)
        )

    d = d[mask].copy()

    if d.empty:
        return pd.DataFrame(columns=data.columns)

    # Now that the batch is down to candidates, strip the HTML. An attribute
    # value (class="sony-block", a CDN path containing the brand) is not a
    # brand claim to a shopper, so matching on raw markup would flag listings
    # that never show the name. The pre-filter above ran on raw text and is
    # therefore slightly over-inclusive, which is the right way round: it can
    # only add candidates for this stricter pass to reject.
    _tag_re = re.compile(r"<[^>]+>")
    for _c in ("_desc_lower", "_sdesc_lower"):
        d[_c] = d[_c].str.replace(_tag_re, " ", regex=True)

    flagged_indices = set()
    comment_map = {}
    where_map = {}
    match_details = {}
    for rule in country_rules:
        brand_name = rule["brand"]
        brand_raw = rule["brand_raw"]
        # Exact match: brand column exactly equals the normalised brand
        main_brand_matches_exact = d["_brand_norm"] == brand_name
        # Substring match: brand column CONTAINS the brand keyword as a distinct word
        main_brand_matches_substr = d["_brand_norm"].str.contains(
            r"\b" + re.escape(brand_name) + r"\b", regex=True, na=False
        )
        main_brand_matches = main_brand_matches_exact | main_brand_matches_substr
        # Match brand name in product NAME:
        # 1. Use _name_lower with \b (punctuation preserved, word boundaries work for most brands)
        # 2. Also check if _name_norm STARTS WITH the brand (catches La Roche-Posay -> larocheposay
        #    at start of name, where the normalized string has no trailing word boundary)
        main_name_lower_matches = d["_name_lower"].str.contains(
            r"\b" + re.escape(brand_raw.lower()) + r"\b", regex=True, na=False, flags=re.IGNORECASE
        )
        main_name_norm_starts = d["_name_norm"].str.startswith(brand_name, na=False)
        main_name_matches = main_name_lower_matches | main_name_norm_starts

        # Same brand, hidden in the copy instead of the labelled fields. Only
        # consulted for brands that are not ordinary words — see
        # RESTRICTED_BRANDS_NOT_SCANNED_IN_PROSE.
        _prose_ok = brand_raw.lower() not in RESTRICTED_BRANDS_NOT_SCANNED_IN_PROSE
        _word_re = r"\b" + re.escape(brand_raw.lower()) + r"\b"
        if _prose_ok:
            desc_matches = d["_desc_lower"].str.contains(
                _word_re, regex=True, na=False, flags=re.IGNORECASE
            )
            sdesc_matches = d["_sdesc_lower"].str.contains(
                _word_re, regex=True, na=False, flags=re.IGNORECASE
            )
        else:
            desc_matches = pd.Series(False, index=d.index)
            sdesc_matches = pd.Series(False, index=d.index)

        current_match_mask = (
            main_brand_matches | main_name_matches | desc_matches | sdesc_matches
        )
        # Most specific surface wins, so a product with the brand in both the
        # name and the description reads as a name match — the description is
        # only the interesting finding when the labelled fields look clean.
        for idx in d[main_brand_matches].index:
            match_details[idx] = ("main_brand", brand_raw, "BRAND")
        for idx in d[main_name_matches & ~main_brand_matches].index:
            match_details[idx] = ("main_name", brand_raw, "NAME")
        _labelled = main_brand_matches | main_name_matches
        for idx in d[desc_matches & ~_labelled].index:
            if idx not in match_details:
                match_details[idx] = ("description", brand_raw, "DESCRIPTION")
        for idx in d[sdesc_matches & ~_labelled & ~desc_matches].index:
            if idx not in match_details:
                match_details[idx] = ("short_description", brand_raw, "SHORT_DESCRIPTION")
        valid_vars = [v for v in rule.get("variations", []) if str(v).strip()]
        if valid_vars:
            sorted_vars = sorted(valid_vars, key=len, reverse=True)
            var_pattern = (
                r"(?:\b" + r"\b|\b".join([re.escape(v) for v in sorted_vars]) + r"\b)"
            )
            var_brand_matches = d["_brand_norm"].str.contains(
                var_pattern, regex=True, na=False
            )
            # Check _name_lower (word-boundary, preserves punctuation)
            # and _name_norm startswith each variation (normalized brand at start of product name)
            var_name_lower_matches = d["_name_lower"].str.contains(
                var_pattern, regex=True, na=False
            )
            # For _name_norm: match if name STARTS WITH any variation (no trailing \b needed)
            var_name_norm_starts = pd.Series(False, index=d.index)
            for var in sorted_vars:
                var_name_norm_starts = var_name_norm_starts | d["_name_norm"].str.startswith(var, na=False)
            var_name_matches = var_name_lower_matches | var_name_norm_starts
            for idx in d[var_brand_matches | var_name_matches].index:
                if idx not in match_details:
                    text_to_check = (
                        d.loc[idx, "_brand_norm"]
                        if var_brand_matches[idx]
                        else (d.loc[idx, "_name_lower"] + " " + d.loc[idx, "_name_norm"])
                    )
                    for var in sorted_vars:
                        if var in text_to_check:
                            match_details[idx] = (
                                "variation",
                                f"{brand_raw} (as '{var}')",
                                "BRAND" if var_brand_matches[idx] else "NAME",
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
            
        # Hardcoded rule: 'Simple' is a restricted beauty brand, limit it to Health & Beauty
        if brand_name.lower() == "simple":
            try:
                hb_codes = load_kebs_hb_codes()
                if hb_codes:
                    current_match = current_match[
                        current_match["_cat_clean"].isin(hb_codes)
                    ]
            except Exception:
                pass
                
        # Hardcoded rule: 'Sony' is allowed for video games/playstation
        if brand_name.lower() == "sony" and "CATEGORY" in current_match.columns:
            cat_str = current_match["CATEGORY"].astype(str).str.lower()
            exclude_mask = cat_str.str.contains("gaming|playstation|ps 5|ps5|ps 4|ps4|video game|console|psp|ps vita", regex=True, na=False)
            current_match = current_match[~exclude_mask]
        if current_match.empty:
            continue
        rejected = current_match[~current_match["_seller_norm"].isin(rule["sellers"])]
        if not rejected.empty:
            for idx in rejected.index:
                flagged_indices.add(idx)
                match_type, match_info, matched_in = match_details.get(
                    idx, ("unknown", brand_raw, "")
                )
                seller_status = (
                    "Seller not in approved list"
                    if rule["sellers"]
                    else "No sellers approved"
                )
                # Where it was found is part of the finding, not a footnote: a
                # brand in the DESCRIPTION with a clean NAME and BRAND is a
                # different thing from a brand in the BRAND field, and the
                # reviewer needs to see which one they are looking at.
                _where = f" in {matched_in}" if matched_in else ""
                comment_map[idx] = (
                    f"Restricted Brand: {match_info}{_where} - {seller_status}"
                )
                where_map[idx] = matched_in
    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)
    flagged_sids = {d.loc[idx, "PRODUCT_SET_SID"] for idx in flagged_indices}
    sid_comment = {d.loc[idx, "PRODUCT_SET_SID"]: comment_map[idx] for idx in flagged_indices}
    sid_where = {d.loc[idx, "PRODUCT_SET_SID"]: where_map.get(idx, "") for idx in flagged_indices}
    result = data[data["PRODUCT_SET_SID"].isin(flagged_sids)].copy()
    result["Comment_Detail"] = result["PRODUCT_SET_SID"].map(sid_comment)
    result["Matched In"] = result["PRODUCT_SET_SID"].map(sid_where)
    return result.drop_duplicates(subset=["PRODUCT_SET_SID"])


# The bare words below exist in the rules file to catch skin bleaching, and are
# scoped to ~1,928 categories — which includes 107 oral-care and deodorant
# categories. That made "Colgate Advanced Whitening Toothpaste" and
# "whitening roll-on deodorant" read as prohibited products.
#
# ONLY these three single words get the exemption. The explicit rules
# ("whitening cream", "skin whitening", "brightening serum", "whitening soap",
# "lightening lotion") name a skin product outright and stay prohibited
# everywhere, so a bleaching cream mis-filed under Oral Care is still caught.
_LIGHTENING_GENERIC_KWS = {"whitening", "brightening", "lightening"}

# Contexts where whitening/brightening is an ordinary product claim.
_LIGHTENING_OK_NAME_RE = re.compile(
    r"\b(?:toothpaste|tooth\s*paste|toothbrush|tooth\s*brush|mouth\s*wash|"
    r"mouth\s*rinse|oral|dental|denture|teeth|tooth|floss|gum\s*care|"
    r"deodorant|anti[\s\-]?perspirant|roll[\s\-]?on)\b",
    re.IGNORECASE,
)
_LIGHTENING_OK_CATEGORY_RE = re.compile(
    r"oral\s*care|oral\s*hygiene|dental|denture|toothbrush|toothpaste|"
    r"mouthwash|deodorant|antiperspirant",
    re.IGNORECASE,
)


def check_prohibited_products(
    data: pd.DataFrame, prohibited_rules: List[Dict], code_to_path: dict = None
) -> pd.DataFrame:
    if not {"NAME", "CATEGORY_CODE"}.issubset(data.columns) or not prohibited_rules:
        return pd.DataFrame(columns=data.columns)

    # Drop rules whose category list is a placeholder rather than real codes.
    #
    # A blank category cell in Prohibbited.xlsx parses to {'None'} — a non-empty
    # set holding the string "None" — instead of an empty set. The lookup below
    # then asks "is this product's category in {'None'}?", which is never true,
    # so the rule matches nothing. Worse, its keyword still goes into the
    # combined regex, and because the alternation is sorted longest-first a dead
    # "whitening soap" SHADOWS the working "whitening": findall returns the
    # longer match, that match is discarded on the category test, and the
    # product escapes entirely. Skin-whitening soaps and serums were passing for
    # exactly this reason.
    #
    # Dropping them restores the broad rules. It deliberately does NOT treat a
    # blank category as "applies everywhere": keywords like "105" and "100inch"
    # in the NG sheet are plainly meant to be category-scoped, and firing them
    # globally would flag every TV with 105 in its name.
    def _is_placeholder(cats) -> bool:
        return bool(cats) and all(
            str(c).strip().lower() in ("none", "nan", "") for c in cats
        )

    # A blank category disables a rule, EXCEPT for the curated list below.
    #
    # Blank categories left the check nearly dead — KE ran 8 of 316 rules,
    # NG 0 of 1831 — because keywords like "vape" and "shisha" are banned
    # whatever they are filed under, so nobody was ever going to fill in a
    # category for them.
    #
    # Inferring "blank means everywhere" was tried and is wrong. Measured
    # against 7,181 real product names, globalising NG's blank rules flagged
    # 4.6% of the catalogue, 89% of it from one keyword: "military", which is
    # meant as military equipment and matches "Military-Grade Case" on every
    # phone cover. "lighter" (meaning cigarette lighter) matched "Lighter
    # Warm Fleece Lining". Those rules genuinely need a category.
    #
    # So the global set is explicit rather than inferred. Every entry was
    # taken from the sheets and checked against that same corpus: the 49
    # below with no corpus match at all, plus 6 whose only matches were
    # genuine prohibited items (adult products and two stun guns).
    #
    # To add a term: confirm it cannot appear innocently in a product name,
    # in any market. If it can, give it a category in Prohibbited.xlsx
    # instead — that is what the category column is for.
    _GLOBAL_PROHIBITED = frozenset({
        # Vaping and smoking paraphernalia
        "vape", "vapes", "vaping", "vape pen", "vape pens", "vape juice",
        "vape liquid", "vape cartridge", "vape cart", "vape kit", "vape mod",
        "vape pod", "vape tank", "vape starter kit", "disposable vape",
        "refillable vape", "herbal vape", "cloud vape",
        "e cigarette", "e-cigarette", "e-cigarettes", "ecigarette",
        "e-juice", "e-hookah",
        "shisha", "shishaa", "shisha pipe", "shisha pen", "shisha flavor",
        "shisha flavour", "hookah",
        # Controlled substances
        "cannabis", "cannabis oil", "cocaine", "heroin", "marijuana", "lsd",
        # Weapons
        "taser", "tasers", "stun gun", "stun gunn", "pepper spray",
        # Adult products
        "sex toy", "sex toys", "wand sex toys", "anal sex toys",
        "fetish sex toy r4", "dildo", "rabbit dildo vibrator g-spot",
        "vibrating rotating dildo", "vibrator g spot dildo", "g-spot",
        "butt plug",
        # Misrepresentation
        "counterfeit",
    })

    scoped_rules, global_rules, disabled = [], [], []
    for r in prohibited_rules:
        if not _is_placeholder(r.get("categories")):
            scoped_rules.append(r)
        elif str(r.get("keyword", "")).strip().lower() in _GLOBAL_PROHIBITED:
            # Empty category set = matches on keyword alone, any category.
            global_rules.append({**r, "categories": set()})
        else:
            disabled.append(str(r.get("keyword", "")).strip())

    if disabled:
        logger.info(
            "[Prohibited] %d rule(s) inactive: no category, and the keyword is "
            "not on the always-prohibited list (e.g. %s). Add category codes "
            "in Prohibbited.xlsx to enable them.",
            len(disabled), ", ".join(sorted({d for d in disabled if d})[:5]),
        )
    logger.info(
        "[Prohibited] %d category-scoped rule(s), %d always-prohibited rule(s) active.",
        len(scoped_rules), len(global_rules),
    )

    prohibited_rules = scoped_rules + global_rules
    if not prohibited_rules:
        return pd.DataFrame(columns=data.columns)

    all_kws = sorted(
        set(rule["keyword"] for rule in prohibited_rules), key=len, reverse=True
    )
    combined_pattern = re.compile(
        r"(?<!\w)(?:" + "|".join(re.escape(k) for k in all_kws) + r")(?!\w)",
        re.IGNORECASE,
    )
    match_mask = data["_name_lower"].str.contains(combined_pattern, na=False)
    if not match_mask.any():
        return pd.DataFrame(columns=data.columns)
    candidates = data[match_mask]

    # The per-row test below reads an empty set as "any category", so a
    # keyword that is global in one rule must stay global even if another
    # rule scopes it — otherwise the union would silently narrow it back to
    # that one category and undo the fix above.
    kw_to_cats = {}
    _global_kws = set()
    for rule in prohibited_rules:
        kw = rule["keyword"]
        cats = rule.get("categories") or set()
        if not cats:
            _global_kws.add(kw)
        kw_to_cats.setdefault(kw, set()).update(cats)
    for kw in _global_kws:
        kw_to_cats[kw] = set()

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
        cat_path = (code_to_path or {}).get(cat_clean, "")
        # Resolved once per row, not per matched keyword.
        _is_oral_or_deo = bool(
            _LIGHTENING_OK_NAME_RE.search(name_lower)
            or (cat_path and _LIGHTENING_OK_CATEGORY_RE.search(cat_path))
        )

        matched_kws = []
        for m in set(matches):
            m_lower = m.lower()
            cats = kw_to_cats.get(m_lower, set())
            if cats and cat_clean not in cats:
                continue
            # A generic "whitening"/"brightening"/"lightening" on a toothpaste,
            # mouthwash or deodorant is a normal product claim, not skin bleaching.
            if m_lower in _LIGHTENING_GENERIC_KWS and _is_oral_or_deo:
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
    data: pd.DataFrame, suspected_fake_df: pd.DataFrame,
    sneaker_category_codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Brand claimed + price under the ceiling for that brand and category.

    The ceiling used to be looked up on the declared BRAND field alone, which
    is the one place a counterfeit listing is careful not to put the brand:

        BRAND "Generic"  NAME "Nike Air Max 90 ..."   -> no ceiling applied
        BRAND "Airmax"   NAME "Airmax tn ultra"       -> no ceiling applied
                                                         ("Airmax" is not a
                                                          column in the sheet)

    A brand is now claimed if it appears in the BRAND field OR as a whole word
    in NAME — and, inside the sneaker categories, in DESCRIPTION or
    SHORT_DESCRIPTION too. The description is scoped to sneakers deliberately:
    across the whole catalogue "compatible with Sony" in an accessory's copy is
    ordinary, but a sneaker's description naming Nike is a claim about the shoe.

    Price ceilings and their categories come from suspected_fake.xlsx as
    before — row 0 is the ceiling, the rows beneath it are the categories it
    applies to, one column per brand.
    """
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
        # Which brands does this listing claim, and where?
        #
        # One alternation over the sheet's brand names (about a dozen), run
        # once per surface rather than per row. Word-bounded, so "Bose" does
        # not match inside "boses" and "AKG" does not match inside a SKU.
        _sheet_brands = sorted(
            {b for (b, _c) in brand_cat_price}, key=len, reverse=True
        )
        # A sub-brand is a claim on its parent's ceiling. "Airmax 97" and
        # "Air Jordan 4" are Nike, so they answer to Nike's $45 — without this
        # they answered to nothing, because neither is a column in the sheet.
        # Only aliases whose parent actually has a ceiling are worth matching.
        _alias_terms = {}
        for _alias, _parent in SNEAKER_BRAND_ALIASES.items():
            if _parent in _sheet_brands and _alias not in _sheet_brands:
                _alias_terms[_alias] = _parent
        _brand_res = {
            b: re.compile(r"(?<!\w)" + re.escape(b) + r"(?!\w)", re.IGNORECASE)
            for b in _sheet_brands
        }
        _alias_res = {
            a: re.compile(r"(?<!\w)" + re.escape(a) + r"(?!\w)", re.IGNORECASE)
            for a in _alias_terms
        }
        _tag_re = re.compile(r"<[^>]+>")
        _name = d.get("NAME", pd.Series("", index=d.index)).astype(str).str.lower()

        _sneaker_cats = {
            clean_category_code(c) for c in (sneaker_category_codes or [])
        }
        _in_sneakers = d["_cat_clean"].isin(_sneaker_cats) if _sneaker_cats else pd.Series(False, index=d.index)
        if _in_sneakers.any():
            _blob = (
                d.get("DESCRIPTION", pd.Series("", index=d.index)).astype(str)
                + " "
                + d.get("SHORT_DESCRIPTION", pd.Series("", index=d.index)).astype(str)
            ).where(_in_sneakers, "").str.replace(_tag_re, " ", regex=True).str.lower()
        else:
            _blob = pd.Series("", index=d.index)

        _claims = {}   # brand -> boolean Series
        for b, rx in _brand_res.items():
            _claims[b] = (
                (d["_brand_lower"] == b)
                | _name.str.contains(rx, na=False)
                | _blob.str.contains(rx, na=False)
            )
        for a, parent in _alias_terms.items():
            rx = _alias_res[a]
            _claims[parent] = _claims.get(parent, pd.Series(False, index=d.index)) | (
                (d["_brand_lower"] == a)
                | _name.str.contains(rx, na=False)
                | _blob.str.contains(rx, na=False)
            )

        prices = d["price_to_use"].values
        cats = d["_cat_clean"].values
        _flag = pd.Series(False, index=d.index)
        _detail = pd.Series("", index=d.index)
        for b, claimed in _claims.items():
            if not claimed.any():
                continue
            _ceil = pd.Series(
                [brand_cat_price.get((b, c), -1) for c in cats], index=d.index
            )
            _under = claimed & (d["price_to_use"] < _ceil) & (_ceil > 0)
            if not _under.any():
                continue
            # Where the claim came from, so a reviewer can see whether the
            # seller declared the brand or buried it in the copy.
            _where = pd.Series("BRAND", index=d.index)
            _where = _where.mask(d["_brand_lower"] != b, "NAME")
            _where = _where.mask(
                (d["_brand_lower"] != b) & ~_name.str.contains(_brand_res[b], na=False),
                "DESCRIPTION",
            )
            _new = _under & ~_flag
            _detail.loc[_new] = (
                f"{b.title()} claimed in " + _where.loc[_new]
                + " but priced under the "
                + _ceil.loc[_new].astype(int).astype(str) + " ceiling"
            )
            _flag = _flag | _under

        d["is_fake"] = _flag
        out = d[d["is_fake"] == True].copy()
        if out.empty:
            return pd.DataFrame(columns=data.columns)
        out["Comment_Detail"] = _detail.loc[out.index]
        _cols = list(data.columns) + ["Comment_Detail"]
        return out[_cols].drop_duplicates(subset=["PRODUCT_SET_SID"])
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


def check_fda(data: pd.DataFrame, country_code: str) -> pd.DataFrame:
    try:
        from targeted_audit_filters import load_qc_excel
        rules = load_qc_excel(country_code)
    except:
        rules = {}
    if not rules:
        return pd.DataFrame(columns=data.columns)

    d = data.copy()
    if "FDA" not in d.columns:
        d["FDA"] = ""
    d["FDA"] = d["FDA"].astype(str).str.strip().fillna("")

    mandatory_cats = {
        cat for cat, rule in rules.items()
        if str(rule.get("FDA Documents", "")).strip().lower() == "mandatory"
    }

    if not mandatory_cats:
        return pd.DataFrame(columns=d.columns)

    flagged = d[
        d["_cat_clean"].isin(mandatory_cats) |
        d["CATEGORY_CODE"].astype(str).str.strip().isin(mandatory_cats)
    ].copy()
    if flagged.empty:
        return pd.DataFrame(columns=d.columns)

    is_missing = flagged["FDA"].isin(["", "nan", "none", "nat", "n/a"]) | flagged["FDA"].isna()
    flagged = flagged[is_missing].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = "Mandatory FDA registration number is missing."
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
        r"\b(?:tester|testeur)s?\b|\btester(?=[\d\-_])", re.IGNORECASE
    )
    flagged = perfume[
        perfume["_name_lower"].str.contains(tester_pattern, na=False)
    ].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = (
            "Perfume tester listed for sale: " + flagged["NAME"].astype(str).str[:60]
        )
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


# SNEAKER_BRAND_ALIASES lives in constants.py — imported at the top of this
# file. It moved there because the review grid needs it too, and having
# ui_components import this module re-executes the entire entry script:
# Streamlit re-runs every widget in it, which raised duplicate element IDs
# and fragment errors on every grid render.

# Deliberately empty, and not a placeholder to fill in.
#
# A blanket "no seller may list this brand" rule was tried and removed. It
# reached 213 of 220 known counterfeits, but sellers ARE approved for these
# brands — the problem is the fake ones, not the brand — so on real data it
# rejected honest listings: a Timberland mention in a slip-on loafer's copy,
# a Converse mention in an unrelated fashion sneaker. Rejecting a whole brand
# is only correct when the brand genuinely has no approved seller, which is
# not the case here.
#
# Add a brand here ONLY when that is actually true for it. Listings that no
# rule can separate from the real thing belong in the visual review grid,
# where a human decides from the photograph.
SNEAKER_BRANDS_NO_APPROVED_SELLER: set = set()


def check_counterfeit_sneakers(
    data: pd.DataFrame,
    sneaker_category_codes: List[str],
    sneaker_sensitive_brands: List[str],
    known_brands: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Sneakers invoking a protected brand the listing does not declare.

    The gate used to be BRAND in ("generic", "fashion"). Measured against 220
    real counterfeit-suspect listings, that caught zero — including zero of
    the 33 the source data rated most likely counterfeit. Nobody leaves the
    field blank any more; they fill it with something derived from the brand
    they are invoking:

        NAME "Airmax tn ultra black"  BRAND "Airmax"      (not Nike)
        NAME "Conver all star"        BRAND "Conver"      (not Converse)
        NAME "j4 black red"           BRAND "Jordana"     (not Jordan)
        NAME "Fashion 254 Nike ..."   BRAND "Fashion 254" (a seller name)

    So the test is now plausibility rather than emptiness: the listing
    invokes a protected sneaker brand, and the BRAND field is not a
    recognised brand at all. A genuine "Nike Air Max 90 / BRAND Nike" passes,
    because Nike is recognised — an unauthorised seller of real Nike is the
    restricted-brand check's job, not this one.

    known_brands is brands.txt. It is optional so existing callers that pass
    only the two lists keep working, but without it this falls back to the
    old generic/fashion gate and catches almost nothing.
    """
    if not {"CATEGORY_CODE", "NAME", "BRAND"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    sneakers = data[
        data["_cat_clean"].isin(
            set(clean_category_code(c) for c in sneaker_category_codes)
        )
    ].copy()
    if sneakers.empty:
        return pd.DataFrame(columns=data.columns)

    # Whole-word matching. This was `any(b in x for b in brands)` — a bare
    # substring test against a 369-entry list containing 56 entries of four
    # characters or fewer ("lv", "tn", "j1", "cd", "af1"). Those matched
    # inside ordinary words and rejected honest listings:
    #
    #   "SILVER Vic Shoes ..."         -> "lv" inside si-LV-er
    #   "SILVER Victorious Heels ..."  -> "lv" again, plus "victori"
    #                                     inside "victorious"
    #
    # Neither is a counterfeit sneaker; both are rhinestone heels. The
    # lookaround form is the same one check_counterfeit_jerseys already uses,
    # and it tolerates the entries containing spaces or punctuation
    # ("af 1", "l v", "d!or", "n!ke") that \b would handle badly.
    #
    # One compiled alternation also replaces 369 substring scans per row.
    _sensitive = [b for b in (sneaker_sensitive_brands or []) if str(b).strip()]
    if not _sensitive:
        return pd.DataFrame(columns=data.columns)
    _sens_re = re.compile(
        r"(?<!\w)(?:"
        + "|".join(re.escape(b) for b in sorted(_sensitive, key=len, reverse=True))
        + r")(?!\w)",
        re.IGNORECASE,
    )
    # The same brand name moved out of the title and into the copy is the same
    # claim to a shopper, so the description and short description are scanned
    # too. Tags are stripped first — an attribute value is not a claim.
    #
    # No ordinary-word exclusion is needed here, unlike the restricted-brand
    # check: this list is counterfeit sneaker brands, and the surrounding
    # BRAND == Generic/Fashion condition already keeps it narrow.
    _tag_re = re.compile(r"<[^>]+>")
    _desc = (
        sneakers.get("DESCRIPTION", pd.Series("", index=sneakers.index))
        .astype(str).str.replace(_tag_re, " ", regex=True).str.lower()
    )
    _sdesc = (
        sneakers.get("SHORT_DESCRIPTION", pd.Series("", index=sneakers.index))
        .astype(str).str.replace(_tag_re, " ", regex=True).str.lower()
    )
    _in_name = sneakers["_name_lower"].str.contains(_sens_re, na=False)
    _in_desc = _desc.str.contains(_sens_re, na=False)
    _in_sdesc = _sdesc.str.contains(_sens_re, na=False)

    # Is the declared brand a real brand at all?
    #
    # Normalised the same way both sides, so "Air-Max" and "air max" compare
    # equal. The pseudo-brands are listed explicitly because they are real
    # entries a seller can pick, not absences — "Fashion" is a selectable
    # value, and it is never a claim about who made the shoe.
    _PSEUDO = {"generic", "fashion", "unbranded", "no brand", "none", "n/a", "other", ""}

    def _norm_brand(s):
        return re.sub(r"[^a-z0-9 ]+", "", str(s).lower()).strip()

    _known = {
        _norm_brand(b) for b in (known_brands or []) if str(b).strip()
    } - _PSEUDO
    _declared = sneakers["_brand_lower"].map(_norm_brand)
    if _known:
        _brand_recognised = _declared.isin(_known) & ~_declared.isin(_PSEUDO)
    else:
        # No brand list to judge against. Fall back to the old, narrow gate
        # rather than treating every brand as unrecognised — an empty list
        # would otherwise flag every sneaker that mentions a protected name,
        # which is the opposite of a safe default for a missing input.
        logger.warning(
            "counterfeit sneakers: no known-brand list supplied, falling back "
            "to the generic/fashion gate"
        )
        _brand_recognised = ~_declared.isin(_PSEUDO)

    # Brands nobody may sell yet. The claim is what matters, so this looks at
    # every surface and resolves aliases first: "Airmax 97" and "Air Jordan 4"
    # are Nike claims, and Nike has no approved seller.
    _blocked_terms = {}
    for _t, _parent in SNEAKER_BRAND_ALIASES.items():
        if _parent in SNEAKER_BRANDS_NO_APPROVED_SELLER:
            _blocked_terms[_t] = _parent
    for _b in SNEAKER_BRANDS_NO_APPROVED_SELLER:
        _blocked_terms[_b] = _b

    _claim_text = (
        sneakers["_name_lower"].astype(str) + " "
        + sneakers["BRAND"].astype(str).str.lower() + " " + _desc + " " + _sdesc
    )
    _blocked_hit = pd.Series(False, index=sneakers.index)
    _blocked_who = pd.Series("", index=sneakers.index)
    for _t in sorted(_blocked_terms, key=len, reverse=True):
        _rx = re.compile(r"(?<!\w)" + re.escape(_t) + r"(?!\w)", re.IGNORECASE)
        _m = _claim_text.str.contains(_rx, na=False) & ~_blocked_hit
        if _m.any():
            _blocked_who.loc[_m] = _blocked_terms[_t]
            _blocked_hit = _blocked_hit | _m

    hit = sneakers[
        (~_brand_recognised & (_in_name | _in_desc | _in_sdesc))
        | _blocked_hit
    ].copy()
    if hit.empty:
        return pd.DataFrame(columns=data.columns)

    # Most specific surface first, same rule as the restricted-brand check:
    # the description is only the interesting finding when the title is clean.
    hit["Matched In"] = "SHORT_DESCRIPTION"
    hit.loc[_in_desc.loc[hit.index], "Matched In"] = "DESCRIPTION"
    hit.loc[_in_name.loc[hit.index], "Matched In"] = "NAME"

    # Why this one was flagged, in the reviewer's words rather than a rule id.
    # "Fashion" and friends read differently from an invented brand, and the
    # two want different follow-up, so they are named separately.
    _d = hit["_brand_lower"].map(_norm_brand)
    _b = hit["BRAND"].astype(str)
    hit["Brand Claim"] = "Brand '" + _b + "' is not a recognised brand"
    hit.loc[_d.isin(_PSEUDO), "Brand Claim"] = "No brand declared (" + _b + ")"
    # The no-approved-seller reason wins where both apply: it is the decisive
    # one, and it is the one a seller can act on.
    _bw = _blocked_who.loc[hit.index]
    hit.loc[_bw.ne(""), "Brand Claim"] = (
        "Claims " + _bw[_bw.ne("")].str.title() + " — no seller is approved for this brand"
    )
    return hit.drop_duplicates(subset=["PRODUCT_SET_SID"])


def check_counterfeit_jerseys(
    data: pd.DataFrame, jerseys_data: Dict, country_code: str
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME", "SELLER_NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    categories = jerseys_data.get("categories", set())
    keywords = jerseys_data.get("keywords", {}).get(country_code, set())
    exempted = jerseys_data.get("exempted", {}).get(country_code, set())
    
    # Fallback: if this country has no specific keywords defined in the Excel sheet
    # (e.g., Egypt might be missing a tab), union all keywords globally to ensure
    # the counterfeit check still runs using known global brand keywords.
    if not keywords:
        global_kws = set()
        for kw_set in jerseys_data.get("keywords", {}).values():
            global_kws.update(kw_set)
        keywords = global_kws

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

    # ── Filter out ambiguous short model terms ────────────────────────────────
    # Single-word model names of ≤4 characters (e.g. "Body", "One", "Men",
    # "Red", "Love") are common English words that fire on innocent products
    # like "Splash Perfume Mist 236ml … Fragrance Body Mist".
    # Keep a model term only if it is:
    #   • multi-word (contains a space), OR
    #   • a single word with ≥ 5 characters that is NOT a color name — models
    #     like "Green" (Coach) or "Amber" are also everyday color variants
    #     ("… Mist - Green 1PC"), so a single color word is not evidence of a
    #     fake. Multi-word models like "Green Tea" remain distinctive and kept.
    # Brand-level terms (legit_brand_terms) are always kept — they are proper
    # nouns specific enough to be reliable signals (Chanel, Dior, Givenchy…).
    color_words = {str(c).strip().lower() for c in kwargs.get("color_words") or []}
    safe_model_terms = {
        t for t in model_terms
        if " " in t or (len(t) >= 5 and t not in color_words)
    }

    all_terms = legit_brand_terms | safe_model_terms
    if not all_terms:
        return pd.DataFrame(columns=data.columns)

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

    # Every matching row is returned (no per-SID dedup): "Suspected Fake
    # Perfume" is a ROW_LEVEL_VALIDATOR, so the runner keeps precisely these
    # rows instead of re-expanding one representative row to its whole SID.
    return flagged.drop(columns=["_pfume_match"])


def check_brand_image_mismatch(
    data: pd.DataFrame, country_rules: list = None, **kwargs
) -> pd.DataFrame:
    """
    Compare the brand the image AI detected on the product photo
    (Brand_Detected_On_Product, merged in from the ZIP QC file) with the
    declared BRAND.

    Two tiers, distinguished in Comment_Detail:
      • detected brand is a restricted brand and the seller is not approved
        for it → strong counterfeit signal
      • plain mismatch (image shows one brand, listing declares another)
    A row passes when either brand contains the other as a whole word
    ("samsung galaxy" vs "Samsung" is fine).
    """
    if "Brand_Detected_On_Product" not in data.columns or "BRAND" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    # fillna before astype. astype(str) no longer renders missing values as
    # "nan" under pandas' new string dtype, so a blank BRAND stayed NaN and
    # reached re.search() below as a float — "need a bytes-like object, float
    # found", which killed the check for the whole batch. NaN is truthy, so
    # the `if not decl` guard in _brands_agree did not catch it either.
    det = data["Brand_Detected_On_Product"].fillna("").astype(str).str.strip()
    valid = det.ne("") & ~det.str.lower().isin(("nan", "none", "no brand", "unknown", "n/a"))
    d = data[valid].copy()
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    d["_det_l"] = det[valid].str.lower()
    d["_decl_l"] = d["BRAND"].fillna("").astype(str).str.strip().str.lower()

    def _brands_agree(decl: str, detected: str) -> bool:
        if not detected:
            return True
        if decl == detected:
            return True
        if not decl or decl in ("nan", "none"):
            return False
        return bool(
            re.search(r"\b" + re.escape(detected) + r"\b", decl)
            or re.search(r"\b" + re.escape(decl) + r"\b", detected)
        )

    # Only rows with a detected brand reach this Python loop — a small subset.
    mismatch_mask = [not _brands_agree(a, b) for a, b in zip(d["_decl_l"], d["_det_l"])]
    flagged = d[pd.Series(mismatch_mask, index=d.index)].copy()
    if flagged.empty:
        return pd.DataFrame(columns=data.columns)

    # brand (normalized) -> approved sellers (normalized), from Restricted_Brands.xlsx
    restricted_map = {r["brand"]: r.get("sellers", set()) for r in (country_rules or [])}

    def _build_comment(row):
        det_raw = str(row["Brand_Detected_On_Product"]).strip()
        decl_raw = str(row["BRAND"]).strip()
        det_norm = normalize_text(det_raw)
        seller_norm = normalize_text(row.get("SELLER_NAME", ""))
        if det_norm in restricted_map and seller_norm not in restricted_map[det_norm]:
            return (f"Restricted brand '{det_raw}' detected on product image but listing "
                    f"brand is '{decl_raw}' — seller not approved for {det_raw}")
        return f"Image shows '{det_raw}' but listing brand is '{decl_raw}'"

    flagged["Comment_Detail"] = flagged.apply(_build_comment, axis=1)
    return flagged.drop(columns=["_det_l", "_decl_l"]).drop_duplicates(subset=["PRODUCT_SET_SID"])


# ── Off-platform contact detection ──────────────────────────────────────────
_HTML_TAG_RE = re.compile(r"<[^>]+>")
# Hard evidence: phone numbers (KE/NG formats + generic international), URLs,
# and unambiguous wa.me links. Bare "whatsapp" is handled separately below —
# it's ambiguous on its own (a smartwatch/earbuds listing legitimately says
# "WhatsApp notification support"), so it doesn't belong in this always-hard
# bucket.
# Hard evidence is split by KIND — phone / website / email / location — so a
# flag can only ever be raised on a concrete contact detail, and the comment can
# name which kind was found. Wording alone ("contact us") is no longer enough:
# it produced flags on listings that contained no way to contact anyone.

# Phone numbers. Country prefixes cover all eight markets the app supports;
# previously only Kenya and Nigeria were matched, so an off-platform number in
# Uganda, Ghana, Morocco, Egypt, Senegal or Ivory Coast went straight through.
# Bare digit runs are deliberately NOT matched — model numbers, EANs and
# capacities would swamp the check with false positives.
# Arabic-Indic and Eastern Arabic-Indic digits fold to ASCII before matching.
# \d already matches ٥ because Python patterns are Unicode-aware, but every
# literal in the patterns below is ASCII — the leading "0" in the local
# formats, and prefixes like 212 or 20 — so "٠٦ ١٢ ٣٤ ٥٦ ٧٨" failed while the
# same number in Latin digits matched. Folding once is far less error-prone
# than writing a second set of patterns.
_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def _fold_digits(text: str) -> str:
    return str(text).translate(_ARABIC_DIGITS)


_PHONE_RE = re.compile(
    r"(?:"
    # International for the eight markets. Digit GROUPING is deliberately not
    # pinned down — the same Senegalese number gets written "+221 77 123 4567"
    # and "+221 77 123 45 67", and fixed 3-3-3 grouping missed both.
    r"\+?(?:254|256|234|233|212|221|225)[\s\-]?(?:\d[\s\-]?){7,11}\d"
    # Egypt's country code is only two digits, so require the leading + to stop
    # ordinary numbers in text ("20 000 mAh") from matching.
    r"|\+20[\s\-]?(?:\d[\s\-]?){8,10}\d"
    r"|\+\d{1,3}[\s\-]?(?:\d[\s\-]?){7,13}\d"                  # any other international
    # Local formats, also grouping-tolerant: "0712 345 678", "0712345678" and
    # "0712-345-678" are the same number written three ways.
    r"|\b0[17](?:[\s\-]?\d){8}\b"                              # 10-digit 07../01.. (KE, UG)
    r"|\b0[789][01](?:[\s\-]?\d){8}\b"                         # 11-digit 080x.... (NG)
    r"|\b0[1-9](?:[\s\-]\d{2}){4}\b"                           # 0X XX XX XX XX (MA, CI, SN)
    r")",
    re.IGNORECASE,
)

# Websites. Explicit schemes and www. plus a conservative bare-domain form —
# restricted to a known TLD list so strings like "3.5mm" or "image.png" cannot
# match.
_WEBSITE_RE = re.compile(
    r"(?:"
    r"https?://\S+"
    r"|\bwww\.\S+"
    r"|wa\.me/\S+"
    r"|\b[a-z0-9][a-z0-9\-]{1,30}\.(?:com|net|org|shop|store|biz|info|"
    r"co\.ke|co\.ug|com\.ng|com\.gh|co\.za|ma|eg|sn|ci)\b"
    r")",
    re.IGNORECASE,
)

# Email addresses — previously not detected at all, despite being one of the
# most direct ways to take a buyer off-platform.
_EMAIL_RE = re.compile(
    r"\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b"
    r"|\b[a-z0-9._%+\-]+\s*(?:\(at\)|\[at\]|\sat\s)\s*[a-z0-9.\-]+\s*"
    r"(?:\(dot\)|\[dot\]|\sdot\s)\s*[a-z]{2,}\b",                # obfuscated
    re.IGNORECASE,
)

# Physical locations. Kept to explicit address markers — a generic
# "<word> road" rule would flag product names like "Silk Road" or "Abbey Road".
#
# The landmarks a directions phrase points at. Shared by "opposite" and "next
# to" so the two cannot drift apart again — the second was anchored to a list
# like this and the first was not, which is how "opposite direction" came to
# read as a shop address.
_LANDMARK_ALT = (
    r"(?:building|mall|plaza|arcade|market|stage|station|petrol|filling|"
    r"supermarket|hospital|clinic|pharmacy|church|mosque|school|college|"
    r"university|hotel|lodge|bank|atm|roundabout|junction|stadium|towers?|"
    r"centre|center|complex|shop|store|showroom|godown|depot)\b"
)

_LOCATION_RE = re.compile(
    r"(?:"
    r"\bp\.?\s*o\.?\s*box\s*\d+"                                # P.O. Box 123
    r"|\b(?:shop|stall|suite|kiosk|office)\s*(?:no\.?|number|#)?\s*\d+"
    r"|\b\d+\s*(?:st|nd|rd|th)?\s*floor\b"
    r"|\balong\s+[a-z]+\s+(?:road|rd|street|st|avenue|ave)\b"
    # "opposite" and "next to" both need a landmark after them. Without one,
    # "opposite <any word>" flagged "opposite direction", "opposite ends",
    # "opposite side" and "opposite pattern" — ordinary product wording, and a
    # fan or a reversible jacket got reported for off-platform contact. The
    # "next to" rule beside it was always anchored this way; this one was not.
    r"|\bopposite\s+(?:the\s+)?(?:[a-z]+\s+){0,2}" + _LANDMARK_ALT +
    r"|\bnext\s+to\s+(?:the\s+)?(?:[a-z]+\s+){0,2}" + _LANDMARK_ALT +
    r"|\b(?:visit|come\s+to|located\s+at|find\s+us\s+at)\s+(?:our\s+)?"
    r"(?:shop|store|office|showroom)\b"
    # French — "BP 1234", "boîte postale", "magasin n° 12", "2ème étage",
    # "en face de", "à côté du marché", "situé à"
    r"|\bb\.?\s*p\.?\s*\d+"
    r"|\bbo[iî]te\s+postale\s*\d+"
    r"|\b(?:magasin|boutique|local|bureau)\s*(?:n[o°]\.?|num[ée]ro|#)?\s*\d+"
    r"|\b\d+\s*(?:er|[eè]me)?\s*[ée]tage\b"
    r"|\ben\s+face\s+d[eu]\b|\b[aà]\s+c[oô]t[ée]\s+d[eu]\b"
    r"|\bsitu[ée]\s+[aà]\b|\bvenez\s+(?:nous\s+voir|[aà])\b"
    # Arabic — "ص.ب ١٢٣" (P.O. Box), "محل رقم", "الطابق", "بجانب", "أمام"
    r"|ص\.?\s*ب\.?\s*\d+"
    r"|(?:محل|متجر|مكتب)\s*(?:رقم)?\s*\d+"
    r"|الطابق\s*\S+|بجانب\s+\S+|أمام\s+\S+|بالقرب\s+من"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# Ordered so the comment names the most actionable kind first.
_CONTACT_KINDS = (
    ("phone number", _PHONE_RE),
    ("website", _WEBSITE_RE),
    ("email", _EMAIL_RE),
    ("location", _LOCATION_RE),
)
# Soft signals: divert-the-buyer wording without a number/link. Deliberately
# phrase-based ("follow us on", not bare "tiktok") so products like ring
# lights "for TikTok videos" don't false-positive.
#
# French and Arabic carry the same intent as the English phrases. \b is not
# used around the Arabic alternatives: Arabic script has no ASCII word
# boundary, so \b next to an Arabic letter never matches.
_OFFPLATFORM_SOFT_RE = re.compile(
    r"(?:\bcall\s+us\b|\bcontact\s+(?:us|the\s+seller|seller)\b"
    r"|\border\s+(?:directly|via|through)\b|\bdm\s+us\b|\binbox\s+us\b"
    r"|\bfollow\s+us\s+on\b|\bfind\s+us\s+on\b|\bvisit\s+our\b"
    # French — "appelez-nous", "contactez le vendeur", "commandez directement",
    # "écrivez-nous", "suivez-nous sur", "visitez notre boutique"
    r"|\bappelez[\s\-]?(?:nous|moi)\b|\bcontactez[\s\-]?(?:nous|moi|le\s+vendeur)\b"
    r"|\bcommandez\s+(?:directement|via|par)\b|\b[ée]crivez[\s\-]?nous\b"
    r"|\bsuivez[\s\-]?nous\s+sur\b|\bvisitez\s+(?:notre|nos)\b"
    r"|\bnous\s+joindre\b|\bjoignez[\s\-]?nous\b"
    # Arabic — "اتصل بنا", "تواصل معنا", "اطلب مباشرة", "تابعنا على",
    # "راسلنا", "زوروا متجرنا"
    r"|اتصل\s*بنا|تواصل\s*مع(?:نا)?|اطلب\s*مباشرة|تابعنا\s*على"
    r"|راسلنا|زوروا?\s*(?:متجرنا|محلنا)|كلمنا"
    r")",
    re.IGNORECASE | re.UNICODE,
)

# WhatsApp needs its own three-way classification because the bare word is
# common in two very different contexts:
#   1. A genuine off-platform solicitation ("chat us on WhatsApp", a phone
#      number sitting right next to it, or a wa.me link — already covered
#      above) → hard evidence.
#   2. A device FEATURE description ("WhatsApp notification support",
#      "compatible with WhatsApp calls" on a smartwatch/earbuds listing) →
#      not evidence of anything; must not be flagged at all.
#   3. Anything else — WhatsApp mentioned with no feature context and no
#      clear solicitation wording → ambiguous, worth a human glance but not
#      an automatic reject, so it's soft evidence only.
# "واتساب" / "واتس اب" is how WhatsApp is written in Arabic listings; the
# Latin spelling never appears in them, so the bare pattern missed it entirely.
_WHATSAPP_ANY_RE = re.compile(r"whats\s*app|واتس\s*اب|واتساب", re.IGNORECASE | re.UNICODE)
_WHATSAPP_FEATURE_CTX_RE = re.compile(
    r"whats\s*app\s*(?:\w+\s+){0,2}(?:notification|notifications|call|calls|calling"
    r"|support|compatib\w*|sync\w*|enabled|feature\w*|message\w*|alert\w*|chat\w*)"
    r"|(?:notification|notifications|call|calls|calling|support|compatib\w*|sync\w*"
    r"|enabled|feature\w*|receive|reply\s+to|read)\s*(?:\w+\s+){0,2}whats\s*app",
    re.IGNORECASE,
)
_WHATSAPP_CONTACT_RE = re.compile(
    r"wa\.me/\S+"
    r"|whats\s*app[^.!?\n]{0,40}\+?\d[\d\s\-]{6,}\d"           # "whatsapp ... 0712 345 678"
    r"|\+?\d[\d\s\-]{6,}\d[^.!?\n]{0,40}whats\s*app"           # "0712 345 678 ... whatsapp"
    r"|whats\s*app\s*(?:us\b|number|no\.?\s*:?\s*\d|:\s*\d)"
    r"|(?:chat|message|msg|contact|order|dm|reach)\s+(?:with\s+)?(?:us\s+)?(?:on|via|through)?\s*whats\s*app",
    re.IGNORECASE,
)


def check_offplatform_contact(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Detect sellers hiding contact details (phones, WhatsApp, URLs) or
    divert-the-buyer wording in NAME/DESCRIPTION/SHORT_DESCRIPTION — the
    standard pattern for taking buyers off-platform.

    HTML tags are stripped before scanning so markup attributes (styles,
    color hex codes) can't false-positive the phone patterns. Matches are
    tracked per-column (not on one merged blob) so Comment_Detail — the only
    field the pipeline actually carries through to the report/card/export —
    can say exactly WHAT was found and WHICH field it was found in, instead
    of just "something matched somewhere in this product".
    """
    text_cols = [c for c in ("NAME", "DESCRIPTION", "SHORT_DESCRIPTION") if c in data.columns]
    if not text_cols or "PRODUCT_SET_SID" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    # Per-column stripped/lowered text — kept separate (not concatenated)
    # so a match can be attributed back to its specific source field.
    # Digits fold here, once per column, so every pattern below sees ASCII
    # numerals whatever script the listing was written in. Doing it at this
    # point also means the comment quotes the folded form, which is what a
    # reviewer can actually dial.
    col_text = {
        c: data[c].astype(str)
                  .str.replace(_HTML_TAG_RE, " ", regex=True)
                  .str.translate(_ARABIC_DIGITS)
                  .str.lower()
        for c in text_cols
    }

    # Cheap vectorized pre-filter: only rows carrying an actual contact detail
    # reach the per-row loop. Divert-the-buyer wording is NOT part of this —
    # on its own it is not evidence that anyone can be contacted off-platform.
    pre_mask = pd.Series(False, index=data.index)
    for c in text_cols:
        for _, _kind_re in _CONTACT_KINDS:
            pre_mask |= col_text[c].str.contains(_kind_re, na=False)
        pre_mask |= col_text[c].str.contains(_WHATSAPP_CONTACT_RE, na=False)
    if not pre_mask.any():
        return pd.DataFrame(columns=data.columns)

    def _is_platform_url(term: str) -> bool:
        """Jumia's own domains and image CDNs are not off-platform contact."""
        t = term.lower()
        return "jumia" in t or "wsrv.nl" in t or "cloudfront" in t

    comments: dict = {}
    for idx in data.index[pre_mask]:
        found_by_col: list = []   # [(column, ["phone number: 0712…", …])]
        soft_terms: set = set()
        for c in text_cols:
            text = col_text[c].loc[idx]
            hits: list = []
            for kind_label, kind_re in _CONTACT_KINDS:
                terms = sorted({
                    m.strip() for m in kind_re.findall(text)
                    if m and m.strip() and not _is_platform_url(m)
                })
                if terms:
                    hits.append(f"{kind_label}: {', '.join(terms[:2])}")

            # A WhatsApp mention only counts when it comes with a number or a
            # wa.me link. A bare mention, or a device-feature mention such as
            # "WhatsApp notification support", is not contact information.
            if _WHATSAPP_CONTACT_RE.search(text):
                hits.append("whatsapp contact")

            if hits:
                found_by_col.append((c, hits))

            # Wording is recorded only to enrich the comment on rows that
            # already have real evidence — it can no longer raise a flag alone.
            soft_terms.update(m.strip() for m in _OFFPLATFORM_SOFT_RE.findall(text))

        if not found_by_col:
            continue

        where = " | ".join(f"{c} — {'; '.join(h)}" for c, h in found_by_col)
        comment = f"Off-platform contact detected — {where}"
        if soft_terms:
            comment += f" (wording: {', '.join(sorted(soft_terms)[:2])})"
        comments[idx] = comment

    if not comments:
        return pd.DataFrame(columns=data.columns)

    flagged = data.loc[list(comments.keys())].copy()
    flagged["Comment_Detail"] = flagged.index.map(comments)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


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
            r"\b(?:case|cases|cover|covers)\b", regex=True, na=False
        )
    gen = data[mask].copy()
    if gen.empty:
        return pd.DataFrame(columns=data.columns)
    # Pre-clean the brands and organize them into an O(1) dictionary lookup by their first word
    brand_trie = {}
    for b in brands_list:
        if not b: continue
        bc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", str(b).lower())).strip()
        if not bc: continue
        first_word = bc.split()[0]
        if first_word not in brand_trie:
            brand_trie[first_word] = []
        brand_trie[first_word].append((bc, b.title()))
        
    # Sort within each bucket by length descending so longer matches (like 'Apple Watch') match before shorter ones ('Apple')
    for fw in brand_trie:
        brand_trie[fw].sort(key=lambda x: len(x[0]), reverse=True)

    def detect(n):
        nc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", str(n).lower())).strip()
        words = nc.split()
        if not words: return None
        first_word = words[0]
        if first_word in brand_trie:
            for bc, original in brand_trie[first_word]:
                if nc.startswith(bc) and (len(nc) == len(bc) or not nc[len(bc)].isalnum()):
                    return original
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


def tv_exempt_category_codes(support_files: Dict = None) -> set:
    """Category codes under the TV path prefix, resolved from the category map.

    Empty when the map is unavailable, which makes the exemption a no-op rather
    than something that silently exempts everything or nothing in particular.
    """
    if support_files is None:
        support_files = st.session_state.get("support_files", {}) or {}
    code_to_path = support_files.get("code_to_path") or {}
    if not code_to_path:
        return set()
    return {
        clean_category_code(str(code))
        for code, path in code_to_path.items()
        if str(path).strip() == TV_COLOR_EXEMPT_NODE
        or str(path).startswith(TV_COLOR_EXEMPT_PREFIX)
    }


def check_missing_color(
    data: pd.DataFrame,
    pattern: re.Pattern,
    color_categories: List[str],
    country_code: str,
) -> pd.DataFrame:
    if not {"CATEGORY_CODE", "NAME"}.issubset(data.columns) or pattern is None:
        return pd.DataFrame(columns=data.columns)
    _cats = set(clean_category_code(c) for c in color_categories)
    # Temporary exemption, off by default. A television has no meaningful
    # colour to declare, so the check produces rejections nobody acts on.
    if st.session_state.get("tv_color_exempt"):
        _cats -= tv_exempt_category_codes()
    target = data[data["_cat_clean"].isin(_cats)].copy()
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
        "random", "random color", "random colour", "assorted", "various",
        "as in the picture", "as in the pictures", "as the picture", "as per image",
        "as shown", "see image", "see photo", "all color available",
        "all color availble", "all colors available",
        "mult", "multic",
        # Printer/technical color model acronyms — not real product colors
        "cmyk", "ymck", "ycmk", "rgb", "rgba", "hsb", "hsl", "hex",
    }
    _MODIFIER_WORDS = {
        "dark", "light", "bright", "deep", "pale", "soft", "matte", "matt",
        "glossy", "metallic", "neon", "pastel", "dusty", "warm", "cool", "royal",
        "navy", "olive", "mustard", "burnt", "forest", "sky", "baby", "hot", "ice",
        "mint", "rose", "coral", "nude", "tan", "charcoal", "ash", "sand", "cream",
        "ivory", "champagne", "coffee", "chocolate", "caramel", "wine", "burgundy",
        "nordic", "jungle", "emerald", "sapphire", "ruby", "amber", "teal", "aqua",
        "indigo", "violet", "lavender", "lilac", "magenta", "fuchsia", "maroon",
        "copper", "bronze", "gold", "silver", "platinum", "dominantly", "accent",
        "accents", "print", "stripe", "striped", "check", "checked", "pattern",
        "bead", "beaded", "ring", "with", "and", "or",
    }

    _MULTICOLOR_VARIANTS = {
        "multicolor", "multicolour", "multicolored", "multicoloured",
        "multi colour", "multi color", "multi-colour", "multi-color",
        "multicolors", "multicolours",
    }

    def _is_valid_color(color_str: str, valid_set: set) -> bool:
        c = color_str.strip().lower()
        if c in _MULTICOLOR_VARIANTS:
            return True   # explicitly accepted
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
            if part in valid_set or part in _MULTICOLOR_VARIANTS:
                return True
            tokens = part.split()
            for token in tokens:
                token = token.strip()
                if token in valid_set and token not in _MODIFIER_WORDS:
                    return True
        return False

    has_color_family = "COLOR_FAMILY" in target.columns
    color_families = (
        target["COLOR_FAMILY"].astype(str).str.strip().str.lower().values
        if has_color_family
        else [""] * len(target)
    )

    mask = []
    for n, c, cf in zip(names, colors, color_families):
        # Check COLOR column validity ONLY — this is the strict requirement.
        # COLOR_FAMILY alone or color appearing in the product title/name
        # is NOT sufficient; the COLOR column must be explicitly filled.
        is_col_valid = False
        if has_color and c not in null_like:
            is_col_valid = _is_valid_color(c, valid_colors)

        if is_col_valid:
            mask.append(False)   # passes — COLOR column has a valid value
        else:
            mask.append(True)    # flag — COLOR column is missing or invalid

    flagged = target[mask].copy()
    if not flagged.empty:

        def get_reason(row):
            c_val = str(row.get("COLOR", "")).strip().lower()
            name_val = str(row.get("NAME", ""))
            cf_val = str(row.get("COLOR_FAMILY", "")).strip()
            color_in_name = bool(pattern.search(name_val))
            if c_val and c_val not in null_like:
                return f"Invalid color value in COLOR column: '{str(row.get('COLOR', '')).strip()}' — please use a specific color (e.g. Red, Blue)"
            if cf_val and cf_val.lower() not in null_like:
                return (
                    f"COLOR_FAMILY is filled ('{cf_val}') but the COLOR column is empty. "
                    "The COLOR column must be explicitly filled for this category."
                )
            if color_in_name:
                return (
                    "Color detected in product title but the COLOR column is empty. "
                    "Please fill the COLOR attribute with the specific color value."
                )
            return "COLOR column is required for this category but is missing or empty."

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
        # Literal characters, not \uXXXX escapes. Python's re understands them,
        # but pandas can hand this pattern to Arrow's RE2 engine instead, and
        # RE2 rejects them outright -- "invalid escape sequence: \u" -- which
        # took this check out entirely on the server. They were redundant too:
        # \u0027 is the apostrophe, \u2019 the curly one, and the two micro
        # signs below were already spelled out as literals in the same group.
        # The Âµ / Î¼ forms were mojibake -- UTF-8 bytes read as
        # latin-1 -- matching nothing a real product name contains.
        r"|\d+['’]?s"
        r"|\b(?:a\s+)?dozen\b"
        r"|\b(?:pack|box|set|bundle|lot)\s+of\s+\d+\b"
        r"|\bper\s+(?:kg|kgs?|g|gm|grams?|mg|mcg|ml|l|ltr|oz|lb)\b"
        r"|\d+\s*(?:mcg|µg|μg)",
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
    pat = re.compile(r"\b\d+\s*(?:gb|tb)\b", re.IGNORECASE)
    flagged = target[~target["_name_lower"].str.contains(pat, na=False)].copy()
    if not flagged.empty:
        flagged["Comment_Detail"] = "Name missing Storage/Memory spec (e.g., 64GB)"
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


# ── Specs inconsistency (phones / tablets / computers) ─────────────────────
_SPEC_ANY_RE = re.compile(r"\d+\s*(?:gb|tb)\b", re.IGNORECASE)
_SPEC_CATEGORY_KEYWORDS_RE = re.compile(
    r"phone|smartphone|tablet|laptop|desktop|computer|notebook|macbook|chromebook",
    re.IGNORECASE,
)
_SPEC_RAM_RE1 = re.compile(r"(\d+)\s*gb\s*ram\b", re.IGNORECASE)
# The colon/dash is REQUIRED (not optional) here — that's what distinguishes
# a genuine "RAM: 8GB" label from "RAM" appearing as a bare dangling word
# between two unrelated numbers, as in "8GB RAM 128GB ROM" (where "RAM
# 128GB" would otherwise misread the STORAGE value as RAM's). A stricter
# lookbehind was tried instead but that also broke the legitimate case of
# two colon-labeled specs listed back-to-back ("ROM: 64GB RAM: 8GB"), since
# RAM there is *also* preceded by another spec's "GB " — punctuation, not
# position, is the reliable signal.
_SPEC_RAM_RE2 = re.compile(r"\bram\s*[:\-]\s*(\d+)\s*gb\b", re.IGNORECASE)
_SPEC_STORAGE_GB_RE1 = re.compile(r"(\d+)\s*gb\s*(?:rom|storage|internal(?:\s+storage)?|memory)\b", re.IGNORECASE)
_SPEC_STORAGE_TB_RE1 = re.compile(r"(\d+)\s*tb\s*(?:rom|storage|internal(?:\s+storage)?|memory)\b", re.IGNORECASE)
# Same reasoning, mirrored for storage labels.
_SPEC_STORAGE_RE2 = re.compile(r"\b(?:rom|storage|internal(?:\s+storage)?|memory)\s*[:\-]\s*(\d+)\s*gb\b", re.IGNORECASE)
_SPEC_COMBO_RE = re.compile(r"\b(\d+)\s*(?:gb)?\s*[/+]\s*(\d+)\s*gb\b", re.IGNORECASE)


def _extract_ram_storage(text: str, allow_combo: bool = False) -> tuple:
    """Pulls every RAM/Storage value mentioned in `text` (already lowercased),
    normalized to GB. Returns (ram_values, storage_values) as sets — a set
    (not a single value) because a field can legitimately state more than one
    without being wrong, e.g. "RAM 8GB" appearing twice, or a combo pattern
    agreeing with an explicit one.

    allow_combo enables the "6/128GB" -> RAM=6, Storage=128 convention. It's
    a reliable shorthand in product TITLES but not in free-form description
    prose, where "Available in 4GB/8GB RAM variants" would otherwise be
    misread as RAM=4/Storage=8 instead of two RAM options — so callers only
    pass allow_combo=True for the NAME field.
    """
    ram = {int(m.group(1)) for m in _SPEC_RAM_RE1.finditer(text)}
    ram |= {int(m.group(1)) for m in _SPEC_RAM_RE2.finditer(text)}
    storage = {int(m.group(1)) for m in _SPEC_STORAGE_GB_RE1.finditer(text)}
    storage |= {int(m.group(1)) * 1024 for m in _SPEC_STORAGE_TB_RE1.finditer(text)}
    storage |= {int(m.group(1)) for m in _SPEC_STORAGE_RE2.finditer(text)}
    if allow_combo:
        for m in _SPEC_COMBO_RE.finditer(text):
            ram.add(int(m.group(1)))
            storage.add(int(m.group(2)))
    return ram, storage


def check_specs_inconsistency(
    data: pd.DataFrame, spec_category_codes: List[str] = None, **kwargs
) -> pd.DataFrame:
    """
    For phones/tablets/computers, cross-checks the RAM/Storage spec stated in
    the NAME (title) against DESCRIPTION and SHORT_DESCRIPTION — catches the
    classic copy-paste error where a listing's title says "8GB RAM" but the
    description (often reused from a different variant/SKU) says "4GB RAM".

    Only flags when the title's value doesn't appear AT ALL among a field's
    mentioned values — a description listing multiple variants ("available
    in 4GB/8GB") is not treated as a mismatch as long as the title's value is
    one of them, which keeps this from firing on legitimate variant blurbs.
    """
    if not {"NAME", "CATEGORY_CODE"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    text_cols = [c for c in ("DESCRIPTION", "SHORT_DESCRIPTION") if c in data.columns]
    if not text_cols:
        return pd.DataFrame(columns=data.columns)

    # Scope: explicit category codes (if configured) OR a category-path
    # keyword match, unioned — works out of the box with no support-file
    # setup, but still respects a precise code list when one is supplied.
    in_scope = pd.Series(False, index=data.index)
    if spec_category_codes:
        cat_codes = set(clean_category_code(c) for c in spec_category_codes)
        in_scope |= data["_cat_clean"].isin(cat_codes)
    if "CATEGORY" in data.columns:
        in_scope |= data["CATEGORY"].astype(str).str.contains(_SPEC_CATEGORY_KEYWORDS_RE, na=False)
    target = data[in_scope].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    # Cheap vectorized pre-filter: title must mention a spec number at all,
    # otherwise there's nothing to cross-check against.
    name_lower = target["NAME"].astype(str).str.lower()
    target = target[name_lower.str.contains(_SPEC_ANY_RE, na=False)]
    if target.empty:
        return pd.DataFrame(columns=data.columns)
    name_lower = name_lower.loc[target.index]

    # HTML-strip only this (usually small) candidate set, not the whole file.
    col_text = {
        c: target[c].astype(str).str.replace(_HTML_TAG_RE, " ", regex=True).str.lower()
        for c in text_cols
    }

    def _fmt(vals: set) -> str:
        return "/".join(f"{v}GB" for v in sorted(vals))

    comments: dict = {}
    for idx in target.index:
        name_ram, name_storage = _extract_ram_storage(name_lower.loc[idx], allow_combo=True)
        if not name_ram and not name_storage:
            continue
        mismatches = []
        for c in text_cols:
            text = col_text[c].loc[idx]
            if not text.strip():
                continue
            f_ram, f_storage = _extract_ram_storage(text, allow_combo=False)
            if name_ram and f_ram and not (name_ram & f_ram):
                mismatches.append(f"RAM: title says {_fmt(name_ram)}, {c} says {_fmt(f_ram)}")
            if name_storage and f_storage and not (name_storage & f_storage):
                mismatches.append(f"Storage: title says {_fmt(name_storage)}, {c} says {_fmt(f_storage)}")
        if mismatches:
            comments[idx] = "Specs inconsistency — " + " | ".join(mismatches)

    if not comments:
        return pd.DataFrame(columns=data.columns)
    flagged = data.loc[list(comments.keys())].copy()
    flagged["Comment_Detail"] = flagged.index.map(comments)
    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])


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
    tokens = []
    for m in _SIZE_UNIT_PATTERN.finditer(name.lower()):
        full = m.group(0).replace(",", ".").replace(" ", "")
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
    full_data: pd.DataFrame = None,
    **kwargs,
) -> pd.DataFrame:
    # Duplicates are a property of the batch, not of a row, so this has to see
    # every product even when it is asked about a subset.
    #
    # The batch is validated in two passes — ZIP-covered products and the rest —
    # and each pass only ever saw its own half. Two copies of the same listing,
    # one in the ZIP and one in the other file, were each unique within their
    # own pass and neither was flagged. That is precisely the case where you
    # upload both files to find the overlap.
    #
    # Grouping therefore runs over full_data when given, and the result is
    # filtered back to the subset at the end so each pass still reports only
    # its own rows.
    _subset_sids = None
    if (
        full_data is not None
        and not full_data.empty
        and "PRODUCT_SET_SID" in data.columns
        and len(full_data) > len(data)
    ):
        _subset_sids = set(data["PRODUCT_SET_SID"].astype(str).str.strip())
        # validate_products derives its helper columns on its own copy of the
        # batch, and full_data is the caller's frame, which has none of them.
        # Swapping it in without rebuilding them raised KeyError('_cat_clean')
        # on the first line that filters exempt categories — caught by the
        # per-check error handler, so the check returned nothing and the
        # Duplicate product expander simply never appeared.
        full_data = full_data.copy()
        if "_cat_clean" not in full_data.columns and "CATEGORY_CODE" in full_data.columns:
            full_data["_cat_clean"] = full_data["CATEGORY_CODE"].apply(clean_category_code)
        for _c, _src in (
            ("_brand_lower", "BRAND"),
            ("_seller_lower", "SELLER_NAME"),
            ("_name_lower", "NAME"),
        ):
            if _c not in full_data.columns and _src in full_data.columns:
                full_data[_c] = (
                    full_data[_src].astype(str).str.lower().str.strip().fillna("")
                )
        data = full_data

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

    _size_keys = [_extract_size_key(str(n)) for n in d["NAME"].values]
    d["_size_key"] = _size_keys

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

    if "_norm_name" not in d.columns:
        _nn = d["NAME"].astype(str).str.lower()
        _nn = _nn.str.replace(r"\b(new|sale|original|genuine|authentic|official|premium|quality|best|hot|2024|2025)\b", "", regex=True)
        _nn = _nn.str.replace(r"[^\w\s]", "", regex=True)
        _nn = _nn.str.replace(r"\s+", "", regex=True)
        d["_norm_name"] = _nn

    # Two model key strategies:
    # 1. _model_key: used for IMAGE dedup — uses first word when no digits so different-named
    #    products sharing a stock image are never false-flagged.
    # 2. _seo_model_key: used for SEO dedup — ONLY returns a key when real digit-based model
    #    numbers exist. Without digits, returns "" so the SEO check is skipped entirely and
    #    the text key (full name match) acts as the sole guard.
    def _extract_model_key(name):
        words = str(name).split()
        digit_words = [re.sub(r'[^a-zA-Z0-9]', '', w).lower() for w in words if any(char.isdigit() for char in w)]
        if digit_words:
            return "-".join(digit_words)
        # Fallback: first word only (distinguishes 'vuik ceiling light' from 'vasar ceiling light')
        if words:
            return re.sub(r'[^a-zA-Z0-9]', '', words[0]).lower()
        return ""

    def _extract_seo_model_key(name):
        """Only returns a model key when explicit digit model numbers are present.
        Without digits, returns '' so SEO dedup is skipped — preventing false positives
        where different products share the same first word (e.g. 'Malaika Cream' vs 'Malaika Lotion')."""
        words = str(name).split()
        digit_words = [re.sub(r'[^a-zA-Z0-9]', '', w).lower() for w in words if any(char.isdigit() for char in w)]
        return "-".join(digit_words) if digit_words else ""

    d["_model_key"] = [_extract_model_key(n) for n in d["NAME"].values]
    d["_seo_model_key"] = [_extract_seo_model_key(n) for n in d["NAME"].values]

    flagged_indices: dict = {}

    if "MAIN_IMAGE" in d.columns:
        with _IMAGE_DIM_LOCK:
            _hash_snap = dict(_IMAGE_HASH_CACHE)  

        img_vals = d["MAIN_IMAGE"].astype(str).str.strip()
        _null_like = {"nan", "none", "", "n/a", "-", "null"}
        valid_img = (img_vals.str.len() > 5) & (~img_vals.str.lower().isin(_null_like))

        if valid_img.any():
            img_d = d[valid_img].copy()
            img_d["_phash"] = img_vals[valid_img].map(lambda u: _hash_snap.get(u, ""))
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
                    + "|"
                    + hash_d["_model_key"]  # Ensures different-named products with same image are not duped
                )
                img_dup_mask = hash_d.duplicated(subset=["_img_key"], keep="first")
                if img_dup_mask.any():
                    _first_img = hash_d.drop_duplicates(
                        subset=["_img_key"], keep="first"
                    ).set_index("_img_key")["NAME"].to_dict()
                    _img_dups = hash_d[img_dup_mask]
                    for idx, k in zip(_img_dups.index, _img_dups["_img_key"]):
                        flagged_indices[idx] = f"Duplicate (same image): '{str(_first_img.get(k, ''))[:40]}'"

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
        _first_text = d.drop_duplicates(subset=["_text_key"], keep="first").set_index("_text_key")["NAME"].to_dict()
        _txt_dups = d[text_dup_mask]
        for idx, k in zip(_txt_dups.index, _txt_dups["_text_key"]):
            if idx not in flagged_indices: 
                flagged_indices[idx] = f"Duplicate: '{str(_first_text.get(k, ''))[:40]}'"


    # _seo_model_key already computed above — digits only, empty string when no model number
    d["_seo_key"] = (
        d["_seller_lower"]
        + "|"
        + d["_brand_lower"]
        + "|"
        + d["_seo_model_key"]
        + "|"
        + d["_color_key"]
        + "|"
        + d["_size_key"]
    )

    # SEO dup check only fires when a real digit-based model number was found.
    # Products without model numbers (e.g. "Ceiling Light") rely on the text key
    # (full name match) so different products with the same generic title type are safe.
    has_seo_model = d["_seo_model_key"] != ""
    seo_dup_mask = d.duplicated(subset=["_seo_key"], keep="first") & has_seo_model

    if seo_dup_mask.any():
        _first_seo = d.drop_duplicates(subset=["_seo_key"], keep="first").set_index("_seo_key")["NAME"].to_dict()
        _seo_dups = d[seo_dup_mask]
        for idx, k in zip(_seo_dups.index, _seo_dups["_seo_key"]):
            if idx not in flagged_indices: 
                flagged_indices[idx] = f"Duplicate (SEO variant): '{str(_first_seo.get(k, ''))[:40]}'"


    if not flagged_indices:
        return pd.DataFrame(columns=data.columns)

    rdf = d.loc[list(flagged_indices.keys())].copy()
    rdf["Comment_Detail"] = rdf.index.map(flagged_indices)
    base_cols = data.columns.tolist()
    extra_cols = [c for c in ["Comment_Detail"] if c not in base_cols]
    _out = rdf[base_cols + extra_cols].drop_duplicates(subset=["PRODUCT_SET_SID"])
    # Grouped over the whole batch above; report only this pass's rows.
    if _subset_sids is not None and "PRODUCT_SET_SID" in _out.columns:
        _out = _out[
            _out["PRODUCT_SET_SID"].astype(str).str.strip().isin(_subset_sids)
        ]
    return _out


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
            "check_brand_image_mismatch": check_brand_image_mismatch,
            "check_offplatform_contact": check_offplatform_contact,
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
            "check_specs_inconsistency": check_specs_inconsistency,
            "check_duplicate_products": check_duplicate_products,
            "check_fda": check_fda,
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
    full_batch: Optional[pd.DataFrame] = None,
):
    data = data.copy()
    # ROW_LEVEL_VALIDATORS map a check's result back onto `data` by index label,
    # which is only meaningful when labels are unique (a concat of per-file
    # frames can repeat them).
    if not data.index.is_unique:
        data = data.reset_index(drop=True)
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
    if "_brand_norm" not in data.columns:
        data["_brand_norm"] = _normalize_series(data["BRAND"])
    if "_name_norm" not in data.columns:
        data["_name_norm"] = _normalize_series(data["NAME"])
    if "_seller_norm" not in data.columns:
        data["_seller_norm"] = _normalize_series(data["SELLER_NAME"])
    if "_cat_clean" not in data.columns:
        data["_cat_clean"] = data["CATEGORY_CODE"].apply(clean_category_code)
    if "_sid_clean" not in data.columns:
        data["_sid_clean"] = data["PRODUCT_SET_SID"]
    
    if "_norm_name" not in data.columns:
        data["_norm_name"] = _normalize_series(data["NAME"])

    validations = [
        (
            "Wrong Category",
            check_miscellaneous_category,
            {
                "categories_list": support_files.get("categories_names_list", []),
                "compiled_rules": st.session_state.get("compiled_json_rules", {}),
                "cat_path_to_code": support_files.get("cat_path_to_code", {}),
                "code_to_path": support_files.get("code_to_path", {}),
                "country_code": country_validator.code,
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
            {
                "suspected_fake_df": support_files.get("suspected_fake", {}).get(country_validator.code, pd.DataFrame()),
                # Scopes the description scan: a sneaker's copy naming Nike is
                # a claim about the shoe, an accessory's "compatible with
                # Sony" is not.
                "sneaker_category_codes": support_files.get("sneaker_category_codes", []),
            },
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
            "FDA",
            check_fda,
            {"country_code": country_validator.code},
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
                # brands.txt, so the check can tell a declared brand that
                # exists from one invented to dodge the filter.
                "known_brands": support_files.get("known_brands", []),
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
                "color_words": support_files.get("colors", []),
            },
        ),
        (
            "Brand Image Mismatch",
            check_brand_image_mismatch,
            {"country_rules": support_files.get("restricted_brands_all", {}).get(country_validator.country, [])},
        ),
        (
            "Off-Platform Contact",
            check_offplatform_contact,
            {},
        ),
        (
            "Prohibited products",
            check_prohibited_products,
            {"prohibited_rules": support_files.get("prohibited_words_all", {}).get(country_validator.code, []),
             "code_to_path": support_files.get("code_to_path", {})},
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
            "Specs Inconsistency",
            check_specs_inconsistency,
            {
                "spec_category_codes": support_files.get(
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
                # The whole upload, so a duplicate split across the ZIP and the
                # other file is still seen. None when this is the only pass.
                "full_data": full_batch,
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
    if country_validator.code in ("KE", "UG", "GH", "SN", "CI", "EG", "MA"):
        validations += [("Powerbank Not Authorized", check_generic_powerbanks, {})]
    if country_validator.code == "KE":
        validations.append(("KEBS Banned Products", check_kebs_banned_products, {}))
        validations.append(("KEBS FDA", check_kebs_fda, {}))
    if country_validator.code == "MA":
        _ma = load_morocco_qc_rules()
        validations = [v for v in validations if v[0] != "Restricted brands"]
        validations.insert(1, ("Restricted brands", check_restricted_brands, {"country_rules": _ma.get("restricted", [])}))
        ma_prohibited_rules = [{"keyword": kw, "categories": set()} for kw in _ma.get("prohibited_keywords", [])]
        validations = [v for v in validations if v[0] != "Prohibited products"]
        validations.append(("Prohibited products", check_prohibited_products,
                            {"prohibited_rules": ma_prohibited_rules,
                             "code_to_path": support_files.get("code_to_path", {})}))
        validations.append(("MA - Marque Interdite", check_morocco_prohibited_brands, {"ma_rules": _ma}))
    if country_validator.code == "GH":
        _gh = load_ghana_qc_rules()
        validations += [("GH - Smart Glasses with Camera", check_ghana_smart_glasses, {"gh_rules": _gh})]

    # Rules from general_rules.py — the file to edit when a new broken pattern
    # turns up. Appended last so a hand-written rule never displaces a built-in
    # check in the flag priority order, and wrapped because a syntax error in a
    # file that gets edited often must not take the whole run down with it.
    try:
        from general_rules import build_validators as _general_validators
        validations += _general_validators(support_files, country_validator.code)
    except Exception:
        logger.exception("general_rules failed to load — its rules are skipped")

    results = {}
    rejected_sids: set = set()
    dup_groups = {}
    if {"NAME", "BRAND", "SELLER_NAME", "COLOR"}.issubset(data.columns):
        dt = data[["NAME", "BRAND", "SELLER_NAME", "COLOR", "PRODUCT_SET_SID"]].copy()
        for col in ["NAME", "BRAND", "SELLER_NAME", "COLOR"]:
            dt[col] = dt[col].astype(str).str.strip().str.lower()
        dup_mask = dt.duplicated(subset=["NAME", "BRAND", "SELLER_NAME", "COLOR"], keep=False)
        if dup_mask.any():
            for _, group in dt[dup_mask].groupby(["NAME", "BRAND", "SELLER_NAME", "COLOR"]):
                sids = group["PRODUCT_SET_SID"].tolist()
                for sid in sids:
                    dup_groups[sid] = sids

    # Rule/support files feed the checks as kwargs. Rather than deep-hashing
    # those (large nested dicts, thousands of compiled patterns), fingerprint the
    # files they are loaded from: any edit changes an mtime and busts the flag
    # cache. The perfume files used to be excluded on purpose, to keep ~500
    # already-written pickles reachable — the versioned key scheme below orphans
    # those pickles regardless, so the exclusion no longer buys anything and the
    # stale fake-perfume results it caused are now fixed.
    _rules_files = [
        "Restricted_Brands.xlsx", "suspected_fake.xlsx", "Prohibbited.xlsx",
        "reason.xlsx", "reasons.xlsx", "Refurb.xlsx", "category_map.xlsx",
        "perfume_catalog.xlsx", "Perfume.xlsx", "Perfume_cat.txt",
        "colors.txt", "color_cats.txt", "brands.txt", "blacklisted.txt",
        "Books_sellers.xlsx", "Books_cat.txt", "Jersey_validation.xlsx",
        "Sneakers_Cat.txt", "Sneakers_Sensitive.txt", "Fashion_cat.txt",
        "fashion brands.xlsx", "duplicate_exempt.txt", "unnecessary.txt",
        "variation.txt", "weight.txt", "smartphones.txt", "warranty.txt",
        "sensitive_words.txt", "category_qc_weighted.json",
        "Nigeria_QC_Rules.xlsx", "Morocco_rules.xlsx", "ghana_rules.py",
    ]
    _rules_sig = hashlib.md5(
        "".join(
            f"{f}:{os.path.getmtime(f):.0f}" for f in _rules_files if os.path.exists(f)
        ).encode()
    ).hexdigest()[:12]

    EXPENSIVE_VALIDATORS = {
        "Image Stretched", "Image Blurry", "Image Mismatch", "Image Infringing",
        "Image Too Many things displayed", "Duplicate product", "Wrong Category",
        "Variation Name Mismatch"
    }
    # Validators whose verdict is per-ROW, not per-PRODUCT_SET_SID. The normal
    # path below re-selects every row sharing a flagged SID (so a check can
    # return one representative row and still flag the whole set). That is wrong
    # for checks gated on a per-row field: a feed with inconsistent BRAND across
    # rows of one SID would drag a legitimately-branded row into the report on a
    # "Generic" sibling's flag. For these, only the rows the check actually
    # matched are kept.
    ROW_LEVEL_VALIDATORS = {"Suspected Fake Perfume"}
    _skip_set = {s.lower() for s in (skip_validators or [])}
    if not data_has_warranty_cols:
        _skip_set.add("product warranty")
    # Build the image dimension/hash cache HERE, on the main thread, and hand it
    # to the image validators. _fetch_all_image_dimensions reads the uploaded ZIP
    # out of st.session_state, which is unreachable from a worker thread — called
    # from inside run_batch it saw an empty store and silently skipped every
    # ZIP-sourced image. Doing it once up front also stops Stretched and Blurry
    # each triggering their own network pass.
    _image_validators = {check_image_stretched, check_image_blurry}
    _needs_image_cache = any(
        v[0].lower() not in _skip_set
        and not country_validator.should_skip_validation(v[0])
        and v[1] in _image_validators
        for v in validations
    )
    _shared_image_cache = None
    if _needs_image_cache:
        try:
            _shared_image_cache = _fetch_all_image_dimensions(data)
        except Exception as _img_err:
            logger.warning("Image dimension prefetch failed: %s", _img_err)
    if _shared_image_cache:
        validations = [
            (name, func, ({**kw, "_image_cache": _shared_image_cache} if func in _image_validators else kw))
            for name, func, kw in validations
        ]

    # Rules from general_rules.py are never skipped by the prefetch skip list.
    #
    # That list is keyed on FLAG name, and the hand-written category rules
    # deliberately file under the built-in "Wrong Category" flag so they land in
    # the same QC bucket. The ZIP ships a Category_Check_Status column, which
    # puts "Wrong Category" in the skip set — so "the ZIP already ran its
    # category check" silently disabled every rule in general_rules.py on
    # exactly the batches they exist for. The Titan Gel / DVD / Small Appliances
    # rules only ever ran when no ZIP was loaded.
    #
    # These rules are cheap string and regex work on columns already in memory,
    # and their entire purpose is to catch verdicts the ZIP's AI got wrong, so
    # there is nothing to save by trusting the ZIP here.
    def _skipped(name: str, func) -> bool:
        if getattr(func, "_rule_id", ""):
            return country_validator.should_skip_validation(name)
        return name.lower() in _skip_set or country_validator.should_skip_validation(name)

    total_tasks = len([v for v in validations if not _skipped(v[0], v[1])])
    processed_count = 0
    restricted_keys = {}
    validation_errors = []
    _last_progress_t = 0.0

    def _emit_progress(name: str, i: int, total: int):
        nonlocal _last_progress_t
        if not on_progress:
            return
        now = time.monotonic()
        if i == total or (now - _last_progress_t) >= 0.4:
            on_progress(name, i, total)
            _last_progress_t = now

    def run_batch(v_list, current_data):
        nonlocal processed_count
        batch_results = {}
        _val_workers = min(8, max(2, (os.cpu_count() or 4) * 2))

        # At most two distinct frames are handed to checks in a batch: the full
        # one, and (for EXPENSIVE_VALIDATORS) the same minus already-rejected
        # SIDs. rejected_sids is only mutated while draining results, never
        # during the submit loop below, so the filtered frame is stable here.
        # The cache key is now derived from whichever frame the check actually
        # receives — previously it was always keyed on the full frame even when
        # the check ran on the filtered one, so a run that skipped different
        # validators (and therefore rejected a different set of SIDs) could be
        # served results computed for a different input.
        _full_digests = _ColumnDigests(current_data)
        _filtered_data = None
        _filtered_digests = None
        if rejected_sids:
            _filtered_data = current_data[
                ~current_data["PRODUCT_SET_SID"].astype(str).isin(rejected_sids)
            ]
            _filtered_digests = _ColumnDigests(_filtered_data)

        with concurrent.futures.ThreadPoolExecutor(max_workers=_val_workers) as executor:
            future_to_name = {}
            for name, func, kwargs in v_list:
                if _skipped(name, func):
                    continue

                working_data = current_data
                digests = _full_digests
                if name in EXPENSIVE_VALIDATORS and rejected_sids:
                    working_data = _filtered_data
                    digests = _filtered_digests
                    if working_data.empty:
                        processed_count += 1
                        _emit_progress(name, processed_count, total_tasks)
                        continue

                ckwargs = {"data": working_data, **kwargs}
                # Empty for the built-in checks, so their cache keys are
                # unchanged; set by general_rules on the checks it builds.
                _key_extra = getattr(func, "_rule_id", "")
                # The duplicate check reads the whole upload, not just the rows
                # it is handed, so the digest of those rows does not identify
                # its answer. Without this the cache is actively wrong in the
                # case the full-batch grouping was added for: upload the CSV
                # alone, then upload it again with the ZIP, and the non-ZIP
                # subset is byte-identical — same digest, so the result computed
                # WITHOUT the ZIP is served and every cross-file duplicate is
                # missed.
                if kwargs.get("full_data") is not None:
                    try:
                        _key_extra = f"{_key_extra}|fb{df_hash(kwargs['full_data'])}"
                    except Exception:
                        # A cache miss is the safe failure here, not a stale hit.
                        _key_extra = f"{_key_extra}|fb{len(kwargs['full_data'])}r"
                cache_path = flag_cache_path(
                    name, digests, country_validator.code, _rules_sig, _key_extra,
                )
                future_to_name[executor.submit(run_cached_check, func, cache_path, ckwargs)] = name

            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]
                processed_count += 1
                _emit_progress(name, processed_count, total_tasks)
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

                        if name in ROW_LEVEL_VALIDATORS:
                            # Keep exactly the rows the check matched. No SID
                            # fan-out; no dup_groups expansion either — a dup
                            # group is identical on NAME/BRAND/SELLER/COLOR, so
                            # its other rows were evaluated and matched on their
                            # own merits if they qualify.
                            _idx = data.index.intersection(res.index)
                            final_res = data.loc[_idx].copy()
                            for _col in ("Comment_Detail", "Reason"):
                                if _col in res.columns:
                                    final_res[_col] = res.loc[_idx, _col]
                        else:
                            final_res = data[data["PRODUCT_SET_SID"].astype(str).isin(_expanded)].copy()
                            if "Comment_Detail" in res.columns:
                                _cd = res.set_index("PRODUCT_SET_SID")["Comment_Detail"].to_dict()
                                final_res["Comment_Detail"] = final_res["PRODUCT_SET_SID"].astype(str).map(_cd)
                            if "Reason" in res.columns:
                                _r = res.set_index("PRODUCT_SET_SID")["Reason"].to_dict()
                                final_res["Reason"] = final_res["PRODUCT_SET_SID"].astype(str).map(_r)

                        # Merge, not overwrite. Two checks can share a flag —
                        # a general rule filed under "Wrong Category" runs
                        # alongside the built-in check of that name — and an
                        # assignment here would silently discard whichever
                        # finished first, leaving a check that appears to run
                        # and find nothing.
                        _prev = batch_results.get(name)
                        if _prev is not None and not _prev.empty:
                            final_res = pd.concat([_prev, final_res], ignore_index=True)
                            if "PRODUCT_SET_SID" in final_res.columns:
                                final_res = final_res.drop_duplicates(subset=["PRODUCT_SET_SID"])
                        batch_results[name] = final_res
                        rejected_sids.update(_expanded)
                    else:
                        batch_results.setdefault(name, pd.DataFrame(columns=data.columns))
                except Exception as e:
                    logger.error(f"Validation error in '{name}': {e}")
                    validation_errors.append((name, str(e)))
        return batch_results

    cheap_v = [v for v in validations if v[0] not in EXPENSIVE_VALIDATORS]
    expensive_v = [v for v in validations if v[0] in EXPENSIVE_VALIDATORS]

    results.update(run_batch(cheap_v, data))
    results.update(run_batch(expensive_v, data))

    # Drain the low-resolution advisory staged by check_image_blurry's worker
    # thread. We are back on the main thread here, so session_state writes stick.
    with _IMAGE_DIM_LOCK:
        _staged_commentary = dict(_IMAGE_BLURRY_COMMENTARY)
        _IMAGE_BLURRY_COMMENTARY.clear()
    if _staged_commentary:
        try:
            _existing = st.session_state.get("_image_blurry_commentary", {})
            _existing.update(_staged_commentary)
            st.session_state["_image_blurry_commentary"] = _existing
        except Exception as _c_err:
            logger.warning("Could not record low-resolution advisory: %s", _c_err)

    if validation_errors:
        st.warning(f"{len(validation_errors)} validation checks encountered errors.")
        with st.expander("View Error Details", type="compact"):
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
    target_lang = "fr" if country_validator.country == "Morocco" else "en"
    
    rows = []
    processed_sids = set()
    
    p_sku_map = data.set_index("PRODUCT_SET_SID")["PARENTSKU"].to_dict() if "PARENTSKU" in data.columns else {}
    s_name_map = data.set_index("PRODUCT_SET_SID")["SELLER_NAME"].to_dict() if "SELLER_NAME" in data.columns else {}
    
    known_flags = [
        "Wrong Category", "Restricted brands", "Suspected Fake product", 
        "Seller Not approved to sell Refurb", "Product Warranty", "Seller Approve to sell books",
        "Seller Approved to Sell Perfume", "Perfume Tester", "Counterfeit Sneakers",
        "Suspected counterfeit Jerseys", "Prohibited products", "Unnecessary words in NAME",
        "Single-word NAME", "Generic BRAND Issues", "Fashion brand issues", "BRAND name repeated in NAME",
        "Wrong Variation", "Generic branded products with genuine brands", "Missing COLOR",
        "Missing Weight/Volume", "Incomplete Smartphone Name", "Specs Inconsistency", "Duplicate product", "Discount too high",
        "Suspicious Discount", "NG - Gift Card Seller", "NG - TV Brand Seller", "NG - HP Toners Seller",
        "NG - Apple Seller", "NG - Xmas Tree Seller", "NG - Rice Brand Seller", "GH - Smart Glasses with Camera",
        "MA - Marque Interdite", "Powerbank Not Authorized"
    ]
    all_flags = known_flags + [f for f in results.keys() if f not in known_flags]

    for name in all_flags:
        if name not in results or results[name].empty:
            continue
        res = results[name].copy()

        if "PRODUCT_SET_SID" not in res.columns:
            for possible in ["ProductSetSid", "sid", "SID"]:
                if possible in res.columns:
                    res.rename(columns={possible: "PRODUCT_SET_SID"}, inplace=True)
                    break
        if "PRODUCT_SET_SID" not in res.columns:
            continue

        res["PRODUCT_SET_SID"] = res["PRODUCT_SET_SID"].astype(str).str.strip()
        
        new_res = res[~res["PRODUCT_SET_SID"].isin(processed_sids)]
        if new_res.empty:
            continue

        rinfo = flags_mapping.get(
            name,
            {"reason": "1000007 - Other Reason", "en": f"Flagged by {name}", "fr": f"Flagged by {name}", "ar": f"Flagged by {name}"}
        )
        base_comment = rinfo.get(target_lang, rinfo.get("en"))

        sids = new_res["PRODUCT_SET_SID"].values
        
        # fillna BEFORE astype, not after. Under pandas' new string dtype
        # astype(str) leaves missing values as NaN instead of rendering them
        # as the string "nan", so astype alone stops guaranteeing a str and
        # the float reaches len() below. Filling first is correct under both
        # behaviours, and "" is a better empty comment than "nan" ever was —
        # it is falsy, so the `det_str or ...` fallbacks now actually fire.
        comments = new_res["Comment_Detail"].fillna("").astype(str).values if "Comment_Detail" in new_res.columns else [""] * len(sids)
        reasons = new_res["Reason"].fillna("").astype(str).values if "Reason" in new_res.columns else [rinfo["reason"]] * len(sids)
        max_prices = new_res["CAT_MAX_PRICE"].fillna("").astype(str).values if "CAT_MAX_PRICE" in new_res.columns else [""] * len(sids)

        for sid, det_str, row_reason, mx_prc in zip(sids, comments, reasons, max_prices):
            if sid in processed_sids:
                continue
            processed_sids.add(sid)
            
            if name == "Powerbank Not Authorized" and ("wrong category" in det_str.lower() or "power bank" in det_str.lower()):
                rows.append({
                    "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Rejected",
                    "Reason": row_reason if row_reason else "1000007 - Wrong Category", 
                    "Comment": det_str or flags_mapping.get("Wrong Category", rinfo).get(target_lang, ""),
                    "FLAG": "Wrong Category", "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": ""
                })
                continue

            comment_str = det_str if len(det_str) > 60 else (f"{base_comment} ({det_str})" if det_str else base_comment)
            rows.append({
                "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Rejected",
                "Reason": row_reason if row_reason else rinfo["reason"], "Comment": comment_str,
                "FLAG": name, "SellerName": s_name_map.get(sid, ""),
                "CAT_MAX_PRICE": mx_prc if name == "Category Max Price Exceeded" else ""
            })

    all_sids = data["PRODUCT_SET_SID"].astype(str).str.strip().unique()
    approved_sids = [s for s in all_sids if s not in processed_sids]
    
    for sid in approved_sids:
        rows.append({
            "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Approved",
            "Reason": "", "Comment": "", "FLAG": "", "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": ""
        })

    final_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["ProductSetSid", "Status", "Reason", "Comment", "FLAG", "SellerName", "CAT_MAX_PRICE"])
    final_df["PRODUCT_SET_SID"] = final_df["ProductSetSid"]
    
    for _bool_col in ("Is_Zip", "Is_Manual"):
        if _bool_col not in final_df.columns:
            final_df[_bool_col] = False
            
    return country_validator.ensure_status_column(final_df), results


def cached_validate_products(
    data_hash: str,
    _data: pd.DataFrame,
    _support_files: Dict,
    country_code: str,
    data_has_warranty_cols: bool,
    skip_validators: Optional[List[str]] = None,
    _on_progress: Optional[callable] = None,
    _full_batch: Optional[pd.DataFrame] = None,
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
        full_batch=_full_batch,
    )


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
    st.session_state.no_computation_zip = False
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
    st.set_page_config(page_title="Product QC", layout=st.session_state.layout_mode)
except:
    pass

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

# Everything visual now comes from one place. app_css() carries the tokens,
# the type scale and the contrast-corrected button rules — the orange fills
# that used to sit under white text at 2.43:1 now carry dark ink at 7.2:1.
from design_tokens import (
    COLORS as DT,
    SEVERITY,
    SEVERITY_ORDER,
    app_css,
    flag_label,
    flag_severity,
    severity_sort_key,
)

_app_css = app_css()

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
        {_app_css}
    </style>
""",
    unsafe_allow_html=True,
)

try:
    from loaders import load_support_files_lazy
    support_files = load_support_files_lazy()
    st.session_state.support_files = support_files
    st.session_state["compiled_json_rules"] = support_files.get("compiled_json_rules", {})
except Exception as e:
    # An error explains what broke and what to do, in the interface's voice.
    # The traceback still goes to the log and stays available for support,
    # but it is not the first thing a reviewer reads.
    logger.exception("Support file load failed")
    st.error(
        "The validation rules could not be loaded, so no products can be "
        "checked. This usually means a rules file is missing or open in "
        "another program. Close any open rules spreadsheets and reload the "
        "page.",
        icon=":material/error:",
    )
    with st.expander("Technical details (for support)", expanded=False, type="compact"):
        st.code(f"{type(e).__name__}: {e}")
    st.stop()


def get_default_country():
    import os
    try:
        if os.path.exists(".country_pref"):
            with open(".country_pref", "r") as f:
                saved = f.read().strip()
                if saved in ["Kenya", "Uganda", "Nigeria", "Ghana", "Morocco", "Egypt", "Senegal", "Ivory Coast"]:
                    return saved
    except:
        pass
    try:
        lang = st.context.headers.get("Accept-Language", "")
        if "KE" in lang: return "Kenya"
        if "UG" in lang: return "Uganda"
        if "NG" in lang: return "Nigeria"
        if "GH" in lang: return "Ghana"
        if "MA" in lang: return "Morocco"
    except:
        pass
    return "Kenya"

def set_country_pref(c: str):
    try:
        with open(".country_pref", "w") as f:
            f.write(c)
    except:
        pass


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
    f"<img src='data:image/png;base64,{logo_base64}' class='rail-logo' alt='Jumia'>"
    if logo_base64
    else "<span class='material-symbols-outlined rail-logo-fallback'>verified_user</span>"
)

# The 25px-padded orange gradient banner that used to live here was the
# largest element on the page and carried no information a reviewer needed.
# What they do need — which market's rules are running, how big the batch is,
# how much of it is rejected — was below the fold or inside a collapsed
# expander. That trade is inverted here: a compact rail, filled in later in
# the script once the country and the report are both known.
_rail_slot = st.container()

with st.sidebar:
    lang_names = list(LANGUAGES.keys())
    current_lang_code = st.session_state.get("ui_lang", "en")
    current_lang_name = next((k for k, v in LANGUAGES.items() if v == current_lang_code), "English")
    selected_lang_name = st.selectbox("Language / Langue / اللغة", lang_names, index=lang_names.index(current_lang_name))
    new_lang_code = LANGUAGES[selected_lang_name]
    if new_lang_code != current_lang_code:
        st.session_state.ui_lang = new_lang_code
        st.rerun()
    st.markdown("---")
    st.header(_t("system_status"))
    if st.button(_t("clear_cache"), width='stretch', type="secondary"):
        st.cache_data.clear()
        st.session_state.display_df_cache = {}

        def robust_cleanup(directory):
            if os.path.exists(directory):
                for root, dirs, files in os.walk(directory, topdown=False):
                    for name in files:
                        try: os.remove(os.path.join(root, name))
                        except (PermissionError, OSError): pass 
                    for name in dirs:
                        try: os.rmdir(os.path.join(root, name))
                        except (PermissionError, OSError): pass

        robust_cleanup(PARQUET_CACHE_DIR)
        robust_cleanup(FLAG_CACHE_DIR)
        st.toast("Cache cleared! (Locked files skipped)", icon="🧹")
        st.rerun()
    st.markdown("---")
    # ── General rules ─────────────────────────────────────────────────────
    # The validator dispatch catches a failing check and logs it, so a rule
    # with a bad category path or a typo does not crash anything — it just
    # never fires. That is the worst way for a file people edit weekly to
    # fail, so its state is shown rather than left to be inferred from an
    # expander that never appears.
    with st.expander("General rules", icon=":material/rule:"):
        try:
            from general_rules import RULES as _GRULES, rule_health

            _health = rule_health(st.session_state.get("support_files", {}))
            _bad = _health[_health["Status"] == "no categories matched"]
            _on = int(_health["Active"].sum())
            st.caption(f"{_on} of {len(_GRULES)} active — edit general_rules.py to change them")
            st.dataframe(_health, hide_index=True, width="stretch")
            if not _bad.empty:
                st.warning(
                    f"{len(_bad)} rule(s) resolved to no categories — check the paths in "
                    "`wrong_in` against the category map. These will never fire.",
                    icon=":material/warning:",
                )
        except Exception as _e:
            st.error(
                f"general_rules.py failed to load — all its rules are inactive.\n\n`{_e}`",
                icon=":material/error:",
            )
    st.markdown("---")
    # ── Temporary rule overrides ──────────────────────────────────────────
    # Kept visibly separate from display settings: these change what gets
    # rejected, not how it looks, and they are meant to be switched off again.
    st.header("Temporary rules")
    # No `value=` here. The key is seeded at module scope, and passing both a
    # default and a session-state value makes Streamlit warn that one of them
    # is being ignored — it is, the session state wins.
    _tv_now = st.toggle(
        "Exempt TVs from colour checks",
        key="tv_color_exempt",
        help=(
            "Skips the Missing COLOR check for everything under "
            "Electronics / Television & Video / Televisions / …, and approves "
            "anything the ZIP rejected for colour in those categories, and "
            "reports those as false rejections in the audit. ON by default, "
            "temporarily. Televisions only — not DVD players, VCRs, AV "
            "receivers, satellite equipment, mounts, remotes or TV furniture."
        ),
    )
    # Compared against the value the last batch was actually processed under,
    # not against session state.
    #
    # This used to read the "previous" value out of st.session_state just above
    # the widget. For a keyed widget that is already the NEW value by the time
    # the script reruns — Streamlit applies widget state before running — so
    # the two were always equal and the reprocess below never fired once. The
    # exemption could be flipped and the stale report stayed on screen, which
    # is the exact failure the block was written to prevent.
    _tv_applied = st.session_state.get("_tv_color_exempt_applied")
    if _tv_applied is not None and _tv_now != _tv_applied:
        # The exemption changes validation output, so the current report is
        # stale the moment it is flipped. Forcing a reprocess is the honest
        # response — leaving the old verdicts on screen under a new rule is
        # how a "temporary" setting turns into a silently wrong batch.
        st.session_state["_tv_color_exempt_applied"] = _tv_now
        st.session_state.last_processed_files = None
        st.rerun()
    st.session_state["_tv_color_exempt_applied"] = _tv_now
    if _tv_now:
        _tv_n = len(tv_exempt_category_codes())
        st.caption(
            f":orange[TV colour exemption is ON]"
            + (f" — {_tv_n} categories" if _tv_n else " — category map unavailable, no effect")
        )
    st.markdown("---")
    st.header(_t("display_settings"))
    new_mode = ("wide" if "Wide" in st.radio("Layout Mode", ["Centered", "Wide"], index=1 if st.session_state.get("layout_mode", "wide") == "wide" else 0) else "centered")
    if new_mode != st.session_state.get("layout_mode", "wide"):
        st.session_state.layout_mode = new_mode

        st.rerun()

    # ── AI Learning admin ────────────────────────────────────────────────
    # The category matcher silently reshapes its own suggestions based on
    # what gets written to cat_learning.db (approved corrections, rejected
    # negatives) — this gives a reviewer visibility into what it "knows" and
    # a way to undo a bad entry (e.g. a mis-click that permanently excludes
    # a category from suggestions for one product name).
    if _CAT_MATCHER_AVAILABLE:
        st.markdown("---")
        st.header("🧠 AI Learning")
        _learn_engine = _get_cat_matcher_engine()
        _n_corr, _n_neg = _learn_engine.counts()
        _lc1, _lc2 = st.columns(2)
        _lc1.metric("Corrections", _n_corr, help="Approved (name → category) pairs the matcher learned from")
        _lc2.metric("Negatives", _n_neg, help="Categories a human explicitly rejected for a product name — excluded from future suggestions")
        with st.expander("Manage learned data", expanded=False, type="compact"):
            # Streamlit builds the body of an expander even while it is
            # collapsed, so these two 500-row tables were being queried,
            # converted to Arrow and pushed to the browser on EVERY rerun —
            # measured at ~0.9s of the ~1.3s a warm script run costs, for a
            # panel almost nobody opens. Load them only when asked.
            if not st.session_state.get("_show_learned_data", False):
                st.caption(
                    f"{_n_corr:,} corrections · {_n_neg:,} negatives learned. "
                    "The tables are loaded on demand to keep every other "
                    "interaction fast."
                )
                if st.button("Load entries", key="load_learned_data", width="stretch"):
                    st.session_state._show_learned_data = True
                    st.rerun()
            else:
                if st.button("Hide entries", key="hide_learned_data", width="stretch"):
                    st.session_state._show_learned_data = False
                    st.rerun()
                _learn_tab_corr, _learn_tab_neg = st.tabs(["Corrections", "Negatives"])
                with _learn_tab_corr:
                    _corr_df = _learn_engine.list_corrections()
                    if _corr_df.empty:
                        st.caption("No learned corrections yet.")
                    else:
                        _corr_sel = st.dataframe(
                            _corr_df, hide_index=True, width='stretch', height=220,
                            selection_mode="multi-row", on_select="rerun", key="corr_admin_df",
                        )
                        _corr_rows = _corr_sel.selection.rows if _corr_sel and _corr_sel.selection else []
                        if st.button(f"Delete selected ({len(_corr_rows)})", key="del_corr_btn", disabled=not _corr_rows):
                            _ids = _corr_df.iloc[_corr_rows]["id"].tolist()
                            _n = _learn_engine.delete_corrections(_ids)
                            st.toast(f"Deleted {_n} correction(s)", icon="🗑")
                            st.rerun()
                with _learn_tab_neg:
                    _neg_df = _learn_engine.list_negatives()
                    if _neg_df.empty:
                        st.caption("No learned negatives yet.")
                    else:
                        _neg_sel = st.dataframe(
                            _neg_df, hide_index=True, width='stretch', height=220,
                            selection_mode="multi-row", on_select="rerun", key="neg_admin_df",
                        )
                        _neg_rows = _neg_sel.selection.rows if _neg_sel and _neg_sel.selection else []
                        if st.button(f"Delete selected ({len(_neg_rows)})", key="del_neg_btn", disabled=not _neg_rows):
                            _ids = _neg_df.iloc[_neg_rows]["id"].tolist()
                            _n = _learn_engine.delete_negatives(_ids)
                            st.toast(f"Deleted {_n} negative(s)", icon="🗑")
                            st.rerun()

st.header(f":material/upload_file: {_t('upload_files')}", anchor=False)
current_country = st.session_state.get("selected_country", get_default_country())

_FLAG_SVGS = {
    "Kenya": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#006600" d="M0 0h512v512H0z"/><path fill="#fff" d="M0 170.7h512v170.7H0z"/><path fill="#000" d="M0 192h512v128H0z"/><path fill="#c8102e" d="M224 256 80 160v192zm64 0 144-96v192z"/><ellipse cx="256" cy="256" rx="30" ry="50" fill="#fff" stroke="#c8102e" stroke-width="8"/><ellipse cx="256" cy="256" rx="18" ry="36" fill="#c8102e"/></svg>""",
    "Uganda": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#000" d="M0 0h512v85.3H0z"/><path fill="#fcdc04" d="M0 85.3h512v85.4H0z"/><path fill="#c8102e" d="M0 170.7h512V256H0z"/><path fill="#000" d="M0 256h512v85.3H0z"/><path fill="#fcdc04" d="M0 341.3h512v85.4H0z"/><path fill="#c8102e" d="M0 426.7h512V512H0z"/><circle cx="256" cy="256" r="72" fill="#fff"/><circle cx="256" cy="256" r="60" fill="#c8102e"/><path fill="#000" d="M256 208c-13 0-22 8-22 18s6 14 14 20c-10 4-20 14-20 30h56c0-16-10-26-20-30 8-6 14-10 14-20s-9-18-22-18z"/></svg>""",
    "Nigeria": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#008751" d="M0 0h170.7v512H0z"/><path fill="#fff" d="M170.7 0h170.6v512H170.7z"/><path fill="#008751" d="M341.3 0H512v512H341.3z"/></svg>""",
    "Ghana": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#006b3f" d="M0 0h512v170.7H0z"/><path fill="#fcd116" d="M0 170.7h512v170.6H0z"/><path fill="#ce1126" d="M0 341.3h512V512H0z"/><path fill="#000" d="M256 183l18 55h58l-47 34 18 55-47-34-47 34 18-55-47-34h58z"/></svg>""",
    "Morocco": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#c1272d" d="M512 0H0v512h512z"/><path fill="none" stroke="#006233" stroke-width="12.5" d="m256 191.4-38 116.8 99.4-72.2H194.6l99.3 72.2z"/></svg>""",
    "Egypt": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#ce1126" d="M0 0h512v170.7H0z"/><path fill="#fff" d="M0 170.7h512v170.6H0z"/><path fill="#000" d="M0 341.3h512V512H0z"/><circle cx="256" cy="256" r="30" fill="#c09300"/></svg>""",
    "Senegal": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#00853f" d="M0 0h170.7v512H0z"/><path fill="#fdef42" d="M170.7 0h170.6v512H170.7z"/><path fill="#e31b23" d="M341.3 0H512v512H341.3z"/><path fill="#00853f" d="M256 183l18 55h58l-47 34 18 55-47-34-47 34 18-55-47-34h58z"/></svg>""",
    "Ivory Coast": """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path fill="#f77f00" d="M0 0h170.7v512H0z"/><path fill="#fff" d="M170.7 0h170.6v512H170.7z"/><path fill="#009e60" d="M341.3 0H512v512H341.3z"/></svg>""",
}

def _svg_to_b64(svg_str: str) -> str:
    encoded = base64.b64encode(svg_str.strip().encode("utf-8")).decode("utf-8")
    return f"data:image/svg+xml;base64,{encoded}"

_FLAG_DIR = Path("flags")
_FILE_MAP = {"Kenya": "ke", "Uganda": "ug", "Nigeria": "ng", "Ghana": "gh", "Morocco": "ma", "Egypt": "eg", "Senegal": "sn", "Ivory Coast": "ic"}
_flag_b64 = {}
for _cname, _code in _FILE_MAP.items():
    _svg_path = _FLAG_DIR / f"{_code}.svg"
    if _svg_path.exists():
        try:
            content = _svg_path.read_text(encoding="utf-8").strip()
            _flag_b64[_cname] = _svg_to_b64(content) if content else _svg_to_b64(_FLAG_SVGS.get(_cname, ""))
        except Exception:
            _flag_b64[_cname] = _svg_to_b64(_FLAG_SVGS.get(_cname, ""))
    else:
        _flag_b64[_cname] = _svg_to_b64(_FLAG_SVGS.get(_cname, ""))

_countries = ["Kenya", "Uganda", "Nigeria", "Ghana", "Morocco", "Egypt", "Senegal", "Ivory Coast"]
_O = JUMIA_COLORS["primary_orange"]

_flag_buttons_html = "".join([f"""<button onclick="selectCountry('{c}')" id="btn-{c}" class="flag-btn {"active" if c == current_country else ""}" title="{c}"><img src="{_flag_b64[c]}" alt="{c} flag" class="flag-img"><span class="flag-label">{c}</span></button>""" for c in _countries])

_flag_selector_html = f"""
<style>
  body {{ margin: 0; padding: 0; background: transparent; }}
  .flag-bar {{ display: flex; gap: 8px; align-items: center; padding: 6px 0; flex-wrap: wrap; }}
  .flag-btn {{ display: flex; align-items: center; gap: 8px; padding: 7px 14px 7px 10px; border: 2px solid #e0e0e0; border-radius: 8px; background: #fff; cursor: pointer; font-family: sans-serif; font-size: 13px; font-weight: 600; color: #444; transition: border-color .15s, box-shadow .15s, background .15s; outline: none; }}
  .flag-btn:hover {{ border-color: {_O}; background: #fff8f2; }}
  .flag-btn.active {{ border-color: {_O}; background: #fff3e6; color: {_O}; box-shadow: 0 0 0 3px rgba(255,136,0,.15); }}
  .flag-img {{ width: 26px; height: 20px; border-radius: 3px; object-fit: cover; box-shadow: 0 1px 3px rgba(0,0,0,.2); flex-shrink: 0; }}
  .flag-label {{ white-space: nowrap; }}
</style>
<div class="flag-bar" id="flag-bar">{_flag_buttons_html}</div>
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

def _reset_report_state(*, clear_uploaded_files: bool = False, clear_zip_cache: bool = False, extra_key_prefixes: tuple = ()):
    """Resets validation results/grid state. Used by every "start fresh" action
    (country switch, clear-all-files, uploader emptied, new file signature) so
    they can't drift apart and leave stale grid/cache data behind after one of
    them — previously each call site hand-copied a slightly different subset
    of this reset, which was a real source of "stale data after switching
    country" style bugs."""
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

    # Everything below is scoped to one uploaded batch and used to survive a
    # new upload, which is how products from a previous file kept appearing.
    # quick_rejects and _stagedRejections carried the old batch's manual
    # rejections onto the new one; the _zip_* maps answered image and status
    # lookups for products that were no longer loaded; post_qc_results made the
    # approval re-check consult the previous batch's flags.
    #
    # current_sig_hash is the dangerous one: it is the filename
    # checkpoint_final_report() writes to, so leaving it set meant the new
    # batch's report was saved over the previous batch's cache entry.
    for _k in (
        "quick_rejects", "_stagedRejections", "post_qc_results", "zip_qc_results",
        "_zip_sid_index", "_zip_status_cols", "_zip_prefetch_map",
        "current_sig_hash", "_data_filtered_ref",
        # PIM_QC_Result.xlsx and the verdict map derived from it. These were
        # missed when the rest of the ZIP state was added here, and they are
        # the loudest omission of the set: the seeding block below feeds every
        # SID in zip_pim_verdicts that the checks never saw straight into the
        # report. zip_qc_results was being cleared while these were not, so a
        # ZIP from an earlier upload kept injecting its whole verdict table
        # into the next, unrelated batch — a 2,085-product CSV was reporting
        # 9,793 products, 7,708 of them from a ZIP no longer loaded.
        "zip_pim_verdicts", "zip_rejection_reasons", "_platform_verdict",
        # Waivers and the carry-forward offer are both per batch too.
        "_flag_overrides", "_predecessor_offer", "_predecessor_handled",
    ):
        st.session_state.pop(_k, None)

    if clear_uploaded_files:
        st.session_state.cached_uploaded_files = []
    if clear_zip_cache:
        st.session_state.zip_image_store = {}
        st.session_state.zip_image_index = {}
        st.session_state.zip_image_source_bytes = None
    # "_flt_" clears the sidebar seller/category filters and "_fs_" the
    # per-flag search boxes, so a new batch never opens with the previous
    # batch's filters silently hiding rows.
    _prefixes = ("quick_rej_", "grid_chk_", "toast_", "_flt_", "_fs_") + tuple(extra_key_prefixes)
    _dead_keys = [k for k in st.session_state.keys() if k.startswith(_prefixes)]
    for k in _dead_keys: del st.session_state[k]


_country_bridge = st.text_input("country_bridge", value="", placeholder="COUNTRY_BRIDGE_DO_NOT_USE", key=f"country_bridge_{st.session_state.get('country_bridge_counter', 0)}", label_visibility="collapsed")
if "country_bridge_counter" not in st.session_state: st.session_state.country_bridge_counter = 0
country_choice = _country_bridge.strip() if _country_bridge.strip() in _countries else None

if country_choice and country_choice != current_country:
    st.session_state.selected_country = country_choice
    set_country_pref(country_choice)
    st.session_state.last_processed_files = None
    _reset_report_state()
    st.session_state.ui_lang = "fr" if country_choice in ["Morocco", "Senegal", "Ivory Coast"] else "en"
    st.session_state.country_bridge_counter += 1
    st.toast(f"Switching to {country_choice}…", icon=":material/public:")
    st.rerun()

country_validator = CountryValidator(st.session_state.selected_country)

_has_files = bool(st.session_state.get("cached_uploaded_files"))
if _has_files:
    if st.button("Run the checks again", width='stretch', help="Ignores the cached result and re-runs every check on the uploaded files"):
        for uf in st.session_state.get("cached_uploaded_files", []):
            fhash = hashlib.sha256(uf["bytes"]).hexdigest()[:24]
            invalidate(country_validator.country, fhash)
        st.session_state.last_processed_files = None
        st.rerun()

if "uploader_key" not in st.session_state: st.session_state.uploader_key = 0
if "confirm_clear_files" not in st.session_state: st.session_state.confirm_clear_files = False
if _has_files:
    if not st.session_state.confirm_clear_files:
        if st.button("Clear files", key="clear_files_btn", type="secondary", icon=":material/close:", help="Remove the uploaded files and start over"):
            st.session_state.confirm_clear_files = True
            st.rerun()
    else:
        st.warning("Clearing removes the uploaded files and this report. You can't undo it.")
        _cc1, _cc2 = st.columns(2)
        with _cc1:
            if st.button("Clear files and report", key="confirm_clear_files_btn", type="primary", width='stretch'):
                st.session_state.confirm_clear_files = False
                # "_sf_" used to hold one seller filter per flag; the shared
                # toolbar replaced those and is cleared by the "_flt_" prefix.
                _reset_report_state(clear_uploaded_files=True, clear_zip_cache=True)
                st.session_state.last_processed_files = "empty"
                st.session_state.uploader_key += 1
                st.rerun()
        with _cc2:
            if st.button("Cancel", key="cancel_clear_files_btn", width='stretch'):
                st.session_state.confirm_clear_files = False
                st.rerun()

uploaded_files = st.file_uploader("Upload files", type=["csv", "xlsx", "zip"], accept_multiple_files=True, key=f"daily_files_{st.session_state.uploader_key}", label_visibility="collapsed")

if uploaded_files:
    _new_cache = []
    for uf in uploaded_files:
        uf.seek(0)
        _raw = uf.read()
        # Digest once, here — process_signature below is rebuilt on every rerun
        # and hashing the raw upload bytes each time costs ~39ms per 10MB of
        # upload on every single click.
        _new_cache.append({"name": uf.name, "bytes": _raw, "md5": hashlib.md5(_raw).hexdigest()})
    st.session_state.cached_uploaded_files = _new_cache
    st.session_state._uploader_had_files = True
# Any state where the widget is not holding files clears the batch, not
# just an explicitly emptied list. A None or otherwise falsy selection
# used to fall through both branches and leave cached_uploaded_files
# pointing at the previous upload.
elif st.session_state.get("_uploader_had_files", False):
    _prev_uploader_key = st.session_state.get("_last_uploader_key", -1)
    _curr_uploader_key = st.session_state.uploader_key
    if _prev_uploader_key == _curr_uploader_key:
        _reset_report_state(clear_uploaded_files=True, clear_zip_cache=True)
        st.session_state.last_processed_files = "empty"
        st.session_state._uploader_had_files = False
st.session_state._last_uploader_key = st.session_state.uploader_key

_large_file_threshold = 5_000
_large_file_skip_validations = [
    "Image Stretched", "Image Blurry", "Image Mismatch", "Image Infringing", "Image Too Many things displayed",
]

_files_for_processing = st.session_state.get("cached_uploaded_files", [])


def _upload_digest(rec: dict) -> str:
    """Digest of one cached upload, computed once and memoised on the record."""
    digest = rec.get("md5")
    if not digest:
        digest = hashlib.md5(rec["bytes"]).hexdigest()
        rec["md5"] = digest
    return digest


# Kept as a list as well as folded into the signature string. The signature is
# hashed into an opaque filename, so on its own it cannot answer "was an
# earlier journal written for a subset of these files?" — which is what makes
# decisions findable after a ZIP is added to a batch already under review.
_process_file_tokens = sorted(f["name"] + _upload_digest(f) for f in _files_for_processing)
st.session_state._process_file_tokens = _process_file_tokens
st.session_state._process_country = country_validator.code

process_signature = (str(_process_file_tokens) + f"_{country_validator.code}" if _files_for_processing else "empty")

# Row-count estimation re-opens every uploaded ZIP/Excel file, so only redo it
# when the uploaded file set actually changes rather than on every rerun (click,
# keystroke, etc. all trigger a Streamlit rerun of this whole script).
if st.session_state.get("_row_estimate_sig") != process_signature:
    _total_estimated_rows = 0
    for _fc in _files_for_processing:
        try:
            _peek = BytesIO(_fc["bytes"])
            _name_lower = _fc["name"].lower()
            if _name_lower.endswith(".zip"):
                with zipfile.ZipFile(_peek) as _zf:
                    _qc_info = next((info for info in _zf.infolist() if "qc_results" in info.filename.lower() and info.filename.lower().endswith((".xlsx", ".xls", ".csv"))), None)
                    if _qc_info:
                        if _qc_info.filename.lower().endswith(".csv"): _total_estimated_rows += _zf.read(_qc_info).count(b"\n")
                        else: _total_estimated_rows += max(1, _qc_info.file_size // 500)
                    else: _total_estimated_rows += max(1, len(_fc["bytes"]) // 500)
            elif _name_lower.endswith(".xlsx"):
                _total_estimated_rows += pd.read_excel(_peek, engine="openpyxl", nrows=1, dtype=str).shape[0]
                _total_estimated_rows += max(0, len(_fc["bytes"]) // 500 - 1)
            else:
                _total_estimated_rows += _fc["bytes"].count(b"\n")
        except Exception:
            pass
    st.session_state._row_estimate_sig = process_signature
    st.session_state._row_estimate_cache = _total_estimated_rows

_total_estimated_rows = st.session_state.get("_row_estimate_cache", 0)
if _total_estimated_rows > _large_file_threshold:
    st.info(f"**Large file detected** (~{_total_estimated_rows:,} rows estimated) — validation may take 30–60 seconds. Image checks run in parallel to keep things fast.", icon=":material/hourglass_top:")

if st.session_state.get("last_processed_files") != process_signature:
    # clear_zip_cache, because the file set has changed: the previous ZIP's
    # image index is keyed on name/brand, so leaving it loaded lets a product
    # from the new upload resolve to an image out of the old archive.
    # _prepare_lazy_zip_images() repopulates it below for whatever ZIP is in
    # the new set, or leaves it empty when there is none.
    _reset_report_state(clear_zip_cache=True)

    if process_signature == "empty":
        st.session_state.last_processed_files = "empty"
    else:
        _engine_for_cache = _get_cat_matcher_engine() if _CAT_MATCHER_AVAILABLE else None
        _learning_stamp = str(len(_engine_for_cache.learning_db)) if _engine_for_cache else "0"
        sig_hash = hashlib.md5((process_signature + _learning_stamp + PROCESSING_CACHE_VERSION).encode()).hexdigest()
        cached_data = load_df_parquet(f"{sig_hash}_data.parquet")
        cached_data_rows = load_df_parquet(f"{sig_hash}_data_rows.parquet")
        cached_report = load_df_parquet(f"{sig_hash}_report.parquet")

        if cached_data is not None and cached_report is not None:
            _prepare_lazy_zip_images(_files_for_processing)
            st.session_state.final_report = cached_report
            st.session_state.all_data_map = cached_data
            st.session_state.all_data_rows = cached_data_rows if cached_data_rows is not None else cached_data.copy()
            st.session_state.last_processed_files = process_signature
            # Restoring from cache must also restore the checkpoint target —
            # without it checkpoint_final_report() silently no-ops and manual
            # decisions made after a cache load are never persisted.
            st.session_state.current_sig_hash = sig_hash
            st.toast("Loaded from cache", icon=":material/bolt:")
            _restored = apply_manual_decisions(
                st.session_state.final_report, load_manual_decisions(process_signature)
            )
            if _restored:
                st.toast(f"Restored {_restored} manual decision(s)", icon=":material/history:")
        else:
            try:
                with st.status("Processing files…", expanded=True) as _status:
                    st.write("Reading uploaded file(s)…")
                    _manual_approvals: set = set()
                    if not st.session_state.final_report.empty:
                        _fr0 = st.session_state.final_report
                        if "Is_Manual" in _fr0.columns:
                            _manual_approvals = set(_fr0[(_fr0["Status"] == "Approved") & (_fr0["Is_Manual"] == True)]["ProductSetSid"].astype(str).str.strip().unique())

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
                    _sid_col_qc: str | None = None

                    for uf in _files_for_processing:
                        _buf = BytesIO(uf["bytes"])
                        raw_data = pd.DataFrame()
                        if uf["name"].lower().endswith(".zip"):
                            has_zip_source = True
                            with zipfile.ZipFile(_buf) as zf:
                                members = zf.infolist()
                                qc_files = [info for info in members if "qc_results" in info.filename.lower() and info.filename.lower().endswith((".xlsx", ".xls", ".csv"))]
                                if qc_files:
                                    qc_dfs = []
                                    for qcf in qc_files:
                                        qc_data = zf.read(qcf)
                                        qdf = (pd.read_csv(BytesIO(qc_data), dtype=str) if qcf.filename.lower().endswith(".csv") else pd.read_excel(BytesIO(qc_data), dtype=str))
                                        
                                        if "QC_Skip_Reason" in qdf.columns:
                                            qdf["QC_Skip_Reason"] = qdf["QC_Skip_Reason"].astype(str).replace(
                                                {
                                                    "VARIATION and SELLER_SKU are blank for this SKU when exporting from PIM; please manually QC": "Variation missing",
                                                    "VARIATION and SELLER_SKU are blank for this SKU when exporting from PIM; please manually QC.": "Variation missing"
                                                }
                                            )
                                        if "incomplete sku" in qcf.filename.lower():
                                            if "QC_Skip_Reason" not in qdf.columns:
                                                qdf["QC_Skip_Reason"] = "Variation missing"
                                            else:
                                                qdf["QC_Skip_Reason"] = qdf["QC_Skip_Reason"].replace({"nan": "Variation missing", "None": "Variation missing", "": "Variation missing"})
                                            if "Manual_Review" not in qdf.columns:
                                                qdf["Manual_Review"] = "True"
                                        
                                        qc_dfs.append(qdf)
                                    
                                    if qc_dfs:
                                        st.session_state.zip_qc_results = pd.concat(qc_dfs, ignore_index=True)
                                        _build_zip_sid_index(st.session_state.zip_qc_results)
                                        raw_data = st.session_state.zip_qc_results.copy()

                                # ── PIM_QC_Result.xlsx ────────────────────
                                # Skipped until now: the filter above matches
                                # "qc_results" (plural) and this file is
                                # "PIM_QC_Result" (singular), so it never
                                # qualified. It is read separately rather than
                                # concatenated because its schema is nothing
                                # like the CSVs' — 5 columns against 182 —
                                # and merging them would corrupt zip_qc_results.
                                #
                                # What it gives us: the platform's own verdict
                                # for every SID (508 = 490 complete + 18
                                # incomplete, verified), already resolved to
                                # Status/Reason/Comment, plus the live list of
                                # rejection reason codes.
                                _pim = next(
                                    (
                                        i for i in members
                                        if "qc_result" in i.filename.lower()
                                        and "qc_results" not in i.filename.lower()
                                        and i.filename.lower().endswith((".xlsx", ".xls"))
                                    ),
                                    None,
                                )
                                if _pim is not None:
                                    try:
                                        _pb = BytesIO(zf.read(_pim))
                                        _xl = pd.ExcelFile(_pb)
                                        if "ProductSets" in _xl.sheet_names:
                                            _verd = _xl.parse("ProductSets", dtype=str).fillna("")
                                            _verd.columns = [str(c).strip() for c in _verd.columns]
                                            st.session_state.zip_pim_verdicts = _verd
                                        if "RejectionReasons" in _xl.sheet_names:
                                            _rr = _xl.parse("RejectionReasons", dtype=str).fillna("")
                                            st.session_state.zip_rejection_reasons = (
                                                _rr.iloc[:, 0].astype(str).str.strip()
                                                .loc[lambda s: s.ne("")].tolist()
                                            )
                                        logger.info(
                                            "PIM_QC_Result loaded: %s verdicts, %s reason codes",
                                            len(st.session_state.get("zip_pim_verdicts", [])),
                                            len(st.session_state.get("zip_rejection_reasons", [])),
                                        )
                                    except Exception as _pim_err:
                                        logger.warning("PIM_QC_Result read failed: %s", _pim_err)
                                st.session_state.zip_image_index = _index_zip_images(zf)
                                st.session_state.zip_image_source_bytes = uf["bytes"]
                        elif any(k in uf["name"].lower() for k in ("qc_results", "qc_result")):
                            has_zip_source = True
                            st.session_state.zip_qc_results = _detect_and_read_csv(_buf) if uf["name"].lower().endswith(".csv") else pd.read_excel(_buf, engine="openpyxl", dtype=str)
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
                    if not all_dfs: raise ValueError("No data could be read from the uploaded file(s).")

                    _file_mode = "pre_qc"
                    try: _file_mode = detect_file_type(all_dfs[0]) if "detect_file_type" in dir() or "detect_file_type" in globals() else "pre_qc"
                    except Exception: pass
                    st.session_state.file_mode = _file_mode

                    if _file_mode == "post_qc":
                        _status.update(label="Post-QC file detected", state="complete", expanded=False)
                        st.info("Post-QC file detected. Please use the Post-QC section.", icon=":material/fact_check:")
                        st.session_state.last_processed_files = process_signature
                    else:
                        st.write("Standardising and merging data…")
                        def _standardize_one(raw_df):
                            _std = standardize_input_data(raw_df)
                            if "PRODUCT_SET_SID" in _std.columns: _std["PRODUCT_SET_SID"] = _std["PRODUCT_SET_SID"].astype(str).str.strip()
                            _std["_has_warranty_data"] = ("PRODUCT_WARRANTY" in _std.columns or "WARRANTY_DURATION" in _std.columns)
                            return _std
                        if len(all_dfs) > 1:
                            with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_dfs)) as _std_pool:
                                std_dfs = list(_std_pool.map(_standardize_one, all_dfs))
                        else: std_dfs = [_standardize_one(all_dfs[0])]
                        for _std in std_dfs:
                            if "PRODUCT_SET_SID" in _std.columns: file_sids_sets.append(set(_std["PRODUCT_SET_SID"].unique()))
                        merged_data = pd.concat(std_dfs, ignore_index=True)
                        st.session_state.intersection_sids = (set.intersection(*file_sids_sets) if len(file_sids_sets) > 1 else set())
                        st.session_state.intersection_count = len(st.session_state.intersection_sids)
                        st.write("Validating file schema…")
                        data_prop = propagate_metadata(merged_data)
                        is_valid, errors = validate_input_schema(data_prop)
                        if not is_valid:
                            _status.update(label="Schema validation failed", state="error", expanded=True)
                            for _ve in errors: st.error(_ve)
                            st.session_state.last_processed_files = "error"
                            st.stop()

                        data_filtered, det_names = filter_by_country(data_prop, country_validator)
                        if data_filtered.empty:
                            _status.update(label="No matching products found", state="error", expanded=True)
                            _det_msg = f"No {country_validator.country} products found."
                            if det_names: _det_msg += f" Detected SKUs belong to: **{', '.join(det_names)}**."
                            st.error(_det_msg, icon=":material/error:")
                            if det_names:
                                if st.button(f"Switch to {det_names[0]} and Reprocess", type="primary", icon=":material/swap_horiz:"):
                                    st.session_state.selected_country = det_names[0]
                                    set_country_pref(det_names[0])
                                    st.session_state.country_bridge_counter += 1
                                    st.rerun()
                            st.stop()
                        if len(det_names) > 1 or (det_names and det_names[0] != country_validator.country):
                            st.toast(f"Multiple countries detected: {', '.join(det_names)}", icon=":material/info:")

                        # ── Country mismatch guard ─────────────────────────────
                        # filter_by_country only hard-stops when ZERO rows match.
                        # If the file is dominated by another country but a few
                        # rows match the selected one, we'd silently validate a
                        # tiny slice and the user would never know. Block and ask.
                        _code_to_name = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana",
                                         "MA": "Morocco", "EG": "Egypt", "SN": "Senegal", "CI": "Ivory Coast"}
                        if ("ACTIVE_STATUS_COUNTRY" in data_prop.columns
                                and st.session_state.get("_country_override_sig") != process_signature):
                            _cc = data_prop["ACTIVE_STATUS_COUNTRY"].astype(str).str.strip().str.upper().value_counts()
                            if len(_cc):
                                _dom_code = _cc.index[0]
                                _dom_share = _cc.iloc[0] / max(len(data_prop), 1)
                                _dom_name = _code_to_name.get(_dom_code)
                                _sel_share = len(data_filtered) / max(len(data_prop), 1)
                                if (_dom_name and _dom_name != country_validator.country
                                        and _dom_share >= 0.5 and _sel_share < 0.5):
                                    _status.update(label="Country mismatch detected", state="error", expanded=True)
                                    st.warning(
                                        f"This file looks like **{_dom_name}** — {_cc.iloc[0]:,} of {len(data_prop):,} rows "
                                        f"say `{_dom_code}`, but you selected **{country_validator.country}** "
                                        f"(only {len(data_filtered):,} matching rows would be validated).",
                                        icon=":material/flag:",
                                    )
                                    _g1, _g2 = st.columns(2)
                                    with _g1:
                                        if st.button(f"Switch to {_dom_name} and Reprocess", type="primary",
                                                     icon=":material/swap_horiz:", key="cmg_switch"):
                                            st.session_state.selected_country = _dom_name
                                            set_country_pref(_dom_name)
                                            st.session_state.country_bridge_counter += 1
                                            st.rerun()
                                    with _g2:
                                        if st.button(f"Process as {country_validator.country} anyway "
                                                     f"({len(data_filtered):,} rows)", key="cmg_continue"):
                                            st.session_state._country_override_sig = process_signature
                                            st.rerun()
                                    st.stop()

                        # ── Same guard, for files with no country column ──────
                        # The block above needs ACTIVE_STATUS_COUNTRY. Without
                        # it nothing filters and nothing is detected, so a
                        # Uganda file selected as Kenya was validated end to end
                        # under Kenyan rules and came back labelled Kenya —
                        # every verdict on it wrong, and nothing said so.
                        # det_names now carries the SKU-prefix inference, which
                        # is the only signal left in that case.
                        if ("ACTIVE_STATUS_COUNTRY" not in data_prop.columns
                                and det_names
                                and country_validator.country not in det_names
                                and st.session_state.get("_country_override_sig") != process_signature):
                            _guess = det_names[0]
                            _status.update(label="Country mismatch detected", state="error", expanded=True)
                            st.warning(
                                f"This file has no country column, and its SKUs look like **{_guess}** — "
                                f"but **{country_validator.country}** is selected. Nothing can be filtered by "
                                f"country here, so every one of the {len(data_prop):,} rows would be validated "
                                f"against {country_validator.country}'s rules.",
                                icon=":material/flag:",
                            )
                            _n1, _n2 = st.columns(2)
                            with _n1:
                                if st.button(f"Switch to {_guess} and Reprocess", type="primary",
                                             icon=":material/swap_horiz:", key="cmg_switch_nocol"):
                                    st.session_state.selected_country = _guess
                                    set_country_pref(_guess)
                                    st.session_state.country_bridge_counter += 1
                                    st.rerun()
                            with _n2:
                                if st.button(f"Process as {country_validator.country} anyway "
                                             f"({len(data_prop):,} rows)", key="cmg_continue_nocol"):
                                    st.session_state._country_override_sig = process_signature
                                    st.rerun()
                            st.stop()

                        actual_counts = data_filtered.groupby("PRODUCT_SET_SID")["PRODUCT_SET_SID"].transform("count")
                        if "COUNT_VARIATIONS" in data_filtered.columns:
                            file_counts = pd.to_numeric(data_filtered["COUNT_VARIATIONS"], errors="coerce").fillna(1)
                            data_filtered["COUNT_VARIATIONS"] = actual_counts.combine(file_counts, max)
                        else:
                            data_filtered["COUNT_VARIATIONS"] = actual_counts

                        for _c in ["NAME", "BRAND", "COLOR", "SELLER_NAME", "CATEGORY_CODE", "LIST_VARIATIONS"]:
                            if _c in data_filtered.columns: data_filtered[_c] = data_filtered[_c].astype(str).fillna("")
                        if "COLOR_FAMILY" not in data_filtered.columns: data_filtered["COLOR_FAMILY"] = ""
                        if "VARIATION" in data_filtered.columns:
                            data_filtered["_var_len"] = data_filtered["VARIATION"].astype(str).str.len()
                            data_filtered = data_filtered.sort_values(by="_var_len", ascending=False).drop(columns=["_var_len"])
                        data = data_filtered.drop_duplicates(subset=["PRODUCT_SET_SID"], keep="first")
                        
                        if "QC_Skip_Reason" in data.columns and "VARIATION" in data.columns:
                            mask = (data["QC_Skip_Reason"] == "Variation missing") & (data["VARIATION"].astype(str).str.strip() != "") & (data["VARIATION"].notna()) & (data["VARIATION"].astype(str).str.lower() != "nan")
                            data.loc[mask, "QC_Skip_Reason"] = ""
                            data.loc[mask, "Manual_Review"] = "False"
                        data_has_warranty = all(c in data.columns for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"])

                        qc_zip = st.session_state.zip_qc_results
                        zip_sids: set = set()
                        if has_zip_source and not qc_zip.empty:
                            _sid_col_qc = next((c for c in ("PRODUCT_SET_SID", "ProductSetSid", "Product Set SID", "cod_productset_sid", "SID",) if c in qc_zip.columns), None)
                            if _sid_col_qc: zip_sids = set(qc_zip[_sid_col_qc].astype(str).str.strip().unique())

                        all_sids = set(data["PRODUCT_SET_SID"].unique())
                        non_zip_sids = all_sids - zip_sids
                        fast_skip_list = _large_file_skip_validations if _total_estimated_rows > _large_file_threshold else []

                        st.write(f"Running validation on {len(all_sids)} products…")
                        if zip_sids: st.write(f"ZIP/QC data: {len(zip_sids)} prefetched SKUs, {len(non_zip_sids)} additional SKUs.")

                        final_report_parts: list = []
                        results_parts: list = []

                        if non_zip_sids:
                            data_non_zip = data[data["PRODUCT_SET_SID"].isin(non_zip_sids)].copy()
                            _prog = st.progress(0, text="Preparing validation...")
                            def _on_flag_done(flag_name: str, i: int, total: int): _prog.progress(int(i / total * 100), text=f"Checking: {flag_name}")
                            # sig_hash prefixes the key so the validation cache is tied to
                            # THIS upload. df_hash alone is content-derived and memoises into
                            # df.attrs, and its own docstring notes it cannot see an in-place
                            # edit that leaves shape and columns unchanged — so a different
                            # file could be served the previous batch's results. Removing a
                            # file and uploading another is exactly when that shows up.
                            fr_non_zip, res_non_zip = cached_validate_products(sig_hash + "|" + df_hash(data_non_zip) + country_validator.code, data_non_zip, support_files, country_validator.code, data_has_warranty, skip_validators=fast_skip_list, _on_progress=_on_flag_done, _full_batch=data)
                            _prog.empty()
                            final_report_parts.append(fr_non_zip)
                            results_parts.append(res_non_zip)

                        if zip_sids:
                            data_zip = data[data["PRODUCT_SET_SID"].isin(zip_sids)].copy()
                            skip_list = sorted(set(_derive_prefetched_skip_list(qc_zip)) | set(fast_skip_list))
                            _prog_zip = st.progress(0, text="Preparing ZIP validation...")
                            def _on_flag_done_zip(flag_name: str, i: int, total: int): _prog_zip.progress(int(i / total * 100), text=f"Checking (ZIP): {flag_name}")
                            fr_zip, res_zip = cached_validate_products(sig_hash + "|" + df_hash(data_zip) + country_validator.code + "_zip_optimized", data_zip, support_files, country_validator.code, data_has_warranty, skip_validators=skip_list, _on_progress=_on_flag_done_zip, _full_batch=data)
                            _prog_zip.empty()
                            final_report_parts.append(fr_zip)
                            results_parts.append(res_zip)

                        if final_report_parts:
                            final_report_subset = pd.concat(final_report_parts, ignore_index=True)
                            combined_results: dict = {}
                            for _r_dict in results_parts:
                                for _flag, _df_r in _r_dict.items():
                                    if _flag not in combined_results: combined_results[_flag] = _df_r
                                    else: combined_results[_flag] = pd.concat([combined_results[_flag], _df_r], ignore_index=True)
                        else:
                            final_report_subset = pd.DataFrame(columns=["ProductSetSid", "Status", "FLAG", "Comment", "Reason"])
                            combined_results = {}

                        final_report = pd.DataFrame({"ProductSetSid": data["PRODUCT_SET_SID"].unique()})
                        final_report["Status"] = "Approved"
                        final_report["FLAG"] = ""
                        final_report["Comment"] = ""
                        final_report["Reason"] = ""
                        final_report["Is_Zip"] = False
                        final_report["Is_Manual"] = False
                        final_report["PRODUCT_SET_SID"] = final_report["ProductSetSid"]
                        if zip_sids:
                            _zip_mask = final_report["ProductSetSid"].astype(str).str.strip().isin(zip_sids)
                            final_report.loc[_zip_mask, "Is_Zip"] = True

                        if has_zip_source and not qc_zip.empty and _sid_col_qc:
                            data = data.copy()
                            _extra_ctx = [c for c in qc_zip.columns if c not in data.columns and "status" not in c.lower() and c != _sid_col_qc]

                            # This loop used to do three O(rows) operations per
                            # column, for ~70 columns:
                            #   1. qc_zip.set_index(...)      — re-indexed the
                            #      whole ZIP frame every iteration
                            #   2. .astype(str).str.strip()   — re-normalised
                            #      every SID in `data` every iteration, a
                            #      Python-level string pass over the batch
                            #   3. data.loc[:, new] = ...     — grew the frame
                            #      one column at a time, fragmenting the block
                            #      manager (this is what the PerformanceWarning
                            #      was reporting, and it was the symptom rather
                            #      than the cause)
                            # Hoisting 1 and 2 out of the loop and building the
                            # columns in one concat leaves a single pass each.
                            #
                            # All of it collapses to one reindex. Building a
                            # per-column dict was still ~70 dicts of one entry
                            # per row; a single reindex of the whole context
                            # block is 40x faster on a 50k-row batch (5.4s ->
                            # 0.13s measured) and produces an identical frame.
                            #
                            # keep="last" on the duplicate filter is not
                            # cosmetic: Series.to_dict() silently kept the last
                            # occurrence of a repeated SID, so anything else
                            # would quietly change which ZIP row wins.
                            _sid_norm = data["PRODUCT_SET_SID"].astype(str).str.strip()
                            _want = list(_extra_ctx)
                            _img1_wanted = (
                                "image1" in qc_zip.columns and "IMAGE1_ZIP" not in data.columns
                            )
                            if _img1_wanted and "image1" not in _want:
                                _want.append("image1")

                            if _want:
                                _ctx = qc_zip.set_index(_sid_col_qc)[_want]
                                _ctx = _ctx[~_ctx.index.duplicated(keep="last")]
                                _ctx = _ctx.reindex(_sid_norm.values)
                                _ctx.index = data.index
                                if _img1_wanted:
                                    # Copy, never rename: when "image1" is also
                                    # in _extra_ctx the original produced both
                                    # columns, and renaming would drop one.
                                    _ctx["IMAGE1_ZIP"] = _ctx["image1"]
                                    if "image1" not in _extra_ctx:
                                        _ctx = _ctx.drop(columns=["image1"])
                                data = pd.concat([data, _ctx], axis=1)
                            status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                            fmap = support_files.get("flags_mapping", {})
                            _fr_sid_to_idx = pd.Series(final_report.index, index=final_report["ProductSetSid"].astype(str).str.strip()).to_dict()
                            # Status as our own checks left it, captured before
                            # the loop below starts editing final_report.
                            #
                            # The overrides answer the ZIP's complaint about ONE
                            # check: the ZIP said the colour was missing, our data
                            # shows a valid colour, so that complaint goes. They
                            # were writing a verdict on the whole product instead —
                            # Status, FLAG, Comment and Reason all overwritten —
                            # which silently erased a rejection our own checks had
                            # made for something unrelated, and blanked the reason
                            # so nothing downstream could tell.
                            #
                            # Snapshotted rather than read live because the loop
                            # writes rejections itself. Reading final_report as it
                            # stands would make the guard depend on the order the
                            # ZIP happens to list its status columns.
                            _qc_rejected_sids = set(
                                final_report.loc[
                                    final_report["Status"].astype(str).str.strip().str.lower() == "rejected",
                                    "ProductSetSid",
                                ].astype(str).str.strip()
                            )

                            def _clear_zip_complaint(_fidx, _sid, _comment, _tag):
                                """Drop the ZIP's complaint about one check.

                                Approves outright only when the product is not
                                already rejected by our own checks; otherwise the
                                complaint is cleared, the rejection stands, and the
                                disagreement is noted on the comment.
                                """
                                if _sid in _qc_rejected_sids:
                                    _prev = str(final_report.at[_fidx, "Comment"] or "").strip()
                                    _note = f"ZIP {_tag} complaint cleared; kept rejected by QC checks"
                                    final_report.at[_fidx, "Comment"] = f"{_prev} — {_note}" if _prev else _note
                                    # Is_Zip and zip_override are deliberately NOT
                                    # set here. zip_override drives a green
                                    # "Overridden — auto-approved" badge on the grid
                                    # card, which would be a lie on a product that
                                    # stayed rejected, and Is_Zip feeds the KPI's
                                    # "from ZIP/prefetch" rejected count — this
                                    # rejection is ours, not the ZIP's.
                                    return
                                final_report.at[_fidx, "Status"] = "Approved"
                                final_report.at[_fidx, "FLAG"] = "Approved by User"
                                final_report.at[_fidx, "Comment"] = _comment
                                final_report.at[_fidx, "Reason"] = ""
                                final_report.at[_fidx, "Is_Zip"] = True
                                final_report.at[_fidx, "zip_override"] = _tag
                            # Resolved once per batch, not per row: this walks
                            # the whole category map. Empty set when the toggle
                            # is off, which makes the branch below a no-op.
                            _tv_exempt_codes = (
                                tv_exempt_category_codes(support_files)
                                if st.session_state.get("tv_color_exempt") else set()
                            )
                            _data_by_sid = {sid: grp for sid, grp in data.groupby(data["PRODUCT_SET_SID"].astype(str).str.strip(), sort=False)}
                            _zip_result_rows: dict = {}
                            _rej_in_zip = 0
                            _learned_count = 0
                            engine = _get_cat_matcher_engine()

                            status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                            melted = qc_zip[[_sid_col_qc] + status_cols].melt(id_vars=_sid_col_qc, var_name="col", value_name="val")
                            melted["val_lower"] = melted["val"].astype(str).str.lower().str.strip()
                            rejected_entries = melted[melted["val_lower"] == "rejected"]
                            rejected_sids_set = set(rejected_entries[_sid_col_qc])

                            if engine:
                                approved_df = qc_zip[~qc_zip[_sid_col_qc].isin(rejected_sids_set)]
                                valid_learning = approved_df[approved_df["NAME"].astype(str).str.strip().astype(bool) & approved_df["CATEGORY"].astype(str).str.strip().astype(bool)]
                                for _name, _cat in zip(valid_learning["NAME"], valid_learning["CATEGORY"]):
                                    engine.apply_learned_correction(str(_name).strip(), str(_cat).strip(), auto_save=False)
                                    _learned_count += 1

                                # Negative learning: category-rejected rows teach the
                                # engine which (name, category) pairings a human said NO
                                # to — those categories are never re-suggested, and
                                # re-listings under them get auto-flagged.
                                if "Category_Check_Status" in status_cols and {"NAME", "CATEGORY"}.issubset(qc_zip.columns):
                                    _cat_rej_sids = set(rejected_entries.loc[rejected_entries["col"] == "Category_Check_Status", _sid_col_qc])
                                    if _cat_rej_sids:
                                        _rej_rows = qc_zip[qc_zip[_sid_col_qc].isin(_cat_rej_sids)]
                                        _rsn_series = (_rej_rows["Category_Check_Rejection_Reason"]
                                                       if "Category_Check_Rejection_Reason" in qc_zip.columns
                                                       else pd.Series([""] * len(_rej_rows), index=_rej_rows.index))
                                        for _name, _cat, _rsn in zip(_rej_rows["NAME"], _rej_rows["CATEGORY"], _rsn_series):
                                            engine.add_negative_correction(str(_name).strip(), str(_cat).strip(), str(_rsn).strip(), auto_save=False)
                                            _learned_count += 1

                            qc_zip_indexed = qc_zip.set_index(_sid_col_qc)
                            for mrow in rejected_entries.to_dict("records"):
                                _sid = str(mrow[_sid_col_qc]).strip()
                                _col = mrow["col"]
                                if _sid not in qc_zip_indexed.index: continue
                                _r = qc_zip_indexed.loc[_sid]
                                if isinstance(_r, pd.DataFrame): _r = _r.iloc[0]
                                _base_key = _prefetch_key_from_status_col(_col)
                                _flag = PREFETCH_MAP.get(_base_key, _base_key.replace("_", " ").title())
                                _flag_pf = f"{_flag} (Prefetched)"
                                _comment = _prefetch_reason_from_row(_r, _col, qc_zip.columns)
                                _mapped = fmap.get(_flag, {})
                                _reason_code = _mapped.get("reason", "1000007 - Other Reason")
                                _default_cmt = _mapped.get("comment", "Rejected")
                                _final_cmt = _comment if (_comment and _comment.lower() != "rejected") else _default_cmt
                                if _flag in ("Wrong Category", "Category Check") and "Category_Check_Rejection_Reason" in qc_zip.columns:
                                    _cr = str(_r["Category_Check_Rejection_Reason"]).strip()
                                    if _cr and _cr.lower() not in ("nan", "rejected"):
                                        _final_cmt = _cr
                                    # Resolve to the specific sub-bucket (e.g. "Category Check – Prohibited Category")
                                    _cat_sub_bucket = _classify_category_check_sub_bucket(_cr)
                                    
                                    # If it's an API error, skip rejecting it in the main UI
                                    # (it will still be visible in the Targeted Audit).
                                    if _cat_sub_bucket == "Category Check \u2013 AI API Errors":
                                        continue

                                    # Override flag + prefetched label to use the sub-bucket
                                    _flag = _cat_sub_bucket
                                    _flag_pf = f"{_cat_sub_bucket} (Prefetched)"

                                if _flag in ("Product Name Brand Name", "BRAND name repeated in NAME"):
                                    _nb_reason = _comment
                                    _nb_sub_bucket = _classify_name_brand_sub_bucket(_nb_reason)
                                    _flag = _nb_sub_bucket
                                    _flag_pf = f"{_nb_sub_bucket} (Prefetched)"
                                
                                if _flag == "Title Language Check":
                                    _tl_reason = _comment
                                    _tl_sub_bucket = _classify_title_language_sub_bucket(_tl_reason)
                                    _flag = _tl_sub_bucket
                                    _flag_pf = f"{_tl_sub_bucket} (Prefetched)"
                                _fidx = _fr_sid_to_idx.get(_sid)
                                if _fidx is not None:
                                    if "warranty" in _base_key.lower():
                                        _wval = str(_r.get("PRODUCT_WARRANTY", "")).strip()
                                        if _wval and _wval.lower() not in ("nan", "none"):
                                            _clear_zip_complaint(
                                                _fidx, _sid, "Approved by user for warranty", "warranty",
                                            )
                                            continue

                                    if "title" in _base_key.lower() and "weight" in _base_key.lower() or "title_language_weight" in _base_key.lower():
                                        missing_vol_df = res_zip.get("Missing Weight/Volume") if res_zip else None
                                        if missing_vol_df is None or missing_vol_df.empty or _sid not in missing_vol_df["PRODUCT_SET_SID"].astype(str).values:
                                            _clear_zip_complaint(
                                                _fidx, _sid, "Approved by user for Title/Volume", "volume",
                                            )
                                            continue

                                    if "color" in _base_key.lower():
                                        # TV exemption, when enabled: the ZIP's
                                        # colour verdict is overridden outright
                                        # for these categories, without the
                                        # usual requirement that a recognised
                                        # COLOR value be present — the whole
                                        # point is that these products do not
                                        # carry one.
                                        if _tv_exempt_codes:
                                            _tv_grp = _data_by_sid.get(_sid)
                                            _tv_code = ""
                                            if _tv_grp is not None and not _tv_grp.empty and "CATEGORY_CODE" in _tv_grp.columns:
                                                _tv_code = clean_category_code(str(_tv_grp["CATEGORY_CODE"].iloc[0]))
                                            if _tv_code and _tv_code in _tv_exempt_codes:
                                                _clear_zip_complaint(
                                                    _fidx, _sid, "Colour check exempt (TV category)", "color_tv_exempt",
                                                )
                                                continue

                                        missing_col_df = res_zip.get("Missing COLOR") if res_zip else None
                                        # To override a color rejection, the product must pass the main
                                        # validation AND have a COLOR value that is recognised in colors.txt.
                                        passed_main_validation = missing_col_df is None or missing_col_df.empty or _sid not in missing_col_df["PRODUCT_SET_SID"].astype(str).values

                                        _has_explicit_color = False
                                        _explicit_color_value = ""
                                        _temp_grp = _data_by_sid.get(_sid)
                                        if _temp_grp is not None and not _temp_grp.empty:
                                            _color_col_name = next((c for c in _temp_grp.columns if str(c).strip().upper() == "COLOR"), None)
                                            if _color_col_name:
                                                _c_vals = _temp_grp[_color_col_name].astype(str).str.strip()
                                                _null_vals = {"nan", "none", "", "n/a", "-", "null"}
                                                _non_null = [v for v in _c_vals if v.lower() not in _null_vals]
                                                if _non_null:
                                                    _explicit_color_value = _non_null[0].lower()
                                                    _has_explicit_color = True

                                        # Validate against colors.txt — the single source of truth for
                                        # recognised colors. A color must appear in colors.txt (supports
                                        # multi-part values like "Red/Blue" — at least one part must match).
                                        # If colors.txt is empty/missing, fall back to the junk-list check.
                                        _zip_valid_colors = load_valid_colors()
                                        _MULTICOLOR_VARIANTS_ZIP = {
                                            "multicolor", "multicolour", "multicolored", "multicoloured",
                                            "multi colour", "multi color", "multi-colour", "multi-color",
                                            "multicolors", "multicolours",
                                        }

                                        def _zip_color_recognised(color_val: str, valid_set: set) -> bool:
                                            """Return True only if color_val is in colors.txt (or a multicolor variant)."""
                                            c = color_val.strip().lower()
                                            if c in _MULTICOLOR_VARIANTS_ZIP:
                                                return True
                                            if not valid_set:
                                                # colors.txt unavailable — reject ambiguous values to be safe
                                                return False
                                            # Split composite values: "Red/Blue", "Black & Gold", etc.
                                            parts = re.split(r"[,/&|]|\s+and\s+|\s+or\s+|\s+with\s+", c)
                                            for part in parts:
                                                part = part.strip()
                                                if not part:
                                                    continue
                                                if part in valid_set or part in _MULTICOLOR_VARIANTS_ZIP:
                                                    return True
                                                # Allow modifier + base color: "dark blue", "light grey"
                                                tokens = part.split()
                                                for token in tokens:
                                                    if token in valid_set:
                                                        return True
                                            return False

                                        _color_is_valid = (
                                            _has_explicit_color
                                            and _zip_color_recognised(_explicit_color_value, _zip_valid_colors)
                                        )

                                        if passed_main_validation and _color_is_valid:
                                            _clear_zip_complaint(
                                                _fidx, _sid, "Approved by user for Color", "color",
                                            )
                                            continue

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
                                            if _zcu in ("INITIAL_CATEGORY", "CORRECT_CATEGORY", "SUGGESTED_CATEGORY", "AI_CATEGORY"): _row_data[_zcol] = _r[_zcol]
                                            elif (_zcol not in data.columns and "status" not in str(_zcol).lower() and "reason" not in str(_zcol).lower() and _zcol != _sid_col_qc): _row_data[_zcol] = _r[_zcol]
                                        _row_data["Comment_Detail"] = _comment
                                        _zip_result_rows.setdefault(_flag, []).append(_row_data)
                                if str(_r.get("Manual_Review", "")).lower() in ("true", "1", "yes"):
                                    _fidx = _fr_sid_to_idx.get(_sid)
                                    if _fidx is not None:
                                        # "Already approved" upstream does not overturn a rejection
                                        # our own checks just made.
                                        #
                                        # This wrote Approved unconditionally, and it runs after
                                        # validation, so it silently replaced every verdict we had
                                        # reached — prohibited terms, restricted brands, wrong
                                        # category, all of it. A sexual wellness product filed as a
                                        # shaving gel was rejected by our rules and then handed back
                                        # as Approved because the file said a human had seen it.
                                        #
                                        # The upstream reviewer approved it against the checks THEY
                                        # ran; that is not a judgement about a check they never
                                        # applied. So the override still approves anything we passed,
                                        # and leaves our rejections alone.
                                        # Against the pre-loop snapshot, not the live
                                        # Status: the loop writes rejections of its
                                        # own, so reading live made this depend on
                                        # the order the ZIP lists its status columns.
                                        if _sid in _qc_rejected_sids:
                                            # Keep the rejection, but record that the file disagreed,
                                            # so the conflict is visible rather than just absent.
                                            _prev_cmt = str(final_report.at[_fidx, "Comment"] or "").strip()
                                            _note = "File marked this Already Approved; kept rejected by QC checks"
                                            final_report.at[_fidx, "Comment"] = (
                                                f"{_prev_cmt} — {_note}" if _prev_cmt else _note
                                            )
                                            final_report.at[_fidx, "Is_Zip"] = True
                                        else:
                                            final_report.at[_fidx, "Status"] = "Approved"
                                            final_report.at[_fidx, "FLAG"] = "Manual review"
                                            final_report.at[_fidx, "Comment"] = "Already Approved"
                                            final_report.at[_fidx, "Is_Zip"] = True
                            for _flag, _rows in _zip_result_rows.items():
                                _combined_r = pd.concat(_rows, ignore_index=True)
                                if _flag in combined_results and not combined_results[_flag].empty: combined_results[_flag] = pd.concat([combined_results[_flag], _combined_r], ignore_index=True)
                                else: combined_results[_flag] = _combined_r
                            if _learned_count > 0 and engine:
                                engine.save_learning_db()
                                st.write(f"AI learned {_learned_count} new category mappings from ZIP.")
                            if _rej_in_zip > 0:
                                st.write(f"Successfully mapped {_rej_in_zip} rejections from ZIP/QC file.")
                                st.session_state.pop("_grid_review_data_cache", None)
                                st.session_state.display_df_cache.clear()

                        if not final_report_subset.empty:
                            fmap = support_files.get("flags_mapping", {})
                            rejected_subset = final_report_subset[final_report_subset["Status"] == "Rejected"]
                            if not rejected_subset.empty:
                                rej_first = rejected_subset.drop_duplicates(subset=["ProductSetSid"], keep="first")
                                rej_sids = set(rej_first["ProductSetSid"].astype(str).str.strip())
                                
                                fr_sids = final_report["ProductSetSid"].astype(str).str.strip()
                                update_mask = fr_sids.isin(rej_sids) & (final_report["Status"] == "Approved")
                                
                                if update_mask.any():
                                    flag_map = rej_first.set_index(rej_first["ProductSetSid"].astype(str).str.strip())["FLAG"].to_dict()
                                    cmt_series = rej_first.get("Comment", pd.Series("", index=rej_first.index))
                                    if "Comment_Detail" in rej_first.columns:
                                        cmt_series = cmt_series.where(cmt_series != "", rej_first["Comment_Detail"])
                                    cmt_map = pd.Series(cmt_series.values, index=rej_first["ProductSetSid"].astype(str).str.strip()).to_dict()
                                    
                                    sids_to_update = fr_sids[update_mask]
                                    final_report.loc[update_mask, "Status"] = "Rejected"
                                    final_report.loc[update_mask, "FLAG"] = sids_to_update.map(flag_map)
                                    final_report.loc[update_mask, "Comment"] = sids_to_update.map(cmt_map)
                                    final_report.loc[update_mask, "Reason"] = final_report.loc[update_mask, "FLAG"].astype(str).map(lambda f: fmap.get(f, {}).get("reason", "1000007 - Other Reason"))
                                    
                                    _rej_in_app = update_mask.sum()
                                    st.write(f"App validation found {_rej_in_app} additional rejections.")

                        # ── Seed from the platform's own verdict table ──────
                        #
                        # PIM_QC_Result.xlsx already resolved every SID to a
                        # Status/Reason/Comment. Two things are taken from it,
                        # and deliberately only two:
                        #
                        #   1. SIDs the app never produced a row for. The 18
                        #      "Incomplete SKU" products land here — they exist
                        #      in the platform's table as Manual Review but are
                        #      absent from the Complete CSV the checks run on,
                        #      so without this they are silently missing from
                        #      the report entirely.
                        #   2. ParentSKU, where the app has none.
                        #
                        # The app's own Status is NOT overwritten. Re-deriving
                        # the verdict independently is the whole point of
                        # Targeted Audit: on this batch 67 of 74 of the
                        # platform's colour rejections sit on rows with
                        # misaligned fields, and trusting its verdict wholesale
                        # would pass all of them straight through.
                        _pim_verdicts = st.session_state.get("zip_pim_verdicts")
                        if isinstance(_pim_verdicts, pd.DataFrame) and not _pim_verdicts.empty \
                                and "ProductSetSid" in _pim_verdicts.columns:
                            _pv = _pim_verdicts.copy()
                            _pv["ProductSetSid"] = _pv["ProductSetSid"].astype(str).str.strip()
                            _pv = _pv[_pv["ProductSetSid"].ne("")].drop_duplicates(
                                subset=["ProductSetSid"], keep="last"
                            )
                            _have = set(final_report["ProductSetSid"].astype(str).str.strip())
                            _missing = _pv[~_pv["ProductSetSid"].isin(_have)]
                            # Seed only SIDs the ZIP's own QC CSVs actually
                            # cover. In every real ZIP checked the verdict
                            # table and the QC CSVs describe exactly the same
                            # SID set (KE 804/805, UG 805: zero on either
                            # side), so this changes nothing for a ZIP that
                            # matches its batch — the 18 Incomplete SKUs this
                            # block exists for are in the Incomplete CSV and
                            # survive.
                            #
                            # What it stops is a verdict table that outlives
                            # the ZIP it came from being poured into an
                            # unrelated batch. zip_pim_verdicts is now cleared
                            # on reset, so this is the second line of defence
                            # rather than the only one, but it is the cheaper
                            # of the two to reason about: no ZIP loaded means
                            # nothing to seed, full stop.
                            if not _missing.empty and not qc_zip.empty:
                                _qc_sid_col = next(
                                    (c for c in ("PRODUCT_SET_SID", "ProductSetSid",
                                                 "Product Set SID", "cod_productset_sid", "SID")
                                     if c in qc_zip.columns),
                                    None,
                                )
                                if _qc_sid_col:
                                    _qc_covered = set(
                                        qc_zip[_qc_sid_col].astype(str).str.strip()
                                    )
                                    _dropped = int((~_missing["ProductSetSid"].isin(_qc_covered)).sum())
                                    if _dropped:
                                        logger.warning(
                                            "PIM_QC_Result lists %s SID(s) absent from both the "
                                            "batch and the ZIP's QC files — not seeded", _dropped,
                                        )
                                    _missing = _missing[_missing["ProductSetSid"].isin(_qc_covered)]
                            elif not _missing.empty:
                                logger.warning(
                                    "Discarding %s PIM_QC_Result verdict(s): no ZIP QC data is "
                                    "loaded for this batch", len(_missing),
                                )
                                _missing = _missing.iloc[0:0]
                            if not _missing.empty:
                                _add = pd.DataFrame({
                                    "ProductSetSid": _missing["ProductSetSid"].values,
                                    "Status": _missing.get("Status", pd.Series(dtype=str)).fillna("Manual review").values,
                                    "FLAG": "Manual review",
                                    "Comment": _missing.get("Comment", pd.Series(dtype=str)).fillna("").values,
                                    "Reason": _missing.get("Reason", pd.Series(dtype=str)).fillna("").values,
                                    "Is_Zip": True,
                                    "Is_Manual": False,
                                })
                                _add["PRODUCT_SET_SID"] = _add["ProductSetSid"]
                                final_report = pd.concat(
                                    [final_report, _add], ignore_index=True
                                )
                                logger.info(
                                    "Seeded %s SID(s) from PIM_QC_Result that the "
                                    "checks never saw", len(_add),
                                )
                            # Keep the platform's verdict alongside ours so the
                            # audit can measure disagreement rather than guess.
                            st.session_state["_platform_verdict"] = dict(
                                zip(_pv["ProductSetSid"], _pv.get("Status", ""))
                            )

                        _parent_map = data.set_index("PRODUCT_SET_SID")["PARENTSKU"].to_dict() if "PARENTSKU" in data.columns else {}
                        _seller_map = data.set_index("PRODUCT_SET_SID")["SELLER_NAME"].to_dict() if "SELLER_NAME" in data.columns else {}
                        final_report["ParentSKU"] = final_report["ProductSetSid"].astype(str).str.strip().map(_parent_map).fillna("")
                        # Backfill ParentSKU from the verdict table where the
                        # product data had none. Same values in this batch, but
                        # it covers the seeded rows, which are not in `data`.
                        if isinstance(_pim_verdicts, pd.DataFrame) and "ParentSKU" in getattr(_pim_verdicts, "columns", []):
                            _pim_parent = dict(zip(
                                _pim_verdicts["ProductSetSid"].astype(str).str.strip(),
                                _pim_verdicts["ParentSKU"].astype(str).str.strip(),
                            ))
                            _blank = final_report["ParentSKU"].astype(str).str.strip().eq("")
                            if _blank.any():
                                final_report.loc[_blank, "ParentSKU"] = (
                                    final_report.loc[_blank, "ProductSetSid"]
                                    .astype(str).str.strip().map(_pim_parent).fillna("")
                                )
                        final_report["SellerName"] = final_report["ProductSetSid"].astype(str).str.strip().map(_seller_map).fillna("")
                        st.session_state.post_qc_results = combined_results

                        if _manual_approvals:
                            _ma_mask = final_report["ProductSetSid"].astype(str).str.strip().isin(_manual_approvals)
                            if _ma_mask.any(): final_report.loc[_ma_mask, ["Status", "Reason", "Comment", "FLAG", "Is_Manual", "Is_Zip"]] = ["Approved", "", "", "Approved by User", True, False]

                        # Re-apply decisions journalled by an earlier session for
                        # this same file set. Runs after _manual_approvals (which
                        # only carries approvals within a live session) because the
                        # journal also covers manual rejections, and is written on
                        # every decision so it is never staler. Applied before the
                        # parquet save below so the checkpoint includes them too.
                        _restored = apply_manual_decisions(
                            final_report, load_manual_decisions(process_signature)
                        )
                        if _restored:
                            st.write(f"Restored {_restored} manual decision(s) from a previous session.")

                        st.session_state.final_report = final_report
                        st.session_state.all_data_map = data
                        st.session_state.all_data_rows = None 
                        st.session_state._data_filtered_ref = data_filtered
                        st.session_state.last_processed_files = process_signature

                        save_df_parquet(data, f"{sig_hash}_data.parquet")
                        save_df_parquet(data_filtered, f"{sig_hash}_data_rows.parquet")
                        save_df_parquet(final_report, f"{sig_hash}_report.parquet")
                        st.session_state.current_sig_hash = sig_hash
                        _prepare_lazy_zip_images(_files_for_processing)

                        try:
                            from constants import GRID_COLS
                            _available_cols = [c for c in GRID_COLS if c in data.columns]
                            if "CATEGORY_CODE" in data.columns and "CATEGORY_CODE" not in _available_cols: _available_cols.append("CATEGORY_CODE")
                            _valid_df = final_report[final_report["Status"] == "Approved"][["ProductSetSid"]]
                            _review_data = pd.merge(_valid_df, data[_available_cols], left_on="ProductSetSid", right_on="PRODUCT_SET_SID", how="left")
                            _code_to_path = support_files.get("code_to_path", {})
                            if _code_to_path and "CATEGORY_CODE" in _review_data.columns:
                                _review_data["CATEGORY"] = _review_data["CATEGORY_CODE"].apply(lambda c: _code_to_path.get(str(c).strip(), str(c)) if pd.notna(c) else "")
                            st.session_state["_grid_review_data_cache"] = _review_data
                            _warm_urls: set = set()
                            if "MAIN_IMAGE" in _review_data.columns:
                                for _url in _review_data.iloc[: 50 * 2]["MAIN_IMAGE"].astype(str):
                                    _url = _url.strip().replace("http://", "https://", 1)
                                    if _url.startswith("https"): _warm_urls.add(_url)
                            st.session_state["_grid_warm_urls"] = list(_warm_urls)
                        except Exception as _pw_err:
                            logger.warning("Grid pre-warm failed: %s", _pw_err)

                        _rej_count = int(final_report[final_report["Status"] == "Rejected"].shape[0])
                        _app_count = int(final_report[final_report["Status"] == "Approved"].shape[0])
                        _status.update(label=f"Done — {_app_count:,} approved, {_rej_count:,} rejected", state="complete", expanded=False)

            except Exception as e:
                logger.exception("Processing error while validating uploaded file(s)")
                st.error(f"Something went wrong while processing your file(s): {e}\n\nTry re-uploading the file, or contact support if this keeps happening.")
                with st.expander("Technical details (for support)", expanded=False, type="compact"):
                    st.code(traceback.format_exc())
                st.session_state.last_processed_files = "error"


# ── Carry decisions forward when the upload grows ──────────────────────────
# Adding the image ZIP to a batch already under review changes the file set, so
# the journal written during that review is keyed under a signature nothing
# looks up again — the decisions are on disk and unreachable, and the run looks
# like it reset. This finds a journal written for a strict subset of what is
# now uploaded and offers it back.
#
# Deliberately an offer, not an automatic merge. Re-applying yesterday's
# verdicts onto a report the reviewer has not looked at yet is not something to
# do silently, and the counts below are the only chance to notice that a
# journal is older or larger than expected before it lands.
if (
    st.session_state.get("last_processed_files") == process_signature
    and process_signature != "empty"
    and st.session_state.get("_predecessor_handled") != process_signature
    and "_predecessor_offer" not in st.session_state
):
    try:
        _pred = find_predecessor_decisions(
            st.session_state.get("_process_file_tokens"),
            st.session_state.get("_process_country", ""),
            process_signature,
        )
        if _pred:
            _pred["preview"] = preview_decision_merge(
                st.session_state.get("final_report"), _pred["decisions"]
            )
            # Nothing of ours survives in this report, so there is nothing to
            # offer and no reason to interrupt.
            if _pred["preview"]["matched"] > 0:
                st.session_state._predecessor_offer = _pred
            else:
                st.session_state._predecessor_handled = process_signature
        else:
            st.session_state._predecessor_handled = process_signature
    except Exception:
        # Never let recovery break the run it is trying to protect.
        logger.exception("Predecessor decision lookup failed")
        st.session_state._predecessor_handled = process_signature

_offer = st.session_state.get("_predecessor_offer")
if _offer:
    _pv = _offer["preview"]
    _added = ", ".join(t.rsplit(".", 1)[0][:40] for t in _offer.get("added", [])) or "new file(s)"
    _age = time.time() - (_offer.get("saved_at") or time.time())
    _ago = (f"{int(_age // 86400)}d ago" if _age >= 86400 else
            f"{int(_age // 3600)}h ago" if _age >= 3600 else
            f"{int(_age // 60)}m ago" if _age >= 60 else "just now")
    with st.container(border=True):
        st.warning(
            f"**{_pv['total']:,} manual decisions found from an earlier run of this batch** "
            f"({_ago}). This upload adds {len(_offer.get('added', []))} file(s).",
            icon=":material/history:",
        )
        _m1, _m2, _m3 = st.columns(3)
        _m1.metric("Will be re-applied", f"{_pv['matched']:,}")
        _m2.metric("No longer in report", f"{_pv['missing']:,}",
                   help="Products decided earlier that this upload no longer contains. They are skipped.")
        _m3.metric("Differ from current", f"{_pv['conflicts']:,}",
                   help="Rows where your earlier decision disagrees with what validation just produced — "
                        "including anything the newly added file flagged. Your decision wins.")
        if _pv["conflicts"]:
            st.caption(
                f"Your earlier decision overrides validation on {_pv['conflicts']:,} row(s). "
                "Anything the new file flagged there will be overwritten by what you already chose."
            )
        _b1, _b2 = st.columns([1, 1])
        if _b1.button(f"Re-apply {_pv['matched']:,} decisions", type="primary",
                      width="stretch", key="pred_apply"):
            _n = apply_manual_decisions(st.session_state.final_report, _offer["decisions"])
            checkpoint_final_report(st.session_state.final_report)
            st.session_state._predecessor_handled = process_signature
            st.session_state.pop("_predecessor_offer", None)
            st.toast(f"Re-applied {_n:,} decision(s)", icon=":material/history:")
            st.rerun()
        if _b2.button("Start fresh", width="stretch", key="pred_skip",
                      help="Keep validation's results. The earlier decisions stay on disk."):
            st.session_state._predecessor_handled = process_signature
            st.session_state.pop("_predecessor_offer", None)
            st.rerun()


@st.fragment
def handle_jtbridge():
    _bridge_val = st.text_input(
        "jtbridge",
        value="",
        placeholder="JTBRIDGE_UNIQUE_DO_NOT_USE",
        key=f"main_bridge_{st.session_state.get('main_bridge_counter', 0)}",
        label_visibility="collapsed",
    )

    if _bridge_val:
        try:
            _msg = json.loads(_bridge_val)
            if _msg.get("action") == "reject_comments":
                _ac = _msg.get("payload", {})
                if isinstance(_ac, dict):
                    if "pending_auto_comments" not in st.session_state: st.session_state.pending_auto_comments = {}
                    st.session_state.pending_auto_comments.update(_ac)
            elif _msg.get("action") == "reject":
                _payload = _msg.get("payload", {})
                _auto_comments = st.session_state.pop("pending_auto_comments", {})
                if isinstance(_payload, dict) and _payload:
                    _rgroups = {}
                    for _sid, _rkey in _payload.items(): _rgroups.setdefault(_rkey, []).append(_sid)
                    _total = 0
                    for _rkey, _sids in _rgroups.items():
                        if _rkey.startswith("Other Reason (Custom): "):
                            _flag = "Other Reason (Custom)"
                            _code = "1000007 - Other Reason"
                            _cmt = _rkey.split(": ", 1)[1]
                        else:
                            _IMAGE_FLAG_FALLBACK = {"REJECT_IMG_STRETCHED": "Image Stretched", "REJECT_IMG_BLURRY": "Image Blurry", "REJECT_IMG_MISMATCH": "Image Mismatch", "REJECT_IMG_INFRINGING": "Image Infringing", "REJECT_IMG_TOO_MANY": "Image Too Many things displayed"}
                            _flag = REASON_MAP.get(_rkey) or _IMAGE_FLAG_FALLBACK.get(_rkey, "Other Reason (Custom)")
                            _rinfo = support_files["flags_mapping"].get(_flag, {"reason": "1000007 - Other Reason", "en": "Manual rejection"})
                            _code = _rinfo["reason"]
                            _cmt_lang = "fr" if st.session_state.selected_country == "Morocco" else "en"
                            _cmt = _rinfo.get(_cmt_lang, _rinfo.get("en"))
                        # --- Performance fix: group by comment so we call apply_status_change
                        #     once per unique (reason, comment) pair instead of once per SID.
                        #     This eliminates the N+1 DataFrame-copy / GC / column-scan pattern.
                        _sids_by_comment: dict = {}
                        for _sid in _sids:
                            _sid_cmt = _auto_comments.get(_sid, _cmt)
                            _sids_by_comment.setdefault(_sid_cmt, []).append(_sid)
                        for _cmt_val, _sid_group in _sids_by_comment.items():
                            apply_status_change(_sid_group, status="Rejected", reason=_code, comment=_cmt_val, flag=_flag, is_manual=True, is_zip=False)
                        _total += len(_sids)
                    st.session_state.main_toasts.append(f"Rejected {_total} product(s)")
                    st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                    st.session_state.do_scroll_top = False
                    st.rerun()
            elif _msg.get("action") == "undo":
                _payload = _msg.get("payload", {})
                _total_restored = 0
                if isinstance(_payload, dict):
                    for _sid in _payload.keys():
                        restore_single_item(_sid)
                        _total_restored += 1
                if _total_restored > 0:
                    st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                    st.session_state.do_scroll_top = False
                    st.rerun()
            elif _msg.get("action") == "grid_sort_issue":
                st.session_state.grid_sort_issue = _msg.get("payload", "")
                st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                st.rerun()
            elif _msg.get("action") == "grid_filter_flag":
                st.session_state.grid_filter_flag = _msg.get("payload", "")
                st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                st.rerun()
            elif _msg.get("action") == "grid_cols_per_row":
                # Clamped to the range the buttons actually offer: this value
                # drives the grid's CSS column count and the wide-dialog
                # threshold, so a bogus payload would produce an unusable
                # layout rather than an error.
                try:
                    st.session_state.grid_cols_per_row = max(3, min(7, int(_msg.get("payload", 4))))
                except (ValueError, TypeError):
                    st.session_state.grid_cols_per_row = 4
                st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                st.rerun()
        except Exception as _e:
            logger.error(f"Bridge parse error: {_e}")


@st.cache_data(show_spinner=False)
def get_enriched_results(fr_df, data_df):
    if fr_df.empty: return pd.DataFrame()
    return pd.merge(fr_df, data_df[["PRODUCT_SET_SID", "SELLER_NAME", "BRAND"]], left_on="ProductSetSid", right_on="PRODUCT_SET_SID", how="left")


@st.cache_data(show_spinner=False, hash_funcs={pd.DataFrame: df_hash})
def _build_dashboard_figures(fr_meta: pd.DataFrame, manual_hours: float):
    """Builds the dashboard's aggregates/plotly figures. Cached (keyed on the cheap
    df_hash signature, not a full-object hash) so repeated fragment reruns — e.g. a
    keystroke in Quick SID Lookup — don't rebuild a groupby + 5 charts every time
    the underlying data hasn't actually changed."""
    app_df = fr_meta[fr_meta["Status"] == "Approved"]
    rej_df = fr_meta[fr_meta["Status"] == "Rejected"]

    fig_bar = None
    if not app_df.empty or not rej_df.empty:
        cat_stats = fr_meta.groupby("BRAND")["Status"].value_counts(normalize=True).unstack().fillna(0)
        if "Approved" in cat_stats.columns:
            top_cats = cat_stats.sort_values("Approved", ascending=False).head(10)
            fig_bar = px.bar(top_cats, y=top_cats.index, x="Approved", orientation="h", title="Top 10 Brands by Approval Rate", labels={"Approved": "Approval Rate"}, color_discrete_sequence=[JUMIA_COLORS["success_green"]])
            fig_bar.update_layout(height=300, margin=dict(t=30, l=10, r=10, b=10))

    mix_df = pd.DataFrame({"Status": ["Approved", "Rejected"], "Count": [len(app_df), len(rej_df)]})
    fig_mix = px.pie(mix_df, values="Count", names="Status", hole=0.5, color="Status", color_discrete_map={"Approved": "#22c55e", "Rejected": "#ef4444"}, title="Validation Mix")
    fig_mix.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0), height=280)

    fig_flags = None
    if not rej_df.empty:
        flag_counts = rej_df["FLAG"].value_counts().head(8).reset_index()
        flag_counts.columns = ["Flag", "Count"]
        fig_flags = px.bar(flag_counts, x="Count", y="Flag", orientation="h", title="Top Issues Breakdown", color="Count", color_continuous_scale="Reds")
        fig_flags.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0), height=280, yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)

    fig_seller = None
    if not rej_df.empty:
        seller_rej = rej_df["SELLER_NAME"].value_counts().head(10).reset_index()
        seller_rej.columns = ["Seller", "Rejections"]
        fig_seller = px.bar(seller_rej, x="Rejections", y="Seller", orientation="h", title="Sellers at Risk (Rejection Count)", color="Rejections", color_continuous_scale="Oranges")
        fig_seller.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=300, yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)

    fig_savings = go.Figure(go.Indicator(mode="gauge+number", value=manual_hours, title={"text": "Hours Saved (Estimate)", "font": {"size": 14}}, gauge={"axis": {"range": [None, max(manual_hours * 1.5, 10)]}, "bar": {"color": "#00e5b0"}, "steps": [{"range": [0, manual_hours * 0.5], "color": "rgba(0,229,176,0.1)"}, {"range": [manual_hours * 0.5, manual_hours], "color": "rgba(0,229,176,0.2)"}]}))
    fig_savings.update_layout(height=300, margin=dict(t=60, b=20, l=30, r=30))

    return len(app_df), len(rej_df), fig_bar, fig_mix, fig_flags, fig_seller, fig_savings

@st.fragment
def render_main_results():
    if not _files_for_processing or st.session_state.final_report.empty or st.session_state.file_mode == "post_qc":
        return

    fr = st.session_state.final_report
    data = st.session_state.all_data_map
    fr_meta = get_enriched_results(fr, data)
    if fr_meta.empty: return

    app_df = fr_meta[fr_meta["Status"] == "Approved"]
    rej_df = fr_meta[fr_meta["Status"] == "Rejected"]

    st.header(_t("val_results"), anchor=False)
    st.markdown('<div class="dashboard-marker"></div>', unsafe_allow_html=True)

    # "Another seller has this exact photo" — raised here so it is seen after
    # a rejection made anywhere, including from inside the visual grid, whose
    # dialog closes on the rerun that follows an action.
    render_sibling_prompt()

    # The KPI strip is the reviewer's orientation — how big is this batch and
    # how bad is it. It used to sit inside the collapsed dashboard expander,
    # one click away and hidden by default, while the six Plotly charts (the
    # analytical deep-dive, not the orientation) had equal billing. Swapped:
    # KPIs always on, charts behind the disclosure.
    render_summary_header(fr)
    # Only renders when something has actually been waived, so it costs a dict
    # lookup on a normal run.
    render_override_history()

    total_count = len(fr)
    auto_count = len(fr[fr["FLAG"] != "Manual review"])
    manual_hours = (auto_count * 3) / 60

    with st.expander(_t("dashboard"), expanded=False):
        app_n, rej_n, fig_bar, fig_mix, fig_flags, fig_seller, fig_savings = _build_dashboard_figures(fr_meta, manual_hours)
        g1, g2 = st.columns(2)
        with g1:
            if rej_n: render_rejection_donut(fr)
            else: st.info("No rejections to visualize.")
        with g2:
            if fig_bar is not None:
                st.plotly_chart(fig_bar, width='stretch', config={"displayModeBar": False})
        with g1:
            st.plotly_chart(fig_mix, width='stretch', config={"displayModeBar": False})
        with g2:
            if fig_flags is not None:
                st.plotly_chart(fig_flags, width='stretch', config={"displayModeBar": False})
            else: st.info("No rejections to visualize.")

        s1, s2 = st.columns(2)
        with s1:
            if fig_seller is not None:
                st.plotly_chart(fig_seller, width='stretch', config={"displayModeBar": False})
        with s2:
            st.plotly_chart(fig_savings, width='stretch', config={"displayModeBar": False})

    # This was a fixed-position HTML toast with an UNDO button that posted a
    # message Streamlit never listened for, plus a real button labelled
    # "Internal Undo" sitting in the page flow underneath it — so the reviewer
    # saw a floating toast with a dead control and a stray debug button below.
    # One real control now, in the flow, saying what it does.
    ut = st.session_state.get("show_undo_toast")
    if ut and (datetime.now() - ut["time"]).seconds < 15:
        _u1, _u2 = st.columns([4, 1], gap="medium", vertical_alignment="center")
        with _u1:
            _n = ut["count"]
            st.info(
                f"{ut['status']}d {_n:,} {'product' if _n == 1 else 'products'}.",
                icon=":material/history:",
            )
        with _u2:
            if st.button(
                f"Undo {ut['status'].lower()}",
                key="undo_trigger",
                type="secondary",
                width="stretch",
                disabled="undo_snapshot" not in st.session_state,
            ):
                st.session_state.final_report = st.session_state.undo_snapshot["final_report"]
                # Undo bypasses apply_status_change, so checkpoint here too —
                # otherwise disk keeps the state the user just undid.
                checkpoint_final_report(st.session_state.final_report)
                st.session_state.pop("show_undo_toast", None)
                st.toast(f"Restored {ut['count']:,} products", icon=":material/undo:")
                st.rerun()

    lookup_col1, lookup_col2 = st.columns([2, 1])
    with lookup_col1:
        search_sid = st.text_input("Quick SID Lookup", placeholder="Paste SID here to view details...", key="global_sid_lookup")

    if search_sid:
        sid_match = data[data["PRODUCT_SET_SID"].astype(str).str.strip() == search_sid.strip()]
        if not sid_match.empty:
            with st.expander(f"Quick View: {search_sid}", expanded=True):
                r = sid_match.iloc[0]
                v_cols = st.columns([1, 2])
                with v_cols[0]:
                    img_data = _get_image_from_zip(r.get("NAME", ""), r.get("BRAND", ""), r.get("MAIN_IMAGE", ""))
                    if img_data: st.image(img_data)
                    else: st.warning("No Image")
                with v_cols[1]:
                    st.write(f"**Name:** {r.get('NAME')}")
                    st.write(f"**Brand:** {r.get('BRAND')}")
                    # Normalize the same way as the `data` lookup above (str + strip)
                    # so a type/whitespace mismatch in ProductSetSid can't crash this
                    # with an IndexError from .iloc[0] on an empty match.
                    _fr_match = fr[fr["ProductSetSid"].astype(str).str.strip() == search_sid.strip()]
                    _status_display = _fr_match["Status"].iloc[0] if not _fr_match.empty else "Unknown (not found in report)"
                    st.write(f"**Current Status:** {_status_display}")

                    if _status_display == "Rejected" and not _fr_match.empty:
                        _rej_row = _fr_match.iloc[0]
                        _flag = str(_rej_row.get("FLAG", "")).strip()
                        _reason_code = str(_rej_row.get("Reason", "")).strip()
                        _comment = str(_rej_row.get("Comment", "")).strip()
                        st.error(f"**Rejection Reason:** {_flag or 'Unknown'}")
                        if _reason_code and _reason_code.lower() not in ("nan", "none", ""):
                            st.caption(f"Reason Code: {_reason_code}")
                        if _comment and _comment.lower() not in ("nan", "none", "", "manual rejection", "rejected"):
                            st.write(f"**Details:** {_comment}")

                    if st.button("Approve Now", key="quick_app"):
                        apply_status_change([search_sid], status="Approved", flag="Manual Quick Approve")
                        st.rerun()

    st.subheader(_t("flags_breakdown"), anchor=False)
    group_by_seller = st.toggle("Group by Seller", help="Toggle to group flagged products by seller instead of flag")


    _blurry_commentary = st.session_state.get("_image_blurry_commentary", {})
    if _blurry_commentary:
        # One pass to find the approved SIDs instead of a full report scan per
        # advisory SID (`fr[fr["ProductSetSid"] == sid]` inside a comprehension).
        _approved_sids = set(
            fr.loc[fr["Status"] == "Approved", "ProductSetSid"].astype(str)
        )
        _commentary_in_scope = {
            sid: comment
            for sid, comment in _blurry_commentary.items()
            if str(sid) in _approved_sids
        }
    else:
        _commentary_in_scope = {}
    if _commentary_in_scope:
        with st.expander(f"Low Resolution Advisory — {len(_commentary_in_scope)} product(s) (not rejected)", expanded=False):
            st.info("These products have images between 201–249px. They have not been rejected, but image quality could be improved. Products ≤200px are automatically rejected as Image Blurry.")
            # Likewise: one indexed lookup table rather than a scan of `data` per row.
            _adv_cols = [c for c in ("NAME", "SELLER_NAME") if c in data.columns]
            _adv_dedup = data.drop_duplicates(subset=["PRODUCT_SET_SID"])
            _adv_lookup = dict(zip(
                _adv_dedup["PRODUCT_SET_SID"].astype(str),
                _adv_dedup[_adv_cols].to_dict("records"),
            )) if _adv_cols else {}
            _advisory_rows = []
            for _sid, _comment in _commentary_in_scope.items():
                _info = _adv_lookup.get(str(_sid))
                if _info:
                    _advisory_rows.append({"PRODUCT_SET_SID": _sid, "NAME": _info.get("NAME", ""), "SELLER_NAME": _info.get("SELLER_NAME", ""), "Resolution Note": _comment})
            if _advisory_rows: st.dataframe(pd.DataFrame(_advisory_rows), hide_index=True, width='stretch')

    if not rej_df.empty:
        if group_by_seller:
            # Count every seller's products once, up front. Doing this as
            # `len(data[data["SELLER_NAME"] == seller])` inside the loop is a
            # full scan of `data` per seller.
            _seller_totals = data["SELLER_NAME"].value_counts() if "SELLER_NAME" in data.columns else pd.Series(dtype=int)
            for seller in sorted(rej_df["SELLER_NAME"].unique()):
                df_seller = rej_df[rej_df["SELLER_NAME"] == seller]
                seller_flags = df_seller["FLAG"].unique()
                with st.expander(f"Seller: {seller} ({len(df_seller)} items, {len(seller_flags)} flags)"):
                    wrong_cat_count = len(df_seller[df_seller["FLAG"] == "Wrong Category"])
                    total_seller_items = int(_seller_totals.get(seller, 0))
                    wrong_cat_pct = ((wrong_cat_count / total_seller_items * 100) if total_seller_items > 0 else 0)
                    if wrong_cat_pct >= 40:
                        st.warning(f"High Error Rate: {wrong_cat_pct:.1f}% of this seller's products have wrong categories.")
                        sc1, sc2 = st.columns(2)
                        if sc1.button(f"Approve All for {seller[:15]}", key=f"app_sel_{seller}"):
                            apply_status_change(df_seller["ProductSetSid"].tolist(), status="Approved")
                            st.rerun()
                        if sc2.button(f"Reject All for {seller[:15]}", key=f"rej_sel_{seller}"):
                            apply_status_change(df_seller["ProductSetSid"].tolist(), status="Rejected", flag="Bulk Seller Reject")
                            st.rerun()
                    render_flag_expander(f"Seller: {seller}", df_seller, data, all(c in data.columns for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]), support_files, country_validator, cached_validate_products)
        else:
            # Severity first, then alphabetical inside each bucket. A flat
            # alphabetical list put a legal blocker below a cosmetic flag
            # purely on the initial letter of its internal name.
            _flags_list = sorted(rej_df["FLAG"].unique(), key=severity_sort_key)

            _grouped = {}
            for _f in _flags_list:
                _grouped.setdefault(flag_severity(_f), []).append(_f)

            _has_warranty_cols = all(
                c in data.columns for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]
            )

            for _level in SEVERITY_ORDER:
                _titles = _grouped.get(_level)
                if not _titles:
                    continue

                _group_skus = int(rej_df["FLAG"].isin(_titles).sum())
                render_severity_group_header(_level, len(_titles), _group_skus)

                for _i, title in enumerate(_titles):
                    df_flagged = rej_df[rej_df["FLAG"] == title]
                    is_zip = "(Prefetched)" in title
                    # The expander summary is what a reviewer scans, so it
                    # carries the readable label and the severity mark. The
                    # raw FLAG stays the key for state and exports.
                    # flag_label() strips the "(Prefetched)" suffix, which is
                    # part of the raw FLAG and was previously visible in the
                    # expander title. Renaming the checks was not meant to
                    # remove the provenance, so it goes back on explicitly:
                    # a reviewer needs to see at a glance which findings came
                    # from the QC system's own ZIP and which this tool ran.
                    exp_label = (
                        f"{SEVERITY[_level]['mark']}  {len(df_flagged):,}"
                        f"   {flag_label(title)}"
                    )
                    if is_zip:
                        exp_label += "  (Prefetched)  ⚡ ZIP"

                    # ZIP/prefetched flags keep their orange treatment — it was
                    # doing real work telling the two sources apart. It comes
                    # from a keyed container now rather than a MutationObserver
                    # painting !important styles over the stylesheet, so it
                    # composes with the severity spine instead of erasing it.
                    _row = st.container(
                        key=f"flagrow_{'zip' if is_zip else 'std'}_{_level}_{_i}"
                    )
                    with _row:
                        with st.expander(exp_label, expanded=st.session_state.get(f"exp_{title}", False)):
                            st.html(flag_pill_header(title, len(df_flagged), is_zip=is_zip))
                            render_flag_expander(title, df_flagged, data, _has_warranty_cols, support_files, country_validator, cached_validate_products)
    else:
        st.success("All products passed validation — no rejections found.")

    render_manual_review_buttons(support_files)
    render_image_grid(support_files)
    render_exports_section(support_files, country_validator)


# Fill the rail reserved at the top of the page. It runs last because only
# now are the country, the upload set and the report all known — Streamlit
# renders a container where it was created, not where it was written to.
with _rail_slot:
    _rail_fr = st.session_state.get("final_report", pd.DataFrame())
    render_context_rail(
        country=st.session_state.get("selected_country", ""),
        flag_src=_flag_b64.get(st.session_state.get("selected_country", ""), ""),
        logo_html=logo_html,
        file_count=len(_files_for_processing),
        sku_count=len(_rail_fr),
        rejected_count=(
            int((_rail_fr["Status"] == "Rejected").sum())
            if not _rail_fr.empty and "Status" in _rail_fr.columns
            else 0
        ),
    )

render_main_results()

st.markdown('''
<style>
div[data-testid="stTextInput"]:has(input[placeholder="LANG_BRIDGE_DO_NOT_USE"]) {
    display: none !important;
}
</style>
''', unsafe_allow_html=True)

_lang_bridge_val = st.text_input("lang_bridge", value="", placeholder="LANG_BRIDGE_DO_NOT_USE", key=f"lang_bridge_{st.session_state.get('lang_bridge_counter', 0)}", label_visibility="collapsed")
if _lang_bridge_val:
    try:
        _msg = json.loads(_lang_bridge_val)
        if _msg.get("action") == "change_lang":
            new_lang = _msg.get("payload")
            if new_lang and new_lang in LANGUAGES.values():
                st.session_state.ui_lang = new_lang
                st.session_state.lang_bridge_counter = st.session_state.get("lang_bridge_counter", 0) + 1
                st.rerun()
    except Exception as e:
        logger.error(f"Lang bridge error: {e}")

handle_jtbridge()