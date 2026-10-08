"""
main.py - Main Streamlit Application Entry Point
"""

import base64
import concurrent.futures
import gc
import hashlib
import json
import logging
import os
import pickle
import re
import shutil
import time
import traceback
import unicodedata
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


try:
    from streamlit.runtime.scriptrunner_utils.script_run_context import add_script_run_ctx, get_script_run_ctx
except ImportError:
    try:
        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
    except ImportError:
        add_script_run_ctx, get_script_run_ctx = None, None


def _run_with_ctx(ctx, fn, *args, **kwargs):
    if ctx and add_script_run_ctx:
        try:
            import threading
            add_script_run_ctx(threading.current_thread(), ctx)
        except Exception:
            pass
    return fn(*args, **kwargs)


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
    ASPECT_ADVISORY_TALL, ASPECT_ADVISORY_WIDE,
    ASPECT_REJECT_TALL, ASPECT_REJECT_WIDE,
    COLOR_VARIANT_TO_BASE,
    COUNTRY_VALIDATOR_CONFIG,
    FLAG_CACHE_DIR,
    GRID_COLS,
    JUMIA_COLORS,
    PARQUET_CACHE_DIR,
    REASON_MAP,
    SNEAKER_BRAND_ALIASES,
    PRICE_CEILING_MODEL_ALIASES,
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
    manual_decisions_mtime,
    preview_decision_merge,
    propagate_metadata,
    save_df_parquet,
    standardize_input_data,
    validate_input_schema,
    normalize_text,
)
from custom_country_rules import (
    check_kebs_banned_products, load_kebs_hb_codes, check_kebs_fda, check_invalid_brand_name,
    check_animal_health_banned_products, check_animal_health_prohibited_products,
    check_animal_health_banned_category, check_animal_health_prohibited_category,
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
from refurbished_rules import check_refurbished_products, check_out_of_market_devices
from translations import LANGUAGES, get_translation
import importlib
import targeted_audit as _ta_mod
try:
    importlib.reload(_ta_mod)
except Exception:
    pass
import ui_components as _ui_mod
try:
    importlib.reload(_ui_mod)
except Exception:
    pass

from ui_components import (
    apply_status_change,
    checkpoint_final_report,
    _clear_result_caches,
    flag_pill_header,
    render_context_rail,
    render_exports_section,
    render_flag_expander,
    render_image_grid,
    render_grid_closing_overlay,
    end_grid_closing_overlay,
    render_sibling_prompt,
    render_manual_review_buttons,
    render_rejection_donut,
    render_severity_group_header,
    render_summary_header,
    render_override_history,
    register_learning_commit,
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

from constants import (
    PREFETCH_MAP,
    NAME_BRAND_SUB_FLAGS,
    TITLE_LANGUAGE_SUB_FLAGS,
    _prefetch_key_from_status_col,
)

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
    "restricted_keyword": [
        "Restricted_Keyword_Reason",
        "Restricted_Block_Hits",
        "Restricted_Keyword_Status",
    ],
}
PROCESSING_CACHE_VERSION = "prefetch_context_v7"  # bumped: potential restricted brand validation added
PREFETCH_VALIDATOR_SKIP_MAP = {
    "category_check": ["Wrong Category", "Category Check"],
    "warranty_check": ["Product Warranty", "Warranty Check"],
    "fda_check": ["FDA"],
    "color_check": ["Missing COLOR", "Color Check", "Color Mismatch: Title vs COLOR Column"],
    "variation_check": ["Wrong Variation", "Variation Check"],
    "product_name_brand_name": [
        "BRAND name repeated in NAME", "Product Name Brand Name",
        *NAME_BRAND_SUB_FLAGS,
    ],
    "brand_image_check": ["Brand Image Check"],
    "restricted_keyword": ["Prohibited products", "Restricted Keywords"],
    "title_language_check": [
        "Missing Weight/Volume", "Incomplete Smartphone Name", "Title Language Check",
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
            if val and val.lower() not in ("nan", "none", "rejected", "block", "clean"):
                return val

    reason_col = re.sub(r"status$", "reason", str(status_col), flags=re.IGNORECASE)
    for candidate in (
        reason_col,
        reason_col.replace("_Status", "_Reason"),
        reason_col.replace("_status", "_reason"),
    ):
        if candidate in qc_columns:
            val = str(row.get(candidate, "")).strip()
            if val and val.lower() not in ("nan", "none", "rejected", "block", "clean"):
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

    if "refurbished" in low or "renewed" in low:
        return "Title Language Check \u2013 Refurbished Missing in Title"

    if (
        ("phone" in low or "tablet" in low or "laptop" in low or "smartphone" in low)
        and ("incomplete" in low or "spec" in low or "ram" in low or "storage" in low)
    ) or ("incomplete" in low and ("spec" in low or "ram" in low or "storage" in low)):
        return "Title Language Check \u2013 Incomplete Phone/Tablet/Laptop Title"

    if any(k in low for k in ("weight", "volume", "count", "quantity", "sold by")):
        return "Title Language Check \u2013 Missing Weight/Volume/Count"

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
    if current_flag == "Potential Restricted Brand" and "zip_override" in fr.columns:
        fr.loc[mask, "zip_override"] = "pot_restricted"
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
    "Potential Restricted Brand": ["NAME", "BRAND", "SELLER_NAME", "CATEGORY_CODE", "CATEGORY"],
    # NAME and the descriptions are listed because the price ceiling now looks
    # for the brand claim there too, not just in the BRAND field. This map also
    # scopes the per-flag cache digest.
    "Suspected Fake product": ["CATEGORY_CODE", "BRAND", "NAME", "GLOBAL_SALE_PRICE",
                               "GLOBAL_PRICE", "DESCRIPTION", "SHORT_DESCRIPTION"],
    "Out of market devices": ["NAME", "BRAND", "CATEGORY_CODE", "CATEGORY"],
    "Seller Not approved to sell Refurb": ["PRODUCT_SET_SID", "CATEGORY_CODE", "SELLER_NAME", "NAME"],
    "Product Warranty": ["PRODUCT_WARRANTY", "WARRANTY_DURATION", "CATEGORY_CODE"],
    "Seller Approve to sell books": ["CATEGORY_CODE", "SELLER_NAME"],
    "Seller Not Approved to Sell Alcohol": ["CATEGORY", "CATEGORY_CODE", "SELLER_NAME"],
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
    "Color Mismatch: Title vs COLOR Column": ["CATEGORY_CODE", "NAME", "COLOR"],
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
    "Product Name Too Short": ["NAME", "CATEGORY_CODE", "CATEGORY"],
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
FLAG_CACHE_KEY_VERSION = "fk5"

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
_IMAGE_THUMB_CACHE = _BoundedDict(maxsize=2500)
_IMAGE_DIM_LOCK = threading.Lock()
# Staging area for the low-resolution advisory produced by check_image_blurry.
# That check runs on a worker thread, where st.session_state writes are silently
# dropped; validate_products drains this into session_state on the main thread.
_IMAGE_BLURRY_COMMENTARY: dict = {}


def _compute_phash(img_bytes: bytes) -> str:
    try:
        import imagehash
        img = Image.open(BytesIO(img_bytes))
        if img.mode == "P":
            img = img.convert("RGBA")
        img = img.convert("RGB")
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
        if img.mode == "P":
            img = img.convert("RGBA")
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
    combined_index = {}
    source_bytes_list = []
    for uf in uploaded_file_records:
        if not uf["name"].lower().endswith(".zip"):
            continue
        try:
            with zipfile.ZipFile(BytesIO(uf["bytes"])) as zf:
                idx = _index_zip_images(zf)
            if idx:
                combined_index.update(idx)
                source_bytes_list.append(uf["bytes"])
        except Exception as e:
            logger.warning("Failed indexing ZIP images from %s: %s", uf["name"], e)
    st.session_state.zip_image_index = combined_index
    st.session_state.zip_image_source_bytes = source_bytes_list if source_bytes_list else None


# 🚀 OPTIMIZED: Double Download Eliminated
def _fetch_all_image_dimensions(data: pd.DataFrame, progress_callback=None) -> dict:
    """
    Download all unique images ONCE and cache. Both caches are filled 
    in a single network pass using session pooling. Thread-safe.
    """
    _image_t0 = time.perf_counter()
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
        st.session_state["validation_stage_timings"] = {
            **st.session_state.get("validation_stage_timings", {}),
            "Image download and pHash": 0.0,
        }
        return _IMAGE_DIM_CACHE

    # Thumbnail files are only needed for the first visible review window.
    # Generating and writing one for every image during validation added a
    # second full image-processing pass to large uploads. Cards can safely use
    # the original URL while a thumbnail is not cached.
    _thumb_url_limit = 200
    _thumbnail_urls = set(new_urls[:_thumb_url_limit])
    _thumbnail_zip_keys = {d[0] for d in zip_descriptors[:_thumb_url_limit]}

    def fetch(url):
        session = get_image_session()
        try:
            # Bumping timeout to 15s to handle internal CDNs like vendorcenter.jumia.com
            r = session.get(url.replace("http://", "https://"), timeout=15)
            if r.status_code == 200:
                raw = r.content
                size, ph = _size_and_phash(raw)
                thumb = thumbnail_data_uri(raw, url, 240) if url in _thumbnail_urls else ""
                return url, size, ph, thumb
        except Exception:
            pass
        # Keep the result shape identical to successful downloads. The image
        # prefetch collector stores (url, dimensions, phash, thumbnail); a
        # three-item failure tuple made one bad image abort the whole prefetch
        # pass with "expected 4, got 3".
        return url, None, "", ""

    def process_zip_img(tup):
        key, payload = tup
        try:
            if isinstance(payload, str) and payload.startswith("data:"):
                _, encoded = payload.split(",", 1)
                raw = base64.b64decode(encoded)
            else:
                raw = payload if isinstance(payload, (bytes, bytearray)) else b""
            size, ph = _size_and_phash(raw)
            thumb = thumbnail_data_uri(raw, key, 240) if key in _thumbnail_zip_keys else ""
            return key, size, ph, thumb
        except Exception:
            pass
        return key, None, "", ""

    results = []
    # Lowered thread concurrency limit to prevent socket exhaustion
    _img_workers = min(16, max(4, (os.cpu_count() or 4) * 2))
    if new_urls:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_img_workers) as executor:
            for _image_index, _image_result in enumerate(executor.map(fetch, new_urls), start=1):
                results.append(_image_result)
                if progress_callback and (_image_index == len(new_urls) or _image_index % 100 == 0):
                    progress_callback(f"downloaded {min(_image_index, len(new_urls)):,}/{len(new_urls):,} external images")

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
                if progress_callback:
                    progress_callback(f"decoded {min(_start + len(_slice), len(zip_descriptors)):,}/{len(zip_descriptors):,} ZIP images")
                # Dropped before the next chunk is built, so two chunks are
                # never alive at once.
                _payloads = None

    with _IMAGE_DIM_LOCK:
        for key, size, ph, thumb in results:
            # Record failures too. Without this, a URL that 404s or times out is
            # absent from the cache, so it lands in `new_urls` again on the next
            # run and is re-fetched with the full 6s timeout + 2 retries — every
            # single validation, forever.
            _IMAGE_DIM_CACHE[key] = size if size else None
            if ph:
                _IMAGE_HASH_CACHE[key] = ph
            if thumb:
                _IMAGE_THUMB_CACHE[key] = thumb

    # Publish a snapshot for ui_components, which needs the hashes to find the
    # same photo listed by another seller but cannot import this module (it is
    # the entry script, and streamlit_app already imports ui_components).
    # A dict copy of at most a few thousand short strings, once per validation.
    try:
        st.session_state["_image_phash_by_url"] = dict(_IMAGE_HASH_CACHE)
        st.session_state["_image_thumbnail_by_url"] = dict(_IMAGE_THUMB_CACHE)
        st.session_state["validation_stage_timings"] = {
            **st.session_state.get("validation_stage_timings", {}),
            "Image download and pHash": round(time.perf_counter() - _image_t0, 3),
        }
    except Exception:
        pass
    return _IMAGE_DIM_CACHE


# ── Blocked image fingerprint list ─────────────────────────────────────────
# Loaded from Image_learn.xlsx in the user's Downloads folder.  The file is
# read once per session; phashes are computed on first encounter and cached
# in Image_learn_cache.json next to the xlsx so subsequent runs are instant.

_BLOCKED_IMG_EXCEL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Image_learn.xlsx")
_BLOCKED_IMG_CACHE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Image_learn_cache.json")

# Valid FLAG values the Excel is allowed to specify (case-insensitive).
_BLOCKED_FLAG_MAP = {
    "poor images":                  "Poor images",
    "poor image":                   "Poor images",
    "image stretched":              "Image Stretched",
    "image blurry":                 "Image Blurry",
    "too many things":              "Image Too Many things displayed",
    "image too many things":        "Image Too Many things displayed",
    "image too many things displayed": "Image Too Many things displayed",
    "restricted brands":            "Restricted brands",
    "restricted brand":             "Restricted brands",
}


from learned_images import (
    _BLOCKED_IMG_EXCEL,
    _BLOCKED_IMG_CACHE,
    _BLOCKED_FLAG_MAP,
    _blocked_excel_mtime,
    load_blocked_image_fingerprints,
    get_blocked_image_matches,
    check_blocked_image_fingerprints,
    _learned_brands_agree,
)
from learned_rules import (
    merge_learned_image_rules,
    load_learned_image_rules,
    record_learned_image_matches_async,
    delete_learned_image_rules,
    restore_learned_image_rules,
    learn_image_rejections,
    learn_image_rejections_bulk,
    reconcile_image_rules,
)
from processing_automation import (
    load_manifest, mark_batch, completed_batches, file_signature,
    save_chunk_results, load_chunk_results, save_stage_frame, load_stage_frame, thumbnail_data_uri,
)



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


_NON_PHONE_KEYWORDS_RE = re.compile(
    r"\b(?:"
    r"heat\s*sealer|sealing\s*machine|packaging\s*machine|"
    r"power\s*bank|battery\s*pack|magsafe\s*battery|inverter|inerver|"
    r"solar\s*(?:lighting|system|charger|panel|lamp|bulb)|"
    r"rear\s*camera|selfie\s*camera|camera\s*lens|camera\s*module|"
    r"replacement\s*(?:screen|battery|camera|lcd|display)|"
    r"bill\s*counter|counterfeit\s*detector|"
    r"disco\s*dj|stage\s*light|magic\s*ball|crystal\s*magic|"
    r"weighing\s*scale|bathroom\s*scale|personal\s*scale|"
    r"subwoofer|home\s*audilo|home\s*audio|soundbar|speaker\s*system|"
    r"storage\s*box|plastic\s*tote|storage\s*bins?|stackable\s*plastic|"
    r"ab\s*roller|workout|exercise\s*wheel|"
    r"blender|kettle|toaster|cooker|vacuum\s*cleaner|"
    r"quadcopter|drone"
    r")\b"
    r"|^x6c$"
    ,
    re.IGNORECASE,
)


def _is_non_phone_product(name: str) -> bool:
    n = str(name).strip().lower()
    return bool(_NON_PHONE_KEYWORDS_RE.search(n))


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

    base_flagged = pd.DataFrame(columns=data.columns)

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
        except Exception as _e:
            logger.warning("check_wrong_category engine error: %s", _e)

    # 1. Non-phone products miscategorised in smartphone/tablet categories
    try:
        _sf = st.session_state.get("support_files", {})
        _phone_codes = set(clean_category_code(c) for c in _sf.get("smartphone_category_codes", []) if c)
        if _phone_codes and "CATEGORY_CODE" in data.columns and "NAME" in data.columns:
            _c_clean = data["_cat_clean"] if "_cat_clean" in data.columns else data["CATEGORY_CODE"].apply(clean_category_code)
            _in_phone = _c_clean.isin(_phone_codes)
            if _in_phone.any():
                _p_cand = data[_in_phone].copy()
                _is_np = _p_cand["NAME"].astype(str).apply(_is_non_phone_product)
                if _is_np.any():
                    _np_df = _p_cand[_is_np].copy()
                    _np_df["Comment_Detail"] = "Assigned to Wrong Category. Non-phone product listed in smartphone/mobile category."
                    _np_df["Reason"] = "1000004 - Wrong Category"
                    if base_flagged is not None and not base_flagged.empty:
                        base_flagged = pd.concat([base_flagged, _np_df], ignore_index=True).drop_duplicates(subset=["PRODUCT_SET_SID"])
                    else:
                        base_flagged = _np_df
    except Exception as _e:
        logger.warning("Non-phone in smartphone category check failed: %s", _e)

    # 2. Jerrycans / fuel containers in cooking appliances / kitchen bundles
    try:
        if {"NAME"}.issubset(data.columns):
            _jerry_re = re.compile(r"\b(?:jerrycan|jerry\s*can|jerrican|fuel\s*can)\b", re.IGNORECASE)
            _is_jerry = data["NAME"].astype(str).str.contains(_jerry_re, na=False)
            if _is_jerry.any():
                _j_cand = data[_is_jerry].copy()
                _cat_cols = [c for c in ("CATEGORY", "Initial_Category_Path", "Category_Path") if c in _j_cand.columns]
                _in_cooking = pd.Series(False, index=_j_cand.index)
                for _cc in _cat_cols:
                    _in_cooking |= _j_cand[_cc].astype(str).str.contains(r"cooking\s*appliance|kitchen\s*bundle", case=False, na=False)
                if _in_cooking.any():
                    _j_flagged = _j_cand[_in_cooking].copy()
                    _j_flagged["Comment_Detail"] = "Assigned to Wrong Category. Jerrycan/liquid container listed in cooking appliances/kitchen bundle."
                    _j_flagged["Reason"] = "1000004 - Wrong Category"
                    if base_flagged is not None and not base_flagged.empty:
                        base_flagged = pd.concat([base_flagged, _j_flagged], ignore_index=True).drop_duplicates(subset=["PRODUCT_SET_SID"])
                    else:
                        base_flagged = _j_flagged
    except Exception as _e:
        logger.warning("Jerrycan category check failed: %s", _e)

    if base_flagged is not None and not base_flagged.empty:
        return base_flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])

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


# Parts of the category tree a restricted brand must never fire in.
#
# For a rule with no category scope of its own, every match anywhere in the
# catalogue is a rejection, and the generated typo variations make a stray hit
# likely. NIVEA carries "niven", which matched the author of "100 Simple
# Secrets Why Dogs Make Us Happy ... by David Niven, Ph.D." and rejected the
# book. Nivea is skincare: a match inside a book title, a laptop listing or a
# lab supply is a false positive by construction.
#
# Matched against the full category PATH, not the leaf, because the leaf of a
# book category is "Philosophy" or "Family & Relationships" and says nothing
# about being a book. Top-level names come from the category map — the exact
# strings are "Books, Movies and Music", "Electronics", "Computing" and
# "Industrial & Scientific".
#
# Add a brand here when its rule has no category scope and it is misfiring in
# a part of the tree it has no business in. This narrows a rule; it never
# widens one.
# Where the Sony rules are allowed to fire at all. Everywhere else, "Sony" is
# a component spec ("Sony sensor", "Sony VAIO-compatible sleeve") or unrelated
# prose, not a claim to be selling a Sony product — reported case: a Tecno
# phone rejected for "Sony's Lytia 600 main camera" in the description.
#
# Shared by every Sony rule, so they cannot drift apart. Scope agreed with the
# user: TVs, all audio, cameras & accessories, projectors, PlayStation
# consoles/games/accessories. Deliberately narrower than "all of Gaming" —
# Xbox, Nintendo, PC gaming and generic toys never legitimately mention Sony,
# and PlayStation controllers sold loose under Computing/Phones & Tablets
# accessory trees were dropped from scope on request.
_SONY_INCLUDED_PATHS = (
    # TVs
    "electronics / television & video",
    "electronics / televisions & recorders",
    # Audio (all types)
    "electronics / audio",
    "electronics / home audio",
    "electronics / portable audio & video",
    "electronics / headphones",
    "electronics / accessories & supplies / audio & video accessories",
    "electronics / accessories & supplies / home audio accessories",
    "electronics / accessories & supplies / poratable audio accessories",
    "electronics / accessories & supplies / microphones",
    "phones & tablets / accessories / portable speakers & audio docks",
    "phones & tablets / accessories / speakers",
    "phones & tablets / accessories / speakerphones",
    # Car audio (radios, speakers, amplifiers, subwoofers) — not the rest of
    # the Car Electronics tree (alarms, GPS, dash cams, radar detectors),
    # which is out of scope
    "automobile / car electronics & accessories / car electronics / car audio",
    "electronics / car & vehicle electronics / car electronics / car audio",
    "electronics / car navigations & car av / car av",
    "electronics / car navigations & car av / portable audio car acccesories",
    # Cameras & accessories
    "electronics / camera & photo",
    "electronics / cameras",
    "electronics / accessories & supplies / camera & photo accessories",
    "electronics / accessories & supplies / camera accessories",
    "electronics / office electronic equipment / document cameras",
    # Projectors
    "electronics / office electronic equipment / presentation products",
    "computing / computer accessories / video projector accessories",
    # Playstations (consoles, games, accessories) — not controllers/hardware
    # sold loose elsewhere in the catalogue
    "gaming / playstation",
    "gaming / sony psp",
)

# Gaming-only paths where the "playstation" brand keyword is expected.
# "PlayStation" in a TV, audio, camera or projector listing is a product feature
# reference ("PS4-compatible", "PlayStation audio output"), not an unapproved
# seller — restricting to gaming paths eliminates those false positives.
_PLAYSTATION_INCLUDED_PATHS = (
    "gaming / playstation",
    "gaming / sony psp",
    "gaming / digital games",
    "gaming / accessories",
    "gaming / consoles",
    "gaming / gaming",
)

_SONY_EXCLUDED_GAME_PATHS = (
    "gaming / playstation / playstation 4 / games",
    "gaming / playstation / playstation 4 / digital games & dlc",
    "gaming / playstation / playstation 5 / ps 5 games",
    "gaming / digital games / playstation 4",
    "gaming / digital games / playstation 5",
)

RESTRICTED_BRAND_EXCLUDED_PATHS = {
    "nivea": ("books, movies and music", "electronics", "computing",
              "industrial & scientific"),
    "nivea baby": ("books, movies and music", "electronics", "computing",
                   "industrial & scientific"),
    "sony": _SONY_EXCLUDED_GAME_PATHS,
    "sony computer entertainment": _SONY_EXCLUDED_GAME_PATHS,
    "sony entertainment": _SONY_EXCLUDED_GAME_PATHS,
    "playstation": _SONY_EXCLUDED_GAME_PATHS,
}

# Parts of the category tree a restricted brand is ALLOWED to fire in —
# the inverse of RESTRICTED_BRAND_EXCLUDED_PATHS above: everything outside
# these path prefixes is dropped, rather than everything inside them.
#
# Sony is the only brand scoped this way today. "Sony" turns up constantly as
# a component spec ("50MP SONY'S LYTIA 600 MAIN CAMERA" on a Tecno phone) or
# a compatibility list ("... fits MacBook HP Dell Lenovo ... Sony VAIO ...
# Chromebook"), nowhere near an actual Sony product, so an exclude-list kept
# needing another category added every time a false positive turned up
# elsewhere. Restricting to where Sony genuinely sells narrows it once.
#
# All FOUR Sony rules share it — the KE list also carries "Sony Computer
# Entertainment" and "Sony Entertainment", and both include "sony" among
# their variations, so any of the three fires on a plain Sony mention; a
# Tecno Spark was once rejected by "Sony Computer Entertainment (as 'sony')"
# while only the plain Sony rule had been excluded, which was easy to miss.
#
# "Playstation" uses a narrower gaming-only scope — it must not fire in Sony
# TV, audio, camera or projector categories where "PlayStation" is a feature
# reference rather than a brand claim.
RESTRICTED_BRAND_INCLUDED_PATHS = {
    "sony": _SONY_INCLUDED_PATHS,
    "sony computer entertainment": _SONY_INCLUDED_PATHS,
    "sony entertainment": _SONY_INCLUDED_PATHS,
    "playstation": _PLAYSTATION_INCLUDED_PATHS,
}

_PS_GAME_CODES = {
    "1000363",  # Gaming / Digital Games / PlayStation 4
    "1000371",  # Gaming / Digital Games / PlayStation 4 / Currency Cards
    "1000404",  # Gaming / Digital Games / PlayStation 4 / Digital Games
    "1000414",  # Gaming / Digital Games / PlayStation 4 / Downloadable Content
    "1000418",  # Gaming / Digital Games / PlayStation 4 / Subscription Cards
    "1030068",  # Gaming / Digital Games / PlayStation 5
    "1005730",  # Gaming / Playstation / PlayStation 4 / Digital Games & DLC
    "1005834",  # Gaming / Playstation / PlayStation 4 / Games
    "1030067",  # Gaming / Playstation / PlayStation 5 / PS 5 Games
}

_PS_HARDWARE_FRAGMENTS = (
    "console", "controller", "headset", "accessories", "accessory",
    "camera", "cable", "vr", "virtual reality", "repair", "mount",
    "cooling", "case", "storage", "cleaning", "faceplate", "protector",
    "skin", "speaker", "thumb grip"
)

def _is_ps_game_category(cat_str: str = "", cat_code: str = "", code_to_path: Optional[Dict] = None) -> bool:
    code = str(cat_code).strip()
    if code in _PS_GAME_CODES:
        return True
    path = str(cat_str).strip()
    if (not path or path.lower() in ("nan", "none", "")) and code and code_to_path:
        path = str(code_to_path.get(code, "")).strip()
    c = path.lower()
    if not c or c in ("nan", "none"):
        return False
    is_ps4 = any(x in c for x in ("ps4", "ps 4", "playstation 4", "playstation4"))
    is_ps5 = any(x in c for x in ("ps5", "ps 5", "playstation 5", "playstation5"))
    if not (is_ps4 or is_ps5):
        return False
    # Hardware/consoles must never be treated as games (consoles remain restricted)
    if any(h in c for h in _PS_HARDWARE_FRAGMENTS):
        return False
    return any(g in c for g in ("game", "dlc")) or "digital games" in c


def _is_books_movies_music_category(cat_str: str = "", cat_code: str = "", code_to_path: Optional[Dict] = None) -> bool:
    code = clean_category_code(cat_code)
    path = str(cat_str or "").strip()
    if (not path or path.lower() in ("nan", "none", "")) and code and code_to_path:
        path = str(code_to_path.get(code, "")).strip()
    elif code and code_to_path and not path.lower().startswith("books, movies and music"):
        full = str(code_to_path.get(code, "")).strip()
        if full:
            path = full
    return path.lower().startswith("books, movies and music")


def check_restricted_brands(
    data: pd.DataFrame, country_rules: List[Dict],
    code_to_path: Optional[Dict] = None,
) -> pd.DataFrame:
    if data.empty or not country_rules:
        return pd.DataFrame(columns=data.columns)

    d = data.copy()

    # Exclude categories that should never be flagged for restricted brands:
    # 1. PS4 and PS5 game titles legitimately contain brand-like keywords,
    #    and game discs/software are exempt from Sony restricted brand rules.
    # 2. Books categories: all categories starting with "Books, Movies and Music"
    #    are exempt from restricted brands.
    # Keep the category exemption vectorized. The previous row-wise helper
    # called category-path lookups and several string checks once per product,
    # which became a major cost on large catalogues.
    _cat_text = d.get("CATEGORY", pd.Series("", index=d.index)).fillna("").astype(str).str.strip()
    _cat_code = d.get("CATEGORY_CODE", pd.Series("", index=d.index)).fillna("").astype(str).str.strip()
    _mapped_path = _cat_code.map(code_to_path or {}).fillna("").astype(str).str.strip()
    _cat_path = _cat_text.where(_cat_text.str.len().gt(0), _mapped_path)
    _cat_path_l = _cat_path.str.lower()
    _is_ps = _cat_code.isin(_PS_GAME_CODES) | _cat_path_l.str.contains(
        r"(?:ps\s*4|ps\s*5|playstation\s*4|playstation\s*5)", regex=True, na=False
    )
    _is_ps &= ~_cat_path_l.str.contains(
        r"console|controller|headset|accessories|accessory|camera|cable|vr|virtual reality|repair|mount|cooling|case|storage|cleaning|faceplate|protector|skin|speaker|thumb grip",
        regex=True, na=False,
    )
    _is_ps &= _cat_path_l.str.contains(r"game|dlc|digital games", regex=True, na=False)
    _is_books = _cat_path_l.str.startswith("books, movies and music")
    _cat_excl_mask = _is_ps | _is_books
    d = d[~_cat_excl_mask].copy()

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
        # Reset per rule. This dict was carried across the whole loop, so a
        # later rule's label overwrote an earlier one on the same row — and the
        # comment then named a rule that had not flagged the product at all.
        #
        # A book, "100 Simple Secrets Why Dogs Make Us Happy ... by David
        # Niven", was reported as "Restricted Brand: Simple". Simple is scoped
        # to 561 Health & Beauty categories and never matched it; NIVEA did,
        # through its generated variation "niven" hitting the author's
        # surname. The wrong label sent the investigation to the wrong rule.
        match_details = {}
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
                
        # Parts of the tree this brand must never fire in. Applied by PATH, so
        # a book category whose leaf reads "Philosophy" is still recognised as
        # a book. Needs code_to_path; without it the rule is left as-is rather
        # than silently doing nothing different.
        # Looked up under both spellings. rule["brand"] is normalised with the
        # spaces stripped — "NIVEA BABY" arrives as "niveababy" — so keying on
        # that alone silently missed it, and the book stayed rejected while the
        # exclusion appeared to be in place.
        _excl = (
            RESTRICTED_BRAND_EXCLUDED_PATHS.get(brand_name.lower())
            or RESTRICTED_BRAND_EXCLUDED_PATHS.get(str(brand_raw).strip().lower())
        )
        if _excl and code_to_path and not current_match.empty:
            _paths = (
                current_match["_cat_clean"].map(code_to_path).fillna("").astype(str).str.lower()
            )
            _blocked = pd.Series(False, index=current_match.index)
            for _frag in _excl:
                _blocked = _blocked | _paths.str.startswith(_frag)
            if _blocked.any():
                logger.info(
                    "[Restricted] %s: %s match(es) dropped, outside its part of "
                    "the catalogue", rule["brand_raw"], int(_blocked.sum()),
                )
            current_match = current_match[~_blocked]

        # Inverse of the block above: brands scoped to only PART of the tree
        # (Sony) rather than excluded from part of it. Superseded the old
        # hardcoded "Sony is allowed for gaming/playstation" text match, which
        # blanket-exempted anything with "gaming" or "console" in CATEGORY
        # regardless of path — that contradicted this scope once PlayStation
        # consoles/games became an included path in their own right, so an
        # unapproved seller listing a genuine PS5 is meant to be flagged.
        _incl = (
            RESTRICTED_BRAND_INCLUDED_PATHS.get(brand_name.lower())
            or RESTRICTED_BRAND_INCLUDED_PATHS.get(str(brand_raw).strip().lower())
        )
        if _incl and code_to_path and not current_match.empty:
            _paths = (
                current_match["_cat_clean"].map(code_to_path).fillna("").astype(str).str.lower()
            )
            _allowed = pd.Series(False, index=current_match.index)
            for _frag in _incl:
                _allowed = _allowed | _paths.str.startswith(_frag)
            if (~_allowed).any():
                logger.info(
                    "[Restricted] %s: %s match(es) dropped, outside its "
                    "allowed part of the catalogue", rule["brand_raw"],
                    int((~_allowed).sum()),
                )
            current_match = current_match[_allowed]

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

_PSEUDO_BRANDS = {
    "generic", "generique", "générique", "fashion", "beauty",
    "unbranded", "no brand", "nobrand", "sans marque",
    "original", "originals", "oroginal", "new", "other", "autre", "nan", "none", ""
}


# ── Potential Restricted Brand Signatures ────────────────────────────────────
# Sourced from Master_All_Brands_Searchable_Catalog.xlsx for restricted brands in Kenya.
# Sellers list products with generic/evasive brand values (Generic, Fashion, Unbranded, etc.)
# while putting restricted product-line or model names in the title.
_POTENTIAL_RESTRICTED_SIGNATURES: Dict[str, List[str]] = {
    "Simple": [
        r"kind\s+to\s+skin",
        r"daily\s+skin\s+detox",
        r"vitamin\s+c\s+glow",
        r"water\s+boost",
        r"active\s+skin\s+barrier",
        r"regeneration\s+age\s+resisting",
        r"age\s+resisting(?:\s+day|\s+night|\s+facial|\s+duo)",
        r"(?:10%|3%)\s+(?:niacinamide|vitamin\s+c|hyaluronic\s+acid)\s+booster\s+serum",
        r"replenishing\s+rich\s+moisturi[sz]er",
        r"protecting\s+light\s+moisturi[sz]er",
    ],
    "NIVEA": [
        r"cellular\s+expert\s+filler",
        r"cellular\s+expert\s+lift",
        r"derma\s+skin\s+clear",
        r"black\s*(?:&|and)\s*white\s+invisible",
        r"radiant\s*(?:&|and)\s*beauty",
        r"perfect\s*(?:&|and)\s*radiant",
        r"perfect\s*(?:&|and)\s*matte",
        r"pearl\s*(?:&|and)\s*beauty",
        r"luminous\s*630",
        r"q10\s*(?:\+|plus)?\s*(?:anti[\s\-]?wrinkle|firming|power|vitamin\s+c)",
        r"nivea\s+men\s+(?:cool\s+kick|deep|dry\s+impact|energy|hyaluron|sensitive)",
        r"nivea\s+sun(?:\s+kids|\s+uv|\s+protect)?",
        r"rich\s+nourishing\s+body\s+(?:milk|lotion)",
        r"hyaluron\s+lip\s+moisture",
        r"blackberry\s+shine\s+lip",
    ],
    "La Roche-Posay": [
        r"anthelios(?:\s+uvmune|\s+age\s+correct|\s+dermo|\s+oil\s+control)?",
        r"cicaplast(?:\s+baume|\s+levres|\s+mains)?",
        r"effaclar(?:\s+duo|\s+mat|\s+purifying|\s+micro|\s+serum|\s+astringent)?",
        r"lipikar(?:\s+baume|\s+huile|\s+syndet)?",
        r"toleriane(?:\s+dermallergo|\s+sensitive|\s+hydrating)?",
        r"hyalu\s+b5",
        r"retinol\s+b3",
        r"mela\s+b3",
        r"(?:pure\s+)?vitamin\s+c10\s+radiance",
        r"thermal\s+spring\s+water\s+soothing",
    ],
    "CeraVe": [
        r"hydrating\s+facial\s+cleanser",
        r"foaming\s+facial\s+cleanser",
        r"acne\s+foaming\s+cream\s+cleanser",
        r"sa\s+smoothing\s+(?:cleanser|cream)",
        r"daily\s+moisturi[sz]ing\s+lotion",
        r"(?:am|pm)\s+facial\s+moisturi[sz]ing\s+lotion",
        r"healing\s+ointment",
        r"eye\s+repair\s+cream",
        r"hydrating\s+foaming\s+oil\s+cleanser",
        r"resurfacing\s+retinol\s+serum",
        r"skin\s+renewing\s+(?:retinol|vitamin\s+c)\s+serum",
        r"hydrating\s+hyaluronic\s+acid\s+serum",
    ],
    "L'Oreal Paris": [
        r"revitalift(?:\s+filler|\s+laser|\s+hyaluronic|\s+vitamin\s+c)?",
        r"elvive(?:\s+dream\s+lengths|\s+extraordinary|\s+hyaluron)?",
        r"infallible\s+(?:32h|matte|fresh\s+wear)",
        r"infaillible\s+(?:32h|matte|fresh\s+wear)",
        r"bright\s+reveal\s+dark\s+spot",
        r"voluminous\s+lash\s+paradise",
        r"hyaluron\s+specialist\s+replumping",
    ],
    "Maybelline": [
        r"fit\s+me(?:\s+matte|\s+liquid|\s+concealer|\s+pressed|\s+poreless|\s+foundation|\s+powder)?",
        r"super\s*stay(?:\s+active|\s+matte|\s+vinyl|\s+ink|\s+24h|\s+30h)?",
        r"instant\s+age\s+rewind",
        r"lash\s+sensational(?:\s+sky\s+high)?",
        r"sky\s+high\s+mascara",
        r"lifter\s+gloss",
        r"baby\s+lips\s+moisturi[sz]ing",
        r"the\s+colossal\s+volum",
        r"hyper\s+precise\s+(?:all\s+day\s+)?liquid\s+eyeliner",
        r"tattoo\s+studio\s+gel\s+eyeliner",
    ],
    "JBL": [
        r"party\s*box(?:\s+(?:110|310|club\s+120|encore|710|1000))?",
        r"boom\s*box(?:\s+[23](?:\s+wi[\s\-]?fi)?)?",
        r"cinema\s+sb\s*170",
        r"(?:jbl\s+)?(?:clip\s+[345]|charge\s+[345]|flip\s+[456]|go\s+[234]|xtreme\s+[234])\b",
        r"(?:jbl\s+)?tune\s+(?:130nc|230nc|310c|500bt|510bt|670nc|720bt|760nc)\b",
        r"(?:jbl\s+)?live\s+(?:770nc|beam\s+3)\b",
        r"(?:jbl\s+)?wave\s+(?:beam|buds|flex)\b",
        r"(?:jbl\s+)?bar\s+(?:1000|500|2\.1)\b",
        r"(?:jbl\s+)?endurance\s+race",
    ],
    "Sony": [
        r"\bbravia(?:\s+[378]|\s+theatre|\s+x\d+)?\b",
        r"\bdualsense(?:\s+wireless\s+controller)?\b",
        r"\bplaystation\s+5\b",
        r"\bps5\s+(?:console|slim|digital)\b",
        r"\blinkbuds(?:\s+[sS]|\s+open)?\b",
        r"\binzone\s+h[379]\b",
        r"\balpha\s+(?:6700|7\s*iv|7r\s*v|7\s*iii|7c)\b",
        r"\b(?:wh|wf)[\s\-]?1000xm[45]\b",
        r"\bwf[\s\-]?(?:c500|c700n)\b",
        r"\bwh[\s\-]?(?:ch520|ch720n)\b",
        r"\bsrs[\s\-](?:xg300|xv800)\b",
        r"\bult\s+(?:field|tower)\b",
        r"\bht[\s\-]?(?:a3000|s20r|s400)\b",
        r"\bzv[\s\-](?:1|e10)\b",
    ],
}

_COMPILED_POTENTIAL_RESTRICTED: Dict[str, re.Pattern] = {
    brand: re.compile(rf"\b(?:{'|'.join(f'(?:{p})' for p in pats)})\b", re.IGNORECASE)
    for brand, pats in _POTENTIAL_RESTRICTED_SIGNATURES.items()
}


def check_potential_restricted_brand(
    data: pd.DataFrame,
    country_rules: Optional[List[Dict]] = None,
    code_to_path: Optional[Dict] = None,
) -> pd.DataFrame:
    """Flag products where sellers use generic/pseudo brands to evade restricted brands.

    Identifies products with BRAND in (Generic, Fashion, Unbranded, etc.) whose
    NAME contains signature product-line or model names of restricted brands
    (e.g., Effaclar, GO 3, Fit Me, Revitalift, Kind to Skin, etc.).
    """
    if data.empty or "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    if "_brand_lower" not in data.columns:
        data["_brand_lower"] = data.get("BRAND", pd.Series("", index=data.index)).astype(str).str.lower().str.strip()

    mask = data["_brand_lower"].isin(_PSEUDO_BRANDS)
    if "CATEGORY" in data.columns:
        mask = mask & ~data["CATEGORY"].astype(str).str.lower().str.contains(
            r"\b(?:case|cases|cover|covers)\b", regex=True, na=False
        )

    # Exclude PS game software/discs & books categories like check_restricted_brands
    if code_to_path:
        cat_mask = data.apply(
            lambda r: (
                _is_ps_game_category(r.get("CATEGORY", ""), r.get("CATEGORY_CODE", ""), code_to_path)
                or _is_books_movies_music_category(r.get("CATEGORY", ""), r.get("CATEGORY_CODE", ""), code_to_path)
            ),
            axis=1,
        )
        mask = mask & ~cat_mask

    target = data[mask].copy()
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    active_patterns = _COMPILED_POTENTIAL_RESTRICTED
    if country_rules:
        restr_brand_names = {
            str(r.get("brand", "")).lower() for r in country_rules if r.get("brand")
        }
        filtered_pats = {}
        for b_name, regex in _COMPILED_POTENTIAL_RESTRICTED.items():
            b_low = b_name.lower()
            if any(b_low in rb or rb in b_low for rb in restr_brand_names):
                filtered_pats[b_name] = regex
        if filtered_pats:
            active_patterns = filtered_pats

    names = target["NAME"].astype(str).values
    detected_brands = []
    detected_matches = []

    for name in names:
        matched_b = None
        matched_phrase = None
        for b_name, regex in active_patterns.items():
            m = regex.search(name)
            if m:
                matched_b = b_name
                matched_phrase = m.group(0).strip()
                break
        detected_brands.append(matched_b)
        detected_matches.append(matched_phrase)

    target["_pot_brand"] = detected_brands
    target["_pot_match"] = detected_matches

    flagged = target[target["_pot_brand"].notna()].copy()
    if flagged.empty:
        return pd.DataFrame(columns=data.columns)

    flagged["Comment_Detail"] = (
        "Potential "
        + flagged["_pot_brand"]
        + ": title contains product line '"
        + flagged["_pot_match"]
        + "'"
    )
    flagged["Pot_Restricted_Brand"] = flagged["_pot_brand"]
    flagged["Pot_Restricted_Match"] = flagged["_pot_match"]

    if "PRODUCT_SET_SID" in flagged.columns:
        return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])
    return flagged


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
        # Drones / UAVs — prohibited without a specific category
        "drone", "drones", "quadcopter", "quadcopters", "uav", "uavs",
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

    # ── Evasion normalisation ────────────────────────────────────────────────
    # Sellers evade keyword filters by:
    #   a) Swapping letters for accented lookalikes: Dronè, Droñe, Droné …
    #   b) Inserting separators mid-word:            d-rone, d+rone, d_rone
    # We build a normalised copy of every name for matching only; the original
    # name is preserved for display and highlighting.
    def _normalise_evasion(text: str) -> str:
        # Strip combining accents (NFKD decomposes ñ → n + combining tilde)
        nfkd = unicodedata.normalize("NFKD", text)
        ascii_text = nfkd.encode("ascii", "ignore").decode("ascii")
        # Remove separator characters inserted between alphabetic chars
        # e.g. d-rone → drone, d+rone → drone, d_rone → drone
        return re.sub(r"(?<=[a-z0-9])[-+_.~*](?=[a-z0-9])", "", ascii_text, flags=re.IGNORECASE)

    _norm_names = data["_name_lower"].astype(str).map(_normalise_evasion)

    all_kws = sorted(
        set(rule["keyword"] for rule in prohibited_rules), key=len, reverse=True
    )
    combined_pattern = re.compile(
        r"(?<!\w)(?:" + "|".join(re.escape(k) for k in all_kws) + r")(?!\w)",
        re.IGNORECASE,
    )
    # Match against the normalised names so accent/separator evasion is caught;
    # fall back to the original _name_lower to ensure non-evasion rules still work.
    match_mask = (
        _norm_names.str.contains(combined_pattern, na=False)
        | data["_name_lower"].str.contains(combined_pattern, na=False)
    )
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
        # Use the normalised form for matching so evasion variants are found
        name_for_match = _norm_names.loc[idx]
        cat_clean = data.loc[idx, "_cat_clean"]
        raw_name = str(data.loc[idx, "NAME"])
        matches = combined_pattern.findall(name_for_match) or combined_pattern.findall(name_lower)
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
    if "PRODUCT_SET_SID" in result.columns:
        return result.drop_duplicates(subset=["PRODUCT_SET_SID"])
    return result.drop_duplicates()


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
        # A sub-brand or a model is a claim on its parent's ceiling. Without
        # this, "Airmax 97", "AirPods Pro 3" and "Daytona" each answered to no
        # ceiling because none is a column in the sheet — while their parents
        # (Nike, Apple, Rolex) sit right there. Sellers evade the check by
        # writing BRAND=Generic/Fashion/Watch/Audio and putting the model in
        # the title, and the model IS the claim to a shopper.
        #
        # PRICE_CEILING_MODEL_ALIASES combines sneaker aliases with the audio
        # and luxury-watch model families. Only aliases whose parent has a
        # ceiling are worth compiling.
        _alias_terms = {}
        for _alias, _parent in PRICE_CEILING_MODEL_ALIASES.items():
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

        # Brands scanned in BRAND and NAME but never in DESCRIPTION. "Beats"
        # turns up constantly in genuine marketing copy as a comparison
        # ("Beats-level bass", "sounds better than Beats"), not a claim to be
        # selling Beats — a $23 oraimo headset with real oraimo NAME/BRAND
        # was flagged over exactly this. NAME/BRAND still count: an actual
        # counterfeit still has to say "Beats" somewhere a shopper reads it.
        _NO_DESCRIPTION_SCAN_BRANDS = {"beats"}

        # Description scanning is scoped to categories the sheet already
        # audits — the sneaker codes plus every category that has a ceiling.
        # Widened from sneaker-only because sellers hide "WH-1000XM5" or "Flip
        # 6" in the description of a Bluetooth-headphones listing exactly the
        # way sneaker sellers hid brand names before. A category with no
        # ceiling is not one we care about here anyway, so scanning it costs
        # false positives ("compatible with Sony" on a phone cable) for no
        # gain.
        _sneaker_cats = {
            clean_category_code(c) for c in (sneaker_category_codes or [])
        }
        _in_scope_cats = _sneaker_cats | {c for (_b, c) in brand_cat_price}
        _in_scope = d["_cat_clean"].isin(_in_scope_cats) if _in_scope_cats else pd.Series(False, index=d.index)
        if _in_scope.any():
            _blob = (
                d.get("DESCRIPTION", pd.Series("", index=d.index)).astype(str)
                + " "
                + d.get("SHORT_DESCRIPTION", pd.Series("", index=d.index)).astype(str)
            ).where(_in_scope, "").str.replace(_tag_re, " ", regex=True).str.lower()
        else:
            _blob = pd.Series("", index=d.index)

        _no_blob = pd.Series(False, index=d.index)
        _claims = {}   # brand -> boolean Series
        for b, rx in _brand_res.items():
            _blob_hit = _no_blob if b in _NO_DESCRIPTION_SCAN_BRANDS else _blob.str.contains(rx, na=False)
            _claims[b] = (
                (d["_brand_lower"] == b)
                | _name.str.contains(rx, na=False)
                | _blob_hit
            )
        for a, parent in _alias_terms.items():
            rx = _alias_res[a]
            _blob_hit = _no_blob if parent in _NO_DESCRIPTION_SCAN_BRANDS else _blob.str.contains(rx, na=False)
            _claims[parent] = _claims.get(parent, pd.Series(False, index=d.index)) | (
                (d["_brand_lower"] == a)
                | _name.str.contains(rx, na=False)
                | _blob_hit
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
            # <= not <. The ceilings in suspected_fake.xlsx are the LOWEST
            # plausible price for a genuine listing, so a price sitting on the
            # floor is on the wrong side of "genuinely selling for at least
            # this much". A Sony WH-1000XM5 at $40 (Sony's floor) reads as a
            # fake to a reviewer, not as a legitimate deal.
            _under = claimed & (d["price_to_use"] <= _ceil) & (_ceil > 0)
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
    data: pd.DataFrame, refurb_data: dict, country_code: str, code_to_path: Optional[Dict] = None
) -> pd.DataFrame:
    required = {"PRODUCT_SET_SID", "CATEGORY_CODE", "SELLER_NAME", "NAME"}
    if not required.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    phone_cats = refurb_data.get("categories", {}).get("Phones", set())
    laptop_cats = refurb_data.get("categories", {}).get("Laptops", set())
    keywords = refurb_data.get("keywords", set())
    sellers = refurb_data.get("sellers", {}).get(country_code, {})
    if not phone_cats and not laptop_cats and not code_to_path:
        return pd.DataFrame(columns=data.columns)

    kw_pattern = None
    if keywords:
        kw_pattern = re.compile(
            r"\b(?:"
            + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
            + r")\b",
            re.IGNORECASE,
        )

    d = data
    c2p = code_to_path or {}

    # Category scope matching (code or path)
    cat_paths = d["_cat_clean"].map(c2p).fillna("").astype(str).str.lower()
    is_phone = d["_cat_clean"].isin(phone_cats) | cat_paths.str.contains(r"\b(?:phone|smartphone|mobile)\b", regex=True)
    is_laptop = d["_cat_clean"].isin(laptop_cats) | cat_paths.str.contains(r"\b(?:laptop|notebook|macbook|ultrabook)\b", regex=True)
    in_scope = is_phone | is_laptop

    # Refurb detection: by keyword in title OR brand explicitly set to Refurbished/Renewed
    is_refurb_brand = d["_brand_lower"].isin({"refurbished", "renewed", "refurbished / renewed", "apple refurbished"})
    has_keyword = pd.Series(False, index=d.index)
    if kw_pattern:
        has_keyword = d["NAME"].astype(str).str.contains(kw_pattern, na=False)
    is_refurb = is_refurb_brand | has_keyword

    approved_phones = sellers.get("Phones", set())
    approved_laptops = sellers.get("Laptops", set())
    not_approved = (is_phone & ~d["_seller_lower"].isin(approved_phones)) | (
        is_laptop & ~d["_seller_lower"].isin(approved_laptops)
    )

    flagged = d[in_scope & is_refurb & not_approved].copy()
    if not flagged.empty:
        REASON_UNAPPROVED_REFURB_SELLER = (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim"
        )
        flagged["Reason"] = REASON_UNAPPROVED_REFURB_SELLER

        def build_comment(row):
            ptype = "Phone" if row["_cat_clean"] in phone_cats or "phone" in str(c2p.get(str(row["_cat_clean"]), "")).lower() else "Laptop"
            seller = row.get("SELLER_NAME", "")
            return f"Unapproved {ptype} refurb seller — seller '{seller}' is not authorized to sell refurbished {ptype}s in {country_code}."

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


def check_seller_approved_for_alcohol(
    data: pd.DataFrame,
    alcohol_data: Dict,
    country_code: str,
    code_to_path: Optional[Dict] = None,
) -> pd.DataFrame:
    """Restrict alcoholic-drink categories to sellers approved in Alcohol.xlsx."""
    if not {"PRODUCT_SET_SID", "SELLER_NAME"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)
    if country_code != "KE":
        return pd.DataFrame(columns=data.columns)

    approved_sellers = alcohol_data.get("sellers", {}).get("KE", set())
    prefixes = tuple(
        str(v).strip().casefold().rstrip("/")
        for v in alcohol_data.get("category_prefixes", set())
        if str(v).strip()
    )
    if not approved_sellers or not prefixes:
        return pd.DataFrame(columns=data.columns)

    # Prefer the resolved category path. Fall back to the raw CATEGORY field
    # when a source already contains full paths instead of category codes.
    raw_path = data.get("CATEGORY", pd.Series("", index=data.index)).fillna("").astype(str).str.strip().str.casefold()
    if code_to_path and "_cat_clean" in data.columns:
        resolved = data["_cat_clean"].map(code_to_path).fillna("").astype(str).str.strip().str.casefold()
        raw_path = raw_path.mask(raw_path.isin({"", "nan", "none"}), resolved)
    in_scope = raw_path.str.startswith(prefixes, na=False)
    not_approved = ~data["_seller_lower"].isin(approved_sellers)
    flagged = data[in_scope & not_approved].copy()
    if not flagged.empty:
        flagged["Reason"] = (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim"
        )
        flagged["Comment_Detail"] = (
            "Seller not approved to sell alcohol: "
            + flagged["SELLER_NAME"].astype(str)
        )
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
    data: pd.DataFrame, jerseys_data: Dict, country_code: str,
    code_to_path: Optional[Dict] = None,
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

    _FOOTWEAR_TERMS = (
        "footwear", "shoe", "shoes", "formal shoes", "lace up", "slip on", "boots",
        "sneaker", "sneakers", "sandal", "sandals", "slipper", "slippers",
        "heels", "loafer", "loafers", "oxford", "oxfords", "derby", "derbies", "moccasin", "moccasins"
    )
    _FOOTWEAR_NAME_RE = re.compile(
        r"\b(?:shoes?|boots?|loafers?|oxfords?|sneakers?|heels?|footwear|sandals?|slippers?|derby|derbies|moccasins?)\b",
        re.IGNORECASE,
    )

    # Hats, caps and headwear are NOT jerseys — exclude them so a "Manchester United Cap" is
    # never flagged as a counterfeit jersey (caps are legitimate licensed merchandise).
    _HEADWEAR_TERMS = (
        "hats & caps", "hats and caps", "caps & hats", "caps and hats",
        "headwear", "cap", "caps", "hat", "hats", "beanie", "beanies",
        "snapback", "trucker cap", "baseball cap", "fitted cap", "bucket hat",
        "headband", "headbands",
    )
    _HEADWEAR_CAT_RE = re.compile(
        r"\b(?:hats?\s*(?:&|and)\s*caps?|caps?\s*(?:&|and)\s*hats?|headwear|snapback"
        r"|baseball\s*cap|trucker\s*cap|fitted\s*cap|bucket\s*hat|beanies?)\b",
        re.IGNORECASE,
    )
    _HEADWEAR_NAME_RE = re.compile(
        r"\b(?:cap|caps|hat|hats|beanie|beanies|snapback|headband|headwear|trucker cap|bucket hat)\b",
        re.IGNORECASE,
    )

    def _is_footwear(row) -> bool:
        cat_str = str(row.get("CATEGORY", "") or "").strip().lower()
        code = clean_category_code(row.get("CATEGORY_CODE", "") or row.get("_cat_clean", ""))
        if any(term in cat_str for term in _FOOTWEAR_TERMS):
            return True
        if code and code_to_path:
            path = str(code_to_path.get(code, "") or "").strip().lower()
            if any(term in path for term in _FOOTWEAR_TERMS):
                return True
        name = str(row.get("NAME", "") or "")
        if _FOOTWEAR_NAME_RE.search(name):
            return True
        return False

    def _is_headwear(row) -> bool:
        """Caps, hats and headwear are legitimate licensed merchandise — NOT counterfeit jerseys."""
        cat_str = str(row.get("CATEGORY", "") or "").strip().lower()
        code = clean_category_code(row.get("CATEGORY_CODE", "") or row.get("_cat_clean", ""))
        if any(term in cat_str for term in _HEADWEAR_TERMS):
            return True
        if _HEADWEAR_CAT_RE.search(cat_str):
            return True
        if code and code_to_path:
            path = str(code_to_path.get(code, "") or "").strip().lower()
            if any(term in path for term in _HEADWEAR_TERMS):
                return True
            if _HEADWEAR_CAT_RE.search(path):
                return True
        name = str(row.get("NAME", "") or "")
        if _HEADWEAR_NAME_RE.search(name):
            return True
        return False

    d = data.copy()
    if "_cat_clean" not in d.columns:
        d["_cat_clean"] = d["CATEGORY_CODE"].apply(clean_category_code)

    # Exclude footwear and headwear from jersey counterfeit validation
    is_footwear_mask = d.apply(_is_footwear, axis=1)
    is_headwear_mask = d.apply(_is_headwear, axis=1)
    d = d[~is_footwear_mask & ~is_headwear_mask].copy()
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    kw_pattern = re.compile(
        r"(?<!\w)(?:"
        + "|".join(re.escape(k) for k in sorted(keywords, key=len, reverse=True))
        + r")(?!\w)",
        re.IGNORECASE,
    )
    in_scope = d["_cat_clean"].isin(categories)
    has_keyword = d["NAME"].astype(str).str.contains(kw_pattern, na=False)
    if "_seller_lower" not in d.columns:
        d["_seller_lower"] = d.get("SELLER_NAME", pd.Series("", index=d.index)).fillna("").astype(str).str.strip().str.lower()
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


def check_name_too_short(
    data: pd.DataFrame,
    code_to_path: Optional[Dict] = None,
    book_category_codes: Optional[List[str]] = None,
    **kwargs
) -> pd.DataFrame:
    if "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)
    d = data.copy()
    _book_codes = set(str(c).strip() for c in (book_category_codes or []))

    def _is_book(row) -> bool:
        cat_str = str(row.get("CATEGORY", "") or "").strip()
        cat_code = clean_category_code(row.get("CATEGORY_CODE", "") or row.get("_cat_clean", ""))
        if cat_code and cat_code in _book_codes:
            return True
        return _is_books_movies_music_category(cat_str, cat_code, code_to_path)

    is_book_mask = d.apply(_is_book, axis=1)
    d_filtered = d[~is_book_mask]
    mask = d_filtered["NAME"].astype(str).str.strip().str.len() < 15
    return d_filtered[mask].copy()


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


# Words that describe what a fragrance IS, not which one it is.
#
# The catalog lists these as model names — "Eau De Parfum" arrives as a Missoni
# model — and matching one is worth nothing, because nearly every listing in
# the category contains it. Left in, they produced findings whose entire
# evidence was the words on the bottle:
#
#   Generic | "Sofia the First Eau De Parfum"
#     -> Suspected fake Missoni perfume — 'eau de parfum' in name
#
# Excluded from the model terms only. A real house's name reaching the same
# listing still matches, which is the signal this check is after.
_GENERIC_FRAGRANCE_TERMS = {
    "eau de parfum", "eau de toilette", "eau de cologne", "eau de perfume",
    "parfum", "perfume", "cologne", "fragrance", "fragrances",
    "body mist", "body spray", "body lotion", "body oil", "body butter",
    "deodorant", "antiperspirant", "roll on", "roll-on",
    "gift set", "travel spray", "travel size", "spray",
    "for men", "for women", "for unisex", "pour homme", "pour femme",
    "natural spray", "long lasting", "edp", "edt", "mist",
    # Ordinary adjectives that happen to be model names. "Original" is a Police
    # model, so "Arabic Original Rave Now Eau de Parfum" was reported as a fake
    # Police — on the strength of the word "original". A real house's name in
    # the same title still matches; this only stops the adjective standing as
    # the whole case.
    "original", "intense", "extreme", "classic", "luxury", "premium",
    "special", "edition", "limited edition", "collection", "set", "travel",
    "gold", "silver", "noir", "blanc", "sport", "elegance",
    # Scent notes. A note names what a fragrance smells of, not which fragrance
    # it is, and it appears in a large share of the category's titles. "Vanilla"
    # is a The Body Shop model, so a Shalimar "Amber Vanilla Eau de Parfum" was
    # reported as a fake Body Shop on the strength of the word vanilla.
    #
    # Same rule as the rest: a real house's name in the same title still
    # matches. This only stops a note standing as the whole case.
    # Listed properly rather than one name at a time. A title that spells out
    # its pyramid — "Ginger, Davana, Osmanthus, Vetiver, Tonka & Tobacco" —
    # collides with whichever house happens to have a model of that name;
    # "osmanthus" is an Acqua di Parma model, and it rejected a Falcon Wazeer.
    "vanilla", "amber", "oud", "musk", "rose", "jasmine", "sandalwood",
    "vetiver", "patchouli", "bergamot", "citrus", "lavender", "coconut",
    "cherry", "caramel", "chocolate", "coffee", "honey", "leather", "tobacco",
    "saffron", "cedar", "lemon", "mint", "peach", "berry", "floral", "woody",
    "osmanthus", "davana", "ginger", "tonka", "tonka bean", "iris", "orris",
    "neroli", "ylang", "ylang ylang", "tuberose", "gardenia", "peony",
    "magnolia", "lily", "violet", "freesia", "orchid", "lotus", "mimosa",
    "geranium", "clary sage", "sage", "thyme", "basil", "cardamom",
    "cinnamon", "clove", "nutmeg", "pepper", "pink pepper", "black pepper",
    "coriander", "cumin", "anise", "fennel", "juniper", "cypress", "pine",
    "fir", "birch", "oakmoss", "moss", "labdanum", "benzoin", "myrrh",
    "frankincense", "incense", "styrax", "tolu", "opoponax", "elemi",
    "vanille", "praline", "hazelnut", "almond", "pistachio", "coconut milk",
    "mandarin", "tangerine", "orange", "blood orange", "grapefruit", "lime",
    "yuzu", "petitgrain", "verbena", "lemongrass", "apple", "pear", "plum",
    "fig", "raspberry", "strawberry", "blackcurrant", "cassis", "pineapple",
    "mango", "melon", "watermelon", "lychee", "papaya", "guava",
    "sea salt", "marine", "aquatic", "ozonic", "aldehydes", "amberwood",
    "ambroxan", "cashmeran", "iso e super", "white musk", "leathery",
    "suede", "vinyl", "metallic", "smoky", "spicy", "powdery", "creamy",
    "gourmand", "oriental", "chypre", "fougere", "aromatic", "fresh",
}


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
        if (" " in t or (len(t) >= 5 and t not in color_words))
        and t not in _GENERIC_FRAGRANCE_TERMS
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
    if flagged.empty:
        return pd.DataFrame(columns=data.columns)

    def build_comment(row):
        term = row["_pfume_match"]
        brand = term_to_brand.get(term, term.title())
        return f"Suspected fake {brand} perfume — '{term}' in name"

    flagged["Comment_Detail"] = flagged.apply(build_comment, axis=1)

    # A house's NAME is proof; one of its MODEL names is a question.
    #
    # "Lancome" or "Versace" in a title under a first-copy brand is
    # unambiguous. A model name is not: many are ordinary words — legend,
    # paris, brown, gold edition, blossom — and the brands that trip this are
    # houses that sell their own catalogue, so their own products collide with
    # somebody's model by chance. Victory Legend and Blossom Paris are
    # Fragrance Deluxe's own; Brown Orchid is Fragrance World's own.
    #
    # There is no text rule separating those from a real copy: "Scandal Pink
    # Perfume" (a genuine Jean Paul Gaultier dupe) has exactly the same shape.
    # Frequency, brand-terms-only and "a fake leads with the copied name" were
    # each measured against the corpus and each failed — the last scored 11/11
    # on hand-picked cases and then discarded real copies carrying a prefix
    # ("Arabic Club de Nuit", "Google Boss").
    #
    # So the model matches stop being rejections and become a badge in the
    # visual grid, where a person decides. No genuine catch is lost; the
    # guesses stop costing rejections.
    _is_brand_term = flagged["_pfume_match"].isin(legit_brand_terms)
    _review = flagged[~_is_brand_term]
    # Handed to the grid through session state rather than an import: the grid
    # lives in ui_components, and importing this module from there re-executes
    # the entry script. Cleared with the rest of the batch in
    # _reset_report_state, or a previous upload's claims would badge this one.
    try:
        st.session_state["_perfume_model_claims"] = {
            str(r["PRODUCT_SET_SID"]).strip(): str(r["Comment_Detail"])
            for _, r in _review.iterrows()
            if str(r.get("PRODUCT_SET_SID", "")).strip()
        }
    except Exception:
        logger.exception("could not record perfume model claims for the grid")

    # Every matching row is returned (no per-SID dedup): "Suspected Fake
    # Perfume" is a ROW_LEVEL_VALIDATOR, so the runner keeps precisely these
    # rows instead of re-expanding one representative row to its whole SID.
    return flagged[_is_brand_term].drop(columns=["_pfume_match"])


# Staging dict: overturned Apple-accessories rows are deposited here by
# check_brand_image_mismatch so validate_products can pick them up.
_APPLE_ACC_OVERTURNED_STAGING: dict = {}


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

    # Apple accessories override: cases/covers for phones/tablets/laptops
    # often carry Apple imagery but are accessories FOR Apple devices.
    # The AI rejection is wrong; approve and log for audit.
    _APPLE_ACC_CAT_RE = re.compile(
        r"\b(?:case|cases|cover|covers|sleeve|sleeves|pouch|pouches|screen.?protector)\b",
        re.IGNORECASE,
    )
    if "CATEGORY" in flagged.columns:
        _acc_apple_mask = (
            flagged["_det_l"].str.strip().eq("apple")
            & flagged["CATEGORY"].astype(str).str.contains(_APPLE_ACC_CAT_RE, na=False)
        )
    else:
        _acc_apple_mask = pd.Series(False, index=flagged.index)

    overturned = flagged[_acc_apple_mask].copy()
    flagged = flagged[~_acc_apple_mask].copy()

    if not overturned.empty:
        overturned["Comment_Detail"] = (
            "AI rejection overturned: Apple brand detected on image of a "
            "cases/covers accessory -- product approved. Original: "
            + overturned["Comment_Detail"].fillna("").astype(str)
        )
        _APPLE_ACC_OVERTURNED_STAGING.clear()
        _APPLE_ACC_OVERTURNED_STAGING.update(
            overturned
            .drop(columns=["_det_l", "_decl_l"], errors="ignore")
            .drop_duplicates(subset=["PRODUCT_SET_SID"])
            .set_index("PRODUCT_SET_SID")
            .to_dict(orient="index")
        )

    if flagged.empty:
        return pd.DataFrame(columns=data.columns)
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
    # International for the eight markets. Require leading + OR valid country operator prefixes when un-prefixed.
    r"\+(?:254|256|234|233|212|221|225)[\s\-]?(?:\d[\s\-]?){7,11}\d\b"
    r"|\b254[\s\-]?[17](?:[\s\-]?\d){8}\b"                                # Kenya without + (07xx, 01xx)
    r"|\b256[\s\-]?[7](?:[\s\-]?\d){8}\b"                                 # Uganda without +
    r"|\b234[\s\-]?[789](?:[\s\-]?\d){9}\b"                               # Nigeria without +
    r"|\b233[\s\-]?[235](?:[\s\-]?\d){8}\b"                               # Ghana without +
    r"|\b212[\s\-]?[567](?:[\s\-]?\d){8}\b"                               # Morocco without +
    r"|\b221[\s\-]?[37](?:[\s\-]?\d){7,8}\b"                              # Senegal without + (valid SN mobile 7x or landline 33)
    r"|\b225[\s\-]?[0-9](?:[\s\-]?\d){9}\b"                               # Ivory Coast without + (10 digits)
    # Egypt's country code is only two digits, so require the leading + to stop
    # ordinary numbers in text ("20 000 mAh") from matching.
    r"|\+20[\s\-]?(?:\d[\s\-]?){8,10}\d\b"
    r"|\+\d{1,3}[\s\-]?(?:\d[\s\-]?){7,13}\d\b"                  # any other international
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
#
# YouTube URLs (youtube.com, youtu.be, youtube-nocookie.com) are explicitly excluded
# at the regex level via a negative lookahead — tutorial/unboxing links in descriptions
# are not off-platform contact and must never fire this check.
#
# IPv4 addresses (e.g. http://192.168.0.1/) are also excluded — they appear
# legitimately in product descriptions for router setup, IP cameras, NAS devices,
# and similar tech products, and are not off-platform seller contact.
_YOUTUBE_LOOKAHEAD = r"(?!(?:[a-z0-9_\-]+\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com))"
_IPV4_LOOKAHEAD = r"(?!\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
_WEBSITE_RE = re.compile(
    r"(?:"
    r"https?://" + _YOUTUBE_LOOKAHEAD + _IPV4_LOOKAHEAD + r"\S+"
    r"|\bwww\.(?!(?:[a-z0-9_\-]+\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com))\S+"
    r"|wa\.me/\S+"
    r"|\b(?!(?:[a-z0-9_\-]+\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com))" + r"[a-z0-9][a-z0-9\-]{1,30}\.(?:com|net|org|shop|store|biz|info|"
    r"co\.ke|co\.ug|com\.ng|com\.gh|co\.za|ma|eg|sn|ci)\b"
    r")",
    re.IGNORECASE,
)
# Domains used as harmless placeholders or examples in product copy.  They
# still match the explicit ``http://`` branch above, so they must be filtered
# after extraction as well as bare-domain false positives.
_WEBSITE_FALSE_POSITIVE_DOMAINS = frozenset({"fl.oz"})


def _is_website_false_positive(term: str) -> bool:
    """Return True for an explicitly allowed example/placeholder domain."""
    value = str(term or "").strip().lower()
    # Markdown links and punctuation can leave a trailing ``]``/``)`` in the
    # regex match. Stop at URL punctuation before extracting the host.
    value = re.split(r"[\s\]\)\}>,'\";]+", value, maxsplit=1)[0]
    value = re.sub(r"^(?:https?://|www\.)", "", value)
    host = value.split("/", 1)[0].rstrip(".")
    return host in _WEBSITE_FALSE_POSITIVE_DOMAINS


# Pre-compiled IPv4 pattern reused in _is_platform_url for belt-and-suspenders filtering
_IPV4_URL_RE = re.compile(r"https?://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", re.IGNORECASE)

# Allowed platform/video URLs that should be masked before phone/contact scanning,
# so digits in video IDs, timestamps, or tracking tokens cannot trigger _PHONE_RE.
_PLATFORM_URL_RE = re.compile(
    r"(?:https?://(?:[a-z0-9_\-]+\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com|jumia\.[a-z.]+|wsrv\.nl|cloudfront\.net)"
    r"|\b(?:www\.)?(?:[a-z0-9_\-]+\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com)"
    r")"
    r"(?:[/?][^\s<\"'>]*[^\s<\"'>.,;:)])?",
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
    r"|\b(?:shop|stall|suite|kiosk)[ \t]*(?:no\.?|number|#)?[ \t]*\d+(?![A-Za-z])"
    r"|\boffice[ \t]*(?:no\.?|number|#)[ \t]*\d+(?![A-Za-z])"
    r"|\b\d+[ \t]*(?:st|nd|rd|th)?[ \t]*floor\b"
    r"|\balong\s+[a-z]+\s+(?:road|rd|street|st|avenue|ave)\b"
    r"|\bopposite[ \t]+(?:the[ \t]+)?(?:[a-z]+[ \t]+){0,2}" + _LANDMARK_ALT +
    r"|\bnext[ \t]+to[ \t]+(?:the[ \t]+)?(?:[a-z]+[ \t]+){0,2}" + _LANDMARK_ALT +
    r"|\b(?:located\s+at|find\s+us\s+at)\s+(?:our\s+)?(?:shop|store|office|showroom)\b"
    r"|\b(?:visit|come\s+to)\s+(?:our\s+)?(?:office|showroom)\b"
    r"|\b(?:visit|come\s+to|located\s+at|find\s+us\s+at)\s+(?:our\s+)?(?:shop|store)\s+(?:at|in|along|opposite|next\s+to|near|behind|around)\s+[a-z0-9]"
    r"|\b(?:visit|come\s+to)\s+(?:our\s+)?(?:shop|store)[ \t]*(?:no\.?|number|#)?[ \t]*\d+"
    # French — "BP 1234", "boîte postale", "magasin n° 12", "2ème étage",
    # "en face de", "à côté du marché", "situé à" (requires ≥3 digits so battery codes like BP02XL never match)
    r"|\b(?:b\.?\s*p\.?|bo[iî]te\s+postale)\s*(?:n[o°]\.?|num[ée]ro|#)?[ \t]*\d{3,6}\b"
    # A bare "local 5" is common in technical text ("local 5G networks"),
    # so require an explicit unit marker for local. The other address words
    # retain their optional marker but stop before an attached letter.
    r"|\b(?:magasin|boutique|bureau)[ \t]*(?:n[o°]\.?|num[ée]ro|#)?[ \t]*\d+(?![A-Za-z])"
    r"|\blocal[ \t]*(?:n[o°]\.?|num[ée]ro|#)[ \t]*\d+(?![A-Za-z])"
    r"|\b\d+[ \t]*(?:er|[eè]me)?[ \t]*[ée]tage\b"
    r"|\ben\s+face\s+d[eu]\b|\b[aà]\s+c[oô]t[ée]\s+d[eu]\b"
    r"|\bsitu[ée]\s+[aà]\b|\bvenez\s+(?:nous\s+voir|[aà])\b"
    # Arabic — "ص.ب ١٢٣" (P.O. Box), "محل رقم", "الطابق", "بجانب", "أمام"
    r"|ص\.?\s*ب\.?\s*\d+"
    r"|(?:محل|متجر|مكتب)[ \t]*(?:رقم)?[ \t]*\d+(?![A-Za-z])"
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

_OFFPLATFORM_SOFT_RE = re.compile(
    r"(?:\bcall\s+us\b|\bcontact\s+(?:us|the\s+seller|seller)\b"
    r"|\border\s+(?:directly|via|through)\b|\bdm\s+us\b|\binbox\s+us\b"
    r"|\bfollow\s+us\s+on\b|\bfind\s+us\s+on\b"
    r"|\bvisit\s+our\s+(?:website|site|facebook|instagram|ig|tiktok|page\s+on)\b"
    # French — "appelez-nous", "contactez le vendeur", "commandez directement",
    # "écrivez-nous", "suivez-nous sur", "visitez notre boutique"
    r"|\bappelez[\s\-]?(?:nous|moi)\b|\bcontactez[\s\-]?(?:nous|moi|le\s+vendeur)\b"
    r"|\bcommandez\s+(?:directement|via|par)\b|\b[ée]crivez[\s\-]?nous\b"
    r"|\bsuivez[\s\-]?nous\s+sur\b|\bvisitez\s+(?:notre|nos)\s+(?:site|page\s+facebook|page\s+instagram)\b"
    r"|\bnous\s+joindre\b|\bjoignez[\s\-]?nous\b"
    # Arabic — "اتصل بنا", "تواصل معنا", "اطلب مباشرة", "تابعنا على",
    # "راسلنا", "زوروا متجرنا"
    r"|اتصل\s*بنا|تواصل\s*مع(?:نا)?|اطلب\s*مباشرة|تابعنا\s*على"
    r"|راسلنا|كلمنا"
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


# Common English words that legitimately appear before TLD-like suffixes in
# product descriptions — e.g. "only.Store in a cool and dry place", "ends.net
# weight 1 kg".  Without this filter, _WEBSITE_RE treats them as bare domains
# and raises a false Off-Platform Contact flag.
# Only the bare-domain branch of _WEBSITE_RE is tested here; matches that
# start with https?://, www., or wa.me/ are never false positives of this kind.
_BARE_DOMAIN_FALSE_POSITIVE_SLDS = frozenset({
    # Adverbs / conjunctions common in instructions
    "only", "also", "just", "even", "then", "when", "than", "that",
    "this", "thus", "else", "away", "back", "from", "into", "over",
    "with", "well", "still", "here", "there",
    # Instruction / storage words
    "ends", "cool", "warm", "cold", "dry", "safe", "best", "good",
    "full", "soft", "hard", "fast", "slow", "free", "more", "less",
    "most", "open", "fine", "real", "true", "pure", "clean", "fresh",
    "dark", "deep", "flat", "long", "wide", "thin", "slim", "mini",
    "dual", "quad", "auto", "gold", "bold",
    # Colour words frequently followed by .net, .org, .info
    "blue", "pink", "grey", "gray", "rose", "mint", "sand", "navy",
    "teal", "lime", "jade",
    # Product/instruction action words
    "keep", "hold", "use", "add", "mix", "wash", "pack",
    "sets", "pair", "unit", "size", "type", "kind", "form",
    "mode", "code", "core", "data", "rate", "port",
    "heat", "fire", "edge", "base", "head", "side", "face", "body",
    "case", "part", "area", "spot", "line", "note", "care", "date",
    # Common English stop words
    "have", "been", "will", "your", "they", "them", "what", "some",
    "many", "much", "each", "both", "such", "like", "same", "time",
    "very", "need", "know", "come", "find", "make", "take", "give",
    # Time / calendar words — "any day.Shop", "all day.store", "every day.net"
    # are sentence fragments, not domain names.
    "day", "days", "today", "everyday", "anyday", "night", "nights",
    "week", "weeks", "year", "years", "hour", "hours", "noon",
    "dawn", "dusk", "morning", "evening", "always", "daily",
    # Quantity / shopping-phrase words that appear before .shop / .store
    "any", "all", "buy", "shop", "sale", "deal", "top", "new",
    "now", "soon", "fast", "easy", "next", "last", "late",
})


def _is_bare_domain_false_positive(term: str) -> bool:
    """Return True when *term* is a bare-domain match (no http/www/wa.me prefix)
    whose second-level domain is a common English word — e.g. 'only.store' from
    'only.Store in a cool and dry place', 'ends.net' from 'product ends.net here'.
    Those are sentence fragments caught by the broad bare-domain pattern, not URLs.
    Matches that carry a scheme or www. prefix are always real URLs and are never
    filtered here.
    """
    t = term.strip().lower()
    if t.startswith(("http://", "https://", "www.", "wa.me/")):
        return False
    domain_part = t.split("/")[0]   # strip any path component
    dot = domain_part.rfind(".")
    if dot <= 0:
        return False
    sld = domain_part[:dot]
    return sld in _BARE_DOMAIN_FALSE_POSITIVE_SLDS


def check_offplatform_contact(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Detect sellers hiding contact details (phones, WhatsApp, URLs) or
    divert-the-buyer wording in DESCRIPTION/SHORT_DESCRIPTION — the
    standard pattern for taking buyers off-platform.

    NAME is intentionally excluded: product titles legitimately contain
    model numbers that look like phone numbers, and many titles include
    manufacturer/demo URLs. Scanning NAME produced far too many false
    positives and missed the real abuse vector (buried in the description).

    HTML tags are stripped before scanning so markup attributes (styles,
    color hex codes) can't false-positive the phone patterns. Matches are
    tracked per-column (not on one merged blob) so Comment_Detail — the only
    field the pipeline actually carries through to the report/card/export —
    can say exactly WHAT was found and WHICH field it was found in, instead
    of just "something matched somewhere in this product".
    """
    # NAME excluded — see docstring above. Product titles legitimately contain
    # model numbers, firmware version strings, and manufacturer URLs. Scanning
    # NAME for contact info produced far too many false positives.
    text_cols = [c for c in ("DESCRIPTION", "SHORT_DESCRIPTION") if c in data.columns]
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
                  .str.replace(_PLATFORM_URL_RE, " ", regex=True)
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
        """Jumia's own domains, image CDNs, content-sharing sites, and IP-address
        URLs are not off-platform contact — a YouTube link or a router admin page
        (http://192.168.0.1/) in a description is not the same as a seller phone
        number or external website."""
        t = term.lower()
        return (
            "jumia" in t
            or "wsrv.nl" in t
            or "cloudfront" in t
            or "youtube" in t
            or "youtu.be" in t
            or _is_website_false_positive(t)
            or bool(_IPV4_URL_RE.match(t))  # e.g. http://192.168.0.1/
        )

    def _is_phone_false_positive(m_match, full_text: str) -> bool:
        start, end = m_match.start(), m_match.end()
        pre_text = full_text[max(0, start - 35):start].lower()
        if re.search(r"\b(?:model|mod|model\s*no\.?|model\s*#|p/?n|mpn|part\s*no\.?|sku|ref\.?|item\s*no\.?|s/?n|serial|barcode|ean|upc|code)\s*[:\-]?\s*$", pre_text):
            return True
        post_text = full_text[end:min(len(full_text), end + 20)].lower()
        if re.search(r"^\s*(?:mah|wh|w\b|v\b|hz|khz|mhz|ghz|rpm|dpi|px|mp|gb|tb|mb|kb|mm|cm|m\b|kg|g\b|pcs|pieces|x\s*\d)", post_text):
            return True
        if start > 0 and full_text[start - 1].isalnum():
            return True
        if end < len(full_text) and full_text[end].isalnum():
            return True
        return False

    comments: dict = {}
    for idx in data.index[pre_mask]:
        found_by_col: list = []   # [(column, ["phone number: 0712…", …])]
        soft_terms: set = set()
        for c in text_cols:
            text = col_text[c].loc[idx]
            hits: list = []
            for kind_label, kind_re in _CONTACT_KINDS:
                if kind_label == "phone number":
                    terms = sorted({
                        m.group(0).strip() for m in kind_re.finditer(text)
                        if m.group(0) and m.group(0).strip() and not _is_platform_url(m.group(0)) and not _is_phone_false_positive(m, text)
                    })
                else:
                    terms = sorted({
                        m.strip() for m in kind_re.findall(text)
                        if m and m.strip() and not _is_platform_url(m)
                        and not (kind_label == "website" and _is_bare_domain_false_positive(m))
                    })
                if terms:
                    hits.append(f"{kind_label}: {', '.join(terms[:2])}")

            # A WhatsApp mention only counts when it comes with a number or a
            # wa.me link. A bare mention, or a device-feature mention such as
            # "WhatsApp notification support", is not contact information.
            wa_match = _WHATSAPP_CONTACT_RE.search(text)
            if wa_match:
                hits.append(f"whatsapp contact: {wa_match.group(0).strip()}")

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
    _PSEUDO_BRANDS = {
        "generic", "generique", "générique", "fashion", "beauty",
        "unbranded", "no brand", "nobrand", "no-brand", "sans marque",
        "original", "originals", "oroginal", "origional", "originel", "origine", "orijinal",
        "genuine", "geniune", "authentic", "authentique", "real", "official", "officiel",
        "100% original", "100% authentic", "100%", "brand",
        "new", "other", "autre", "nan", "none"
    }
    _SKIP_PREFIX_TOKENS = {
        "generic", "generique", "générique", "fashion", "beauty",
        "unbranded", "nobrand",
        "original", "originals", "oroginal", "origional", "originel", "origine", "orijinal", "orig", "og",
        "authentic", "authentique", "genuine", "geniune", "real", "official", "officiel",
        "new", "brandnew", "nouveau", "nouvelle", "neuf", "latest",
        "100%", "100", "brand", "no", "sans", "marque", "high", "top", "best", "quality", "premium",
        "men", "mens", "women", "womens", "ladies", "unisex", "kids", "boy", "boys", "girl", "girls",
        "casual", "canvas", "leather", "cotton", "summer", "vintage", "retro", "classic", "waterproof",
        "digital", "sport", "sports", "running", "walking", "watch", "shoes", "sneakers", "sneaker",
        "scientific", "calculator", "calculators",
    }
    _ALLOWED_SHORT_BRANDS = {"hp", "lg", "mi", "ge", "mk", "ck"}
    # Single-word entries in brands.txt that are also ordinary English words.
    # Matching these at position-0 causes false positives on normal product
    # titles (e.g. "Just a Girl Who Loves Christmas" -> brand "Just").
    # They are excluded from the trie entirely; multi-word brand entries that
    # START with one of these words are still indexed via their first word but
    # a bare single-word match is rejected.
    _COMMON_WORDS_NOT_BRANDS = {
        "just", "simple", "rapid", "like", "love", "good", "best", "top",
        "next", "life", "style", "pure", "true", "real", "ace", "cool",
        "hot", "select", "choice", "plus", "ultra", "mega", "smart",
        "bright", "clean", "fresh", "soft", "light", "classic", "modern",
        "global", "local", "natural", "active", "power", "clear", "fast",
        "quick", "basic", "super", "pro", "prime", "first", "core",
        "peak", "rise", "elite", "hero", "nice", "fine", "bold", "rich",
        "easy", "well", "great", "grand",
    }

    _EVASION_BRANDS: dict[str, str] = {
        "air": "Nike",
        "air max": "Nike",
        "airmax": "Nike",
        "air-max": "Nike",
        "air jordan": "Nike",
        "airjordan": "Nike",
        "air-jordan": "Nike",
        "jumpman": "Nike",
        "air force": "Nike",
        "airforce": "Nike",
        "af1": "Nike",
        "all star": "Converse",
        "all stars": "Converse",
        "allstar": "Converse",
        "allstars": "Converse",
        "chuck taylor": "Converse",
        "g-shock": "Casio",
        "gshock": "Casio",
        "g shock": "Casio",
        "edifice": "Casio",
        "fx-82ms": "Casio",
        "fx82ms": "Casio",
        "fx-991": "Casio",
        "fx991": "Casio",
        "fx-991ex": "Casio",
        "fx991ex": "Casio",
        "classwiz": "Casio",
        "yeezy": "Adidas",
    }

    # 1. Directly flag listings that declared an evasion alias as the brand
    evasion_mask = data["_brand_lower"].isin(_EVASION_BRANDS)
    evasion_flagged = data[evasion_mask].copy()
    if not evasion_flagged.empty:
        evasion_flagged["Detected_Brand"] = evasion_flagged["_brand_lower"].map(_EVASION_BRANDS)
        evasion_flagged["Comment_Detail"] = (
            "Brand declared as '"
            + evasion_flagged["_brand_lower"].str.title()
            + "' which is a protected model line of "
            + evasion_flagged["Detected_Brand"]
        )

    # 2. Check pseudo-brands for protected brands in title (brands.txt only)
    mask = data["_brand_lower"].isin(_PSEUDO_BRANDS)
    if "CATEGORY" in data.columns:
        mask = mask & ~data["CATEGORY"].astype(str).str.lower().str.contains(
            r"\b(?:case|cases|cover|covers)\b", regex=True, na=False
        )
    gen = data[mask].copy()

    flagged_list = []
    if not evasion_flagged.empty:
        flagged_list.append(evasion_flagged)

    if not gen.empty:
        brand_trie = {}
        for b in brands_list:
            if not b:
                continue
            bc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", str(b).lower())).strip()
            if not bc or bc in _PSEUDO_BRANDS or (len(bc) < 3 and bc not in _ALLOWED_SHORT_BRANDS):
                continue
            if bc in _COMMON_WORDS_NOT_BRANDS:
                continue
            first_word = bc.split()[0]
            if first_word not in brand_trie:
                brand_trie[first_word] = []
            brand_trie[first_word].append((bc, b.title() if len(b) > 2 else b.upper()))

        # NOTE: PRICE_CEILING_MODEL_ALIASES is intentionally NOT used here.
        # Model aliases like "boston", "low top", "motion", "desktop" are too
        # broad and cause false positives on ordinary product titles.
        # Price ceiling model checks belong only in check_suspected_fake_products.

        for fw in brand_trie:
            brand_trie[fw].sort(key=lambda x: len(x[0]), reverse=True)

        def detect(n):
            if not n or str(n).strip() == "":
                return None
            nc = re.sub(r"\s+", " ", re.sub(r"['\.\-\[\]\(\)\/\\:,_\|]", " ", str(n).lower())).strip()
            words = nc.split()
            if not words:
                return None
            max_skip = min(len(words), 8)
            for i in range(max_skip):
                sub_text = " ".join(words[i:])
                first_word = words[i]
                if first_word in brand_trie:
                    for bc, original in brand_trie[first_word]:
                        if sub_text.startswith(bc) and (len(sub_text) == len(bc) or not sub_text[len(bc)].isalnum()):
                            return original
                if words[i] not in _SKIP_PREFIX_TOKENS:
                    break
            return None

        gen["Detected_Brand"] = [detect(n) for n in gen["NAME"].values]
        title_flagged = gen[gen["Detected_Brand"].notna()].copy()
        if not title_flagged.empty:
            title_flagged["Comment_Detail"] = (
                "Brand field '"
                + title_flagged["_brand_lower"].str.title()
                + "' but title claims brand: "
                + title_flagged["Detected_Brand"]
            )
            flagged_list.append(title_flagged)

    if not flagged_list:
        return pd.DataFrame(columns=data.columns)

    combined = pd.concat(flagged_list, ignore_index=True)
    return combined.drop_duplicates(subset=["PRODUCT_SET_SID"])



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
    _cats = set(clean_category_code(c) for c in color_categories)
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


# Compound phrases where a color word is part of the product identity, ingredient, or flavor, not its physical color
_COLOR_COMPOUND_PHRASES = re.compile(
    r"\b(?:"
    # Tea & Coffee & Wine & Beverages
    r"green\s+tea|black\s+tea|white\s+tea|red\s+tea|herbal\s+tea|earl\s+grey|"
    r"tea(?:\s+(?:kettles?|pots?|makers?|infusers?|bags?|cups?|sets?|tree(?:\s+oil)?))?|"
    r"coffee(?:\s+(?:tables?|makers?|machines?|pots?|mugs?|cups?|beans?|grinders?|sets?|press|filters?|scoops?))?|"
    r"wine(?:\s+(?:glasses?|glass|openers?|bottles?|racks?|coolers?|chillers?|cellars?|aerators?|stoppers?|pourers?|accessories|sets?|totes?|bags?|corkscrews?|refrigerators?))?|"
    r"red\s+wine|white\s+wine|green\s+coffee|green\s+gram|"
    # Seeds, Spices, Food, Pantry
    r"black\s+seed|black\s+soap|african\s+black\s+soap|black\s+pepper|black\s+garlic|"
    r"pink\s+salt|himalayan\s+pink\s+salt|brown\s+sugar|white\s+sugar|brown\s+rice|white\s+rice|"
    r"red\s+beans|black\s+beans|white\s+beans|yellow\s+(?:peas?|lentils?|corn)|"
    r"olive\s+(?:oil(?:\s+(?:dispensers?|bottles?|sprayers?|pourers?|cruets?|cans?|pots?|jars?))?|leaf|extract|branch|pitter|tapenade)|"
    r"lemon\s+(?:juice|squeezers?|zesters?|extract|oil|grass|lemongrass|tea|soap|fresh|bleach|dishwashing|press|slices?)|"
    r"lime\s+(?:juice|squeezers?|extract|oil|tea|press|fresh)|key\s+lime|soda\s+lime|quick\s+lime|"
    r"orange\s+(?:juice|oil|peel|extract|blossom|peelers?|squeezers?|press|marmalade)|blood\s+orange|sweet\s+orange|"
    r"peach\s+(?:extract|tea|blossom|juice|slices?|jam|gum)|"
    r"apricot\s+(?:scrub|oil|kernel|extract|jam)|"
    r"plum\s+(?:sauce|jam|extract|blossom|oil)|dark\s+plum|sugar\s+plum|"
    r"caramel\s+(?:sauce|syrup|candy|popcorn|extract|coloring|colouring)|salted\s+caramel|"
    r"mustard\s+(?:seeds?|oil|sauce|paste|powder)|dijon\s+mustard|yellow\s+mustard|"
    r"mint\s+(?:condition|tea|cand(?:y|ies)|gum|toothpaste|leaves?|oil|extract)|peppermint(?:\s+oil)?|spearmint(?:\s+oil)?|fresh\s+mint|cool\s+mint|"
    r"chocolate(?:\s+(?:mou?lds?|fountains?|syrups?|bars?|powders?|cand(?:y|ies)|melters?|cookies?|biscuits?|wafers?|chips?|spread|cakes?|cereals?|pastes?|drops?|shakes?))?|"
    r"(?:cereal|breakfast|protein|powder|bar|shake|drink|flavor|flavoured|taste|cookies?|biscuits?|wafers?|snack|syrup|ice\s*cream|whey|hot|dark|milk|white)\s+chocolate|"
    r"(?:shea|body|cocoa|peanut|almond|cashew|mango)\s+butter|butter\s+(?:dish(?:es)?|knives|knife|crock)|"
    r"(?:body|cleansing|face|coconut|almond|oat|soy)\s+milk|milk\s+(?:frothers?|pitchers?|bottles?|jugs?|cartons?|maker|creamer)|"
    r"honey(?:\s+(?:jars?|dippers?|pots?|dispensers?|straws?|balms?|extract|syrup|scent|comb))?|raw\s+honey|pure\s+honey|"
    r"ice(?:\s+(?:makers?|machines?|trays?|buckets?|crushers?|mou?lds?|packs?|shavers?|cubes?|scrapers?))|ice\s*cream|"
    r"ginger(?:\s+(?:tea|powder|oil|garlic|root|extract|cand(?:y|ies)|chews?|beer))|"
    r"cherry(?:\s+(?:blossom|lip|balm|pie|extract|syrup|pitter))|"
    # Personal Care, Cosmetics, Formulations
    r"rose\s+(?:water|oil|hip|rosehip|petals?|extract|essence|mist|toner|serum|masks?|flowers?|floral|bouquet|quartz)|"
    r"(?:face|body|hand|eye|skin|hair|foot|shaving|ice|cold|anti[- ]aging|moisturi[sz]ing|whipped|bb|cc|night|day)\s+cream|"
    r"cream\s+(?:lotion|moisturi[sz]er|jar|tub|dispenser|maker|separator|formula)|"
    r"(?:nipple|face|body|hand|skin|eye|day|night|anti[- ]?aging|anti[- ]?wrinkle|moisturiz(?:ing|er)|bb|cc|cold|whitening|lightening|brightening|bleaching|sun|sunscreen|shaving|aftershave|barrier|massage|lanolin|stretch\s*mark|acne|repair|cleansing|hydrocortisone|curling|curl|firming|lifting|collagen|retinol|hyaluronic|cica|nourishing|hydrating|depilatory|removal|pain\s+relief|analgesic|fungal|antifungal|diaper|nappy|rash|healing|soothing)\s+cream|"
    r"cream\s+(?:for\b|\d+\s*(?:ml|g|oz|kg|fl\s*oz)|gel|lotion|serum|paste|ointment|balm|foundation|contour|blush|bronzer|cleanser|soap|wash)|"
    r"(?:lanolin|moisturizer|lotion|ointment|balm|salve)\s+(?:cream\b)?|"
    r"cr[eè]me\s+(?:hydratante|visage|corps|mains?|solaire|anti[- ]rides|blanchissante|[eé]claircissante|de\s+marrons|fra[iî]che|pour\b)|"
    r"(?:powder|liquid|cream|cheek|face|mineral|matte|shimmer|baked)?\s*blush(?:er)?(?:\s+(?:palette|brush|powder|stick|makeup|duo|trio|compact|pot|wand|tint))?|"
    r"(?:activated|bamboo)?\s*charcoal(?:\s+(?:masks?|peels?|soaps?|toothpastes?|powders?|scrubs?|cleansers?|grills?|briquettes?|filters?|pencils?|drawings?|bags?|shampoo|sponge|toothbrush(?:es)?))?|"
    r"(?:anti[- ]?)?gr[ea]y\s+hair(?:\s+(?:darkening|reversal|cover|coverage|treatment|shampoo|soap|bar|dye|removal))?|"
    r"black\s*heads?(?:\s+(?:remover|vacuum|extractor|strips?|masks?|nose|suction|cleanser|needles?|tools?))?|"
    r"(?:self|sun|de|spray|fake)?\s*tan(?:ning)?\s*(?:oil|lotion|cream|spray|scrub|remover|removal|pack|foam|mousse|drops|accelerator|booster|towel)\b|"
    # Technology, Optics, Hardware
    r"anti[- ]?blue(?:[- ]?light)?|blue[- ]?light(?:\s+(?:blocking|blocker|filter|cut|protective|screen|glasses|protection|shield|lenses?))?|"
    r"blue\s*tooth|bluetooth|"
    r"blue\s+ray|blu\s+ray|blu-ray|"
    r"water\s+jet|jet\s+(?:flame|torch|lighter|nozzle|flosser|cleaner|engine|washer)|ink\s*jet|turbo\s*jet|"
    r"ash\s*(?:tray|trays|bin|bins|bucket|blonde)|"
    r"anti[- ]?rust|rust[- ]?(?:proof|resistant)|rust\s+(?:remover|cleaner|prevention|inhibitor|converter|treatment|protection)|"
    r"black\s*out\s*(?:curtains?|drapes?|shades?|blinds?|eye\s*mask|fabric)|"
    r"white\s+board|black\s+board|chalkboard|white\s+noise|"
    # Materials, Gems, Stones, Wood
    r"(?:solid|natural|real|teak|pine|oak|cedar|sandal|drift|fire|balsa|ply|hard|bamboo)?\s*wood(?:en)?(?:\s+(?:handles?|grips?|spoons?|tables?|chairs?|furniture|cutting\s+board|frames?|racks?|trays?|cabinets?|doors?|boxes?|hangers?|signs?|welcome\s+sign|decor|crafts?|utensils?|bases?|stands?|legs?|lids?|tops?|beads?|toys?|blocks?|kitchenware|kitchen|board|boards|plaque|plaques|sticks?|carvings?|cutters?|sculptures?|pulp|veneer|dowels?|panels?|cases?|watch(?:es)?|clocks?|lamps?|bowls?|plates?|coasters?|chess|puzzles?|wicks?|burning|seasoning|polish|slices?|pellets?|chips?))?|"
    r"with\s+wood(?:en)?\s+\w+|wood(?:en)?(?!\s+(?:colou?r|paint|dye|finish|hue|tint))|"
    r"(?:en\s+)?bois(?!\s+(?:colou?r|peinture))|"
    r"(?:freshwater|baroque|mother\s+of|faux|imitation|cultured|natural|tapioca|boba|white)?\s*pearls?(?:\s+(?:necklaces?|earrings?|bracelets?|rings?|pendants?|brooch(?:es)?|beads?|chokers?|chains?|jewelry|jewellery|hair(?:\s*(?:pins?|clips?|bands?|accessories))?|buttons?|buckles?|belts?|powders?|extracts?|essence|creams?|masks?|serums?|drops?|shells?|barley|couscous|onions?|dials?|watches?))?|"
    r"amber\s+(?:glass|bottles?|jars?|droppers?|vials?|containers?|spray|resin|fossil|teething|perfume|oil|fragrance|scents?)|"
    r"jade\s+(?:rollers?|gua\s*sha|stones?|bracelets?|bangles?|pendants?|necklaces?|rings?|carvings?|beads?|comb|massagers?)|"
    r"onyx\s+(?:stone|ring|marble)|slate\s+(?:coasters?|cheese\s+board|stones?|tiles?|labels?|pencils?)|"
    r"snow\s+(?:boots?|chains?|shovel|spray|foam|globe|tires?|jackets?|coats?|pants?|flakes?|white)|snowflake|"
    r"coral\s+(?:fleece|velvet|reef|calcium)|"
    r"(?:18k|14k|24k|rose\s+gold|white\s+gold|yellow\s+gold|gold|silver|rhodium|platinum|chrome)\s+(?:plated|plating|filled)|"
    r"925\s+(?:sterling\s+)?silver|sterling\s+silver|"
    r"stainless\s+steel|titanium\s+steel|"
    # Brands, Proper Nouns, Specific Compound Names
    r"black\s*(&|\+|and)\s*decker|silver\s*crest|silvercrest|redragon|redmi|blackview|orange\s+money|"
    r"black\s+panther|red\s+bull|blue\s+band|golden\s+penny|white\s+star|red\s+star|black\s+horse|white\s+pearl|green\s+forest|"
    r"red\s+label|black\s+label|gold\s+label|blue\s+label|white\s+label|"
    r"sky\s+(?:view|worth|high|line|pro|star|run|way|fall|walk|fly|box|net|tech|land|flag|wing|wave)|sky(?!\s*(?:blue|bleu))|"
    r"blanche\s+\d+|(?:luxury|women'?s?|men'?s?|quartz|watch|montre)\s+blanche|mont\s*blanc|"
    r"drakkar\s+noir|film\s+noir|cafe\s+noir|pinot\s+noir|savon\s+noir|"
    r"rouge\s+a\s+l[eè]vres|rouge\s+à\s+l[eè]vres|moulin\s+rouge|baton\s+de\s+rouge|"
    r"th[eé]\s+vert|argile\s+verte?|point\s+vert|"
    r"(?:tempered\s+)?glass(?:\s+(?:screen\s+protector|film|bottles?|jars?|cups?|mugs?|tables?|doors?))"
    r")\b",
    re.IGNORECASE,
)

_COLOR_MISMATCH_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in sorted(COLOR_VARIANT_TO_BASE.keys(), key=lambda x: (-len(x), x)) if k.lower() not in ("coffee", "wine")) + r")\b",
    re.IGNORECASE,
)


def check_color_title_mismatch(
    data: pd.DataFrame,
    color_categories: Optional[List[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Flags products where a specific color declared in the product title (NAME)
    contradicts / differs from the color declared in the COLOR column.

    Follows color validation rules:
    - Only checks categories that require / are checked for color (using color_categories).
    - Excludes categories that do not require color, such as groceries/food/supermarket.
    - Excludes 'coffee' and 'wine' (catching food/drinks, tables, glasses, openers, makers, etc.).
    - Treats 'graphite' and 'black' as the same color.
    - Treats 'stainless steel' and 'silver' as the same color.
    - Handles color equivalences: Rose Gold = Pink/Gold, Charcoal = Black/Gray, Midnight = Black/Blue,
      Beige/Khaki/Nude = Brown/Cream, Teal/Turquoise = Blue/Green, Coral/Peach = Pink/Orange.
    """
    if not {"NAME", "COLOR"}.issubset(data.columns):
        return pd.DataFrame(columns=data.columns)

    target = data

    # Follow color validation rules: ONLY check products in color-required
    # categories. An empty/missing eligibility list must disable this check;
    # otherwise a failed support-file load would accidentally make every
    # product eligible for a title/color comparison.
    if not color_categories or "CATEGORY_CODE" not in target.columns:
        return pd.DataFrame(columns=data.columns)
    if "_cat_clean" not in target.columns:
        target = target.copy()
        target["_cat_clean"] = target["CATEGORY_CODE"].apply(clean_category_code)
    _cats = {clean_category_code(c) for c in color_categories if str(c).strip()}
    if not _cats:
        return pd.DataFrame(columns=data.columns)
    target = target[target["_cat_clean"].isin(_cats)]

    if target.empty:
        return pd.DataFrame(columns=data.columns)


    # Safeguard: exclude groceries, food, beverages, books that do not require color
    _EXCLUDED_CAT_KEYWORDS = re.compile(
        r"\b(?:grocery|groceries|supermarket|food|beverage|beverages|drinks?|pantry|snacks?|candy|candies|confectionery|livres?|books?)\b",
        re.IGNORECASE,
    )
    for cat_col in ["CATEGORY", "Initial_Category_Path", "Category_Path"]:
        if cat_col in target.columns:
            target = target[~target[cat_col].astype(str).str.contains(_EXCLUDED_CAT_KEYWORDS, na=False)]

    if target.empty:
        return pd.DataFrame(columns=data.columns)

    target = target[target["NAME"].notna() & target["COLOR"].notna()]
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    null_like = {"", "nan", "none", "null", "n/a", "na", "-", "undefined"}
    _c_series = target["COLOR"].astype(str).str.strip()
    target = target[~_c_series.str.lower().isin(null_like)]
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    # High-performance vectorized pre-filter: only process rows where title actually contains a color candidate
    _name_str = target["NAME"].astype(str)
    has_color_in_name = _name_str.str.contains(_COLOR_MISMATCH_PATTERN, na=False)
    target = target[has_color_in_name]
    if target.empty:
        return pd.DataFrame(columns=data.columns)

    has_brand = "BRAND" in target.columns
    _COLOR_ROOTS = (
        "silver", "gold", "rose", "orange", "black", "pink", "brown", "white", "blue", "green", "red",
        "grey", "gray", "yellow", "purple", "graphite", "stainless steel", "steel", "charcoal", "midnight",
        "beige", "khaki", "nude", "bronze", "copper", "teal", "turquoise", "coral", "peach", "navy",
        "champagne", "rust", "terracotta", "wood", "inox", "acier", "chrome", "titanium", "maroon", "burgundy"
    )

    def _expand_color_bases(matches: List[str], raw_str: str) -> Set[str]:
        bases = {COLOR_VARIANT_TO_BASE.get(m.lower()) for m in matches if COLOR_VARIANT_TO_BASE.get(m.lower())}
        lower_matches = [m.lower() for m in matches]
        lower_raw = raw_str.lower()

        # 1. Graphite = Black & Gray
        if any(m == "graphite" for m in lower_matches) or "graphite" in lower_raw:
            bases.update(["black", "gray"])

        # 2. Stainless steel, Inox, Acier, Chrome, Titanium, Platinum, Silver = Gray / Silver
        _METALLIC_WORDS = ("stainless steel", "stainless-steel", "steel", "silver", "inox", "acier", "chrome", "titanium", "platinum", "platine", "gunmetal", "anthracite")
        if any(m in _METALLIC_WORDS for m in lower_matches) or any(t in lower_raw for t in ("stainless", "silver", "inox", "acier", "chrome", "titanium")):
            bases.update(["gray", "black"])

        # 3. Charcoal = Black & Gray
        if any(m == "charcoal" for m in lower_matches) or "charcoal" in lower_raw:
            bases.update(["black", "gray"])

        # 4. Rose Gold = Pink & Gold (Yellow)
        if any("rose gold" in m or "rosegold" in m for m in lower_matches) or "rose gold" in lower_raw or "rosegold" in lower_raw:
            bases.update(["pink", "yellow"])

        # 5. Midnight = Black & Blue / Navy
        if any(m == "midnight" for m in lower_matches) or "midnight" in lower_raw:
            bases.update(["black", "blue"])

        # 6. Navy = Blue & Black
        if any(m == "navy" for m in lower_matches) or "navy" in lower_raw:
            bases.update(["blue", "black"])

        # 7. Earth tones: Beige, Khaki, Nude, Tan = Brown & White (Cream)
        if any(m in ("beige", "khaki", "nude", "tan") for m in lower_matches) or any(t in lower_raw for t in ("beige", "khaki", "nude", "tan")):
            bases.update(["brown", "white"])

        # 8. Teal, Turquoise, Cyan, Aqua = Blue & Green
        if any(m in ("teal", "turquoise", "cyan", "aqua") for m in lower_matches) or any(t in lower_raw for t in ("teal", "turquoise", "cyan", "aqua")):
            bases.update(["blue", "green"])

        # 9. Coral, Peach, Apricot, Salmon = Pink & Orange
        if any(m in ("coral", "peach", "apricot", "salmon") for m in lower_matches) or any(t in lower_raw for t in ("coral", "peach", "apricot", "salmon")):
            bases.update(["pink", "orange"])

        # 10. Maroon & Burgundy = Red & Brown & Purple
        if any(m in ("maroon", "burgundy", "bordeaux") for m in lower_matches) or any(t in lower_raw for t in ("maroon", "burgundy", "bordeaux")):
            bases.update(["red", "brown", "purple"])

        # 11. Champagne = White & Yellow (Gold) & Brown (Beige)
        if any(m == "champagne" for m in lower_matches) or "champagne" in lower_raw:
            bases.update(["white", "yellow", "brown"])

        # 12. Rust & Terracotta = Orange & Brown & Red
        if any(m in ("rust", "terracotta") for m in lower_matches) or any(t in lower_raw for t in ("rust", "terracotta")):
            bases.update(["orange", "brown", "red"])

        # 13. Wood / Wooden / Natural = Brown
        if any(m in ("wood", "wooden", "bois") for m in lower_matches) or any(t in lower_raw for t in ("wood", "wooden", "bois", "natural")):
            bases.update(["brown"])

        # 14. Bronze & Copper = Brown & Orange
        if any(m in ("bronze", "copper") for m in lower_matches) or any(t in lower_raw for t in ("bronze", "copper")):
            bases.update(["brown", "orange"])

        # 15. Wine in declared color = Red
        if "wine" in lower_raw:
            bases.add("red")

        # 16. Clear & Transparent = White
        if any(m in ("clear", "transparent") for m in lower_matches) or any(t in lower_raw for t in ("clear", "transparent")):
            bases.add("white")

        # 17. Multicolor
        if "multi" in lower_raw:
            bases.add("multicolor")

        return bases

    comments = {}

    for row in target.itertuples(index=True):
        c_raw = str(getattr(row, "COLOR", "")).strip()
        if not c_raw or c_raw.lower() in null_like:
            continue

        raw_name = str(getattr(row, "NAME", ""))
        clean_name = raw_name

        # Mask brand from name if present and brand contains a color word (e.g. brand 'Silver Crest')
        if has_brand:
            b_val = str(getattr(row, "BRAND", "")).strip().lower()
            if b_val and b_val not in {"generic", "fashion", "nan", "none", "null", "unknown brand"}:
                if any(cr in b_val for cr in _COLOR_ROOTS):
                    clean_name = re.sub(r"\b" + re.escape(b_val) + r"\b", " ", clean_name, flags=re.IGNORECASE)

        # Mask compound terms where color word is ingredient/product type (masks coffee, wine glasses/openers, face cream, olive oil, etc.)
        clean_name = _COLOR_COMPOUND_PHRASES.sub(" ", clean_name)
        clean_name = re.sub(r"\bcoffee\b", " ", clean_name, flags=re.IGNORECASE)
        clean_name = re.sub(r"\bwine\b(?!\s*red\b)", " ", clean_name, flags=re.IGNORECASE)

        title_matches = [m.group(0) for m in _COLOR_MISMATCH_PATTERN.finditer(clean_name) if m.group(0).lower() not in ("coffee", "wine")]
        if not title_matches:
            continue

        title_bases = _expand_color_bases(title_matches, clean_name)
        if not title_bases:
            continue

        dec_matches = [m.group(0) for m in _COLOR_MISMATCH_PATTERN.finditer(c_raw) if m.group(0).lower() not in ("coffee", "wine")]
        dec_bases = _expand_color_bases(dec_matches, c_raw)

        if not dec_bases:
            continue

        # If either title or declared color represents multicolor, no strict contradiction
        if "multicolor" in title_bases or "multicolor" in dec_bases:
            continue

        # If there is an overlap in color families (e.g. Title says 'Black & Red', COLOR is 'Black') -> passes
        if not title_bases.isdisjoint(dec_bases):
            continue

        # Mismatch detected: title specifies one color family, but COLOR column specifies a completely different one
        title_words_str = ", ".join(sorted(set(w.capitalize() for w in title_matches)))
        comments[row.Index] = (
            f"Color mismatch: Title specifies '{title_words_str}' but COLOR column is '{c_raw}'."
        )

    if not comments:
        return pd.DataFrame(columns=data.columns)

    flagged = data.loc[list(comments.keys())].copy()
    flagged["Comment_Detail"] = flagged.index.map(comments)
    if "PRODUCT_SET_SID" in flagged.columns:
        return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])
    return flagged


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
    # Exclude non-phone products from being flagged for incomplete smartphone specs
    target = target[~target["NAME"].astype(str).apply(_is_non_phone_product)]
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
_PHONE_TABLET_PAT = re.compile(
    r"phone|smartphone|tablet|ipad|mobile\s*phone|cellular", re.IGNORECASE
)
_LAPTOP_COMP_PAT = re.compile(
    r"laptop|notebook|macbook|chromebook|desktop|computer|all-in-one|pc\b|workstation", re.IGNORECASE
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
# ssd/hdd/nvme/emmc are how laptops actually state storage — "256GB SSD" is
# the norm and "256GB Storage" is the exception. Without them the title side
# extracted nothing and no laptop storage mismatch could ever be found.
_SPEC_STORAGE_GB_RE1 = re.compile(r"(\d+)\s*gb\s*(?:rom|storage|internal(?:\s+storage)?|ssd|hdd|nvme|emmc)\b", re.IGNORECASE)
_SPEC_STORAGE_TB_RE1 = re.compile(r"(\d+)\s*tb\s*(?:rom|storage|internal(?:\s+storage)?|memory|ssd|hdd|nvme|emmc)\b", re.IGNORECASE)
# Same reasoning, mirrored for storage labels.
_SPEC_STORAGE_RE2 = re.compile(r"\b(?:rom|storage|internal(?:\s+storage)?|ssd|hdd|nvme|emmc)\s*[:\-]\s*(\d+)\s*gb\b", re.IGNORECASE)

# "Memory" is ambiguous and was counted as storage, which is wrong far more
# often than it is right. On a laptop "16GB Memory" is RAM, so a listing whose
# title correctly said 256GB SSD was reported as
#   Storage: title says 256GB, DESCRIPTION says 16GB
# — the check comparing a storage figure against a RAM one.
#
# Resolved by size rather than by label, because the word alone cannot settle
# it: at or below _MEMORY_IS_RAM_MAX_GB it is RAM, above it is storage. No
# consumer laptop or phone ships more than 64GB of RAM, and no listing calls
# 128GB+ of flash "memory" while meaning RAM. TB is unambiguous and stays with
# storage above.
_SPEC_MEMORY_RE = re.compile(
    r"(?:(\d+)\s*gb\s*memory\b|\bmemory\s*[:\-]\s*(\d+)\s*gb\b)", re.IGNORECASE
)
_MEMORY_IS_RAM_MAX_GB = 64
_SPEC_COMBO_RE = re.compile(r"\b(\d+)\s*(?:gb)?\s*[/+]\s*(\d+)\s*gb\b", re.IGNORECASE)
# Virtual / extended RAM notation used by budget phones (Tecno, Infinix, Itel):
#   "8GB RAM(4+4)GB"  — total 8GB stated, 4GB physical + 4GB virtual/extended
#   "Upto 8GB RAM(4+4)GB+128GB Storage"
# The outer total and both addends are captured so the description's "4GB RAM"
# (the physical part) is not flagged as a mismatch against the title's "8GB".
# Group 1 = physical_A, Group 2 = extended_B  (sum = stated total)
_SPEC_VIRTUAL_RAM_RE = re.compile(
    r"(?:\d+\s*gb\s*ram|ram\s*[:\-]?\s*\d+\s*gb)\s*[\(\[\{]?\s*(\d+)\s*\+\s*(\d+)\s*[\)\]\}]?\s*gb",
    re.IGNORECASE,
)

# Operating system, for laptops and desktops. Same failure as RAM and storage
# and from the same cause — a description reused from another SKU — but with a
# sharper consequence, since the OS is what the buyer is paying a licence for.
#
# Only versioned names are matched. A bare "windows" or "linux" says nothing
# that can disagree with anything, and matching it would turn every mention
# into a finding.
_SPEC_OS_RE = re.compile(
    r"\b("
    r"windows\s*(?:11|10|8\.1|8|7)"
    r"|win\s*(?:11|10|8\.1|8|7)\b"
    r"|chrome\s*os"
    r"|mac\s*os(?:\s*x)?"
    r"|ubuntu(?:\s*\d{2}\.\d{2})?"
    r"|dos"
    r")\b",
    re.IGNORECASE,
)


def _extract_os(text: str) -> set:
    """Every OS named in `text`, normalised so "Win 11" and "Windows 11" are
    one value and spacing differences do not read as a disagreement."""
    out = set()
    for m in _SPEC_OS_RE.finditer(text):
        v = re.sub(r"\s+", "", m.group(1).lower())
        v = v.replace("win11", "windows11").replace("win10", "windows10")
        v = v.replace("win8.1", "windows8.1").replace("win8", "windows8")
        v = v.replace("win7", "windows7")
        v = v.replace("macosx", "macos")
        out.add(v)
    return out


def _extract_ram_storage(
    text: str,
    allow_combo: bool = False,
    max_ram: int = 64,
    max_memory_as_ram: int = 32,
) -> tuple:
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

    max_ram bounds the plausible RAM size (24GB for phones, 64GB for laptops).
    Any extracted RAM value above max_ram (e.g. 64GB/256GB stated as RAM) is
    impossible as RAM and is reassigned to storage if >= 16GB, or dropped.
    """
    ram = {int(m.group(1)) for m in _SPEC_RAM_RE1.finditer(text)}
    ram |= {int(m.group(1)) for m in _SPEC_RAM_RE2.finditer(text)}
    storage = {int(m.group(1)) for m in _SPEC_STORAGE_GB_RE1.finditer(text)}
    storage |= {int(m.group(1)) * 1024 for m in _SPEC_STORAGE_TB_RE1.finditer(text)}
    storage |= {int(m.group(1)) for m in _SPEC_STORAGE_RE2.finditer(text)}
    # "N GB memory" — RAM at consumer sizes, storage above.
    for m in _SPEC_MEMORY_RE.finditer(text):
        _v = int(m.group(1) or m.group(2))
        (ram if _v <= max_memory_as_ram else storage).add(_v)
    if allow_combo:
        for m in _SPEC_COMBO_RE.finditer(text):
            _a, _b = int(m.group(1)), int(m.group(2))
            # "6/128GB" is RAM/Storage, but "512GB/12GB" is the same shorthand
            # written the other way round, and a real listing used it:
            #   "M91 5G Tablet Memory 512GB/12GB Android"
            # read as RAM=512, which then disagreed with a description saying
            # 12 and reported "RAM: title says 512GB". Order is settled by size
            # rather than position — RAM is never the larger of the two.
            if _a > _b:
                _a, _b = _b, _a
            ram.add(_a)
            storage.add(_b)
        # Virtual/extended RAM notation: "8GB RAM(4+4)GB" — the title states the
        # total (8) which is already captured by _SPEC_RAM_RE1, but the description
        # correctly says the physical portion (4GB). Add both addends to name_ram
        # so that the description's physical value passes the intersection check.
        for m in _SPEC_VIRTUAL_RAM_RE.finditer(text):
            phys = int(m.group(1))
            ext = int(m.group(2))
            if phys <= max_ram:
                ram.add(phys)
            if ext <= max_ram:
                ram.add(ext)

    # Reassign impossible RAM values (> max_ram) to storage if plausible, otherwise drop
    impossible_ram = {v for v in ram if v > max_ram}
    if impossible_ram:
        ram -= impossible_ram
        # Values >= 16GB erroneously parsed or stated as RAM are almost certainly storage (e.g. 64GB, 128GB, 256GB)
        storage |= {v for v in impossible_ram if v >= 16}

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
        cat_str = str(target.at[idx, "CATEGORY"]) if "CATEGORY" in target.columns else ""
        cat_clean_str = str(target.at[idx, "_cat_clean"]) if "_cat_clean" in target.columns else ""
        name_str = str(target.at[idx, "NAME"]) if "NAME" in target.columns else ""
        combined_dev_text = f"{cat_str} {cat_clean_str} {name_str}".lower()

        # Classify device type for RAM limits
        is_phone_tablet = bool(_PHONE_TABLET_PAT.search(combined_dev_text)) and not bool(_LAPTOP_COMP_PAT.search(combined_dev_text))
        if is_phone_tablet:
            row_max_ram = 24           # Maximum 24GB RAM for phones/tablets
            row_max_memory_as_ram = 24
        else:
            row_max_ram = 64           # Maximum 64GB RAM for laptops/computers
            row_max_memory_as_ram = 32 # Ambiguous "64GB Memory" on laptops is flash/eMMC/SSD storage

        name_ram, name_storage = _extract_ram_storage(
            name_lower.loc[idx],
            allow_combo=True,
            max_ram=row_max_ram,
            max_memory_as_ram=row_max_memory_as_ram,
        )
        name_os = _extract_os(name_lower.loc[idx])
        if not name_ram and not name_storage and not name_os:
            continue
        mismatches = []
        for c in text_cols:
            text = col_text[c].loc[idx]
            if not text.strip():
                continue
            f_ram, f_storage = _extract_ram_storage(
                text,
                allow_combo=False,
                max_ram=row_max_ram,
                max_memory_as_ram=row_max_memory_as_ram,
            )
            f_os = _extract_os(text)

            # Modern phones and laptops store files in at least 16GB.
            # Numbers smaller than 16GB (e.g. 4GB, 6GB, 8GB) parsed as storage
            # cannot be storage — they are almost certainly mislabeled RAM.
            # Real storage of 16GB, 32GB, 64GB+ is completely valid and must NOT be demoted to RAM.
            _MIN_STORAGE_GB = 16
            _f_storage_plausible = {v for v in f_storage if v >= _MIN_STORAGE_GB}
            _demoted = f_storage - _f_storage_plausible
            if _demoted:
                # Move to RAM only if RAM was not itself already stated for
                # the field and within plausible RAM size.
                if not f_ram:
                    f_ram = f_ram | {v for v in _demoted if v <= row_max_ram}
                f_storage = _f_storage_plausible

            if name_ram and f_ram and not (name_ram & f_ram):
                mismatches.append(f"RAM: title says {_fmt(name_ram)}, {c} says {_fmt(f_ram)}")
            if name_storage and f_storage and not (name_storage & f_storage):
                mismatches.append(f"Storage: title says {_fmt(name_storage)}, {c} says {_fmt(f_storage)}")
            # Same rule as the other two: only a disagreement when the title's
            # OS appears nowhere in the field, so a description listing several
            # ("Windows 10 or Windows 11") is not a mismatch.
            if name_os and f_os and not (name_os & f_os):
                mismatches.append(
                    f"OS: title says {'/'.join(sorted(name_os))}, "
                    f"{c} says {'/'.join(sorted(f_os))}"
                )
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


_RING_FILLER_WORDS = {
    "portable", "rechargeable", "high", "capacity", "mobile", "phone",
    "pack", "battery", "with", "for", "and", "the", "a", "an",
    "charging", "charger", "fast", "quick", "support", "compatible",
}


def _norm_ring_name_tokens(name: str) -> frozenset:
    """Normalise a product name to a frozenset of meaningful tokens."""
    s = str(name).lower()
    # Collapse numeric formatting: 20,000 -> 20000
    s = re.sub(r'(\d),(\d)', r'\1\2', s)
    # Strip all non-alphanumeric characters
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    tokens = s.split()
    # Drop single-char tokens and filler words
    tokens = [t for t in tokens if len(t) > 1 and t not in _RING_FILLER_WORDS]
    return frozenset(tokens)


def _ring_token_jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def check_duplicate_products(
    data: pd.DataFrame,
    exempt_categories: List[str] = None,
    similarity_threshold: float = 0.70,
    known_colors: List[str] = None,
    full_data: pd.DataFrame = None,
    ring_sellers: dict = None,
    _progress_callback=None,
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
    for _c, _src in (
        ("_brand_lower", "BRAND"),
        ("_seller_lower", "SELLER_NAME"),
        ("_name_lower", "NAME"),
    ):
        if _c not in d.columns and _src in d.columns:
            d[_c] = d[_src].astype(str).str.lower().str.strip().fillna("")

    # The full-upload duplicate path can receive a frame that has not gone
    # through the normal validation preprocessing pass. Always derive the
    # normalized category key before applying category exemptions.
    if "_cat_clean" not in d.columns:
        if "CATEGORY_CODE" in d.columns:
            d["_cat_clean"] = d["CATEGORY_CODE"].astype(str).map(clean_category_code)
        else:
            # Some ZIP/QC exports do not include category codes. They cannot
            # match an exempt-category list, but they must still be valid for
            # duplicate detection.
            d["_cat_clean"] = ""

    if exempt_categories:
        _category_key = d["_cat_clean"] if "_cat_clean" in d.columns else pd.Series("", index=d.index)
        _exempt_keys = set(clean_category_code(c) for c in exempt_categories)
        d = d[~_category_key.isin(_exempt_keys)]
    if d.empty:
        return pd.DataFrame(columns=data.columns)

    _color_pat = compile_regex_patterns(known_colors) if known_colors else None

    _size_keys = [_extract_size_key(str(n)) for n in d["NAME"].values]
    d["_size_key"] = _size_keys

    _names_lower = d["NAME"].astype(str).str.lower()
    if _color_pat and _color_pat.pattern:
        _from_name = _names_lower.str.extract(f"({_color_pat.pattern})", flags=re.IGNORECASE, expand=False).str.lower().str.strip().fillna("")
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
        # Also strip freebie/gift text from the norm name so products that differ
        # only in what gift they include get different _freebie_key values, not
        # the same _norm_name. Without this, "TV + Free Bracket" and "TV + Free
        # HDMI Cable" collapse to the same key and look like duplicates.
        _nn = _nn.str.replace(
            r"(?:[\+&]|\b(?:with|plus|including|includes|and)\b)\s*(?:(?:a|an|the|1)\s+)?(?:free|gift|gifts|freebie|freebies|bonus)\b.*$"
            r"|\b(?:free|gift|gifts|freebie|freebies|bonus)\s*[:\-]?\s*.+$",
            "", regex=True,
        )
        _nn = _nn.str.replace(r"[^\w\s]", "", regex=True)
        _nn = _nn.str.replace(r"\s+", "", regex=True)
        d["_norm_name"] = _nn

    # Pre-compile the freebie regex once rather than once-per-row
    _freebie_re_compiled = re.compile(
        r"(?:[\+&]|(?:\b(?:with|plus|including|includes|and)\b))\s*(?:(?:a|an|the|1)\s+)?(?:free|gift|gifts|freebie|freebies|bonus)\s+([^,\-\u2013|\(\)]+)"
        r"|\b(?:free|gift|gifts|freebie|freebies|bonus)\s*[:\-]?\s*([^,\-\u2013|\(\)]+)",
        re.IGNORECASE,
    )
    _freebie_clean_re = re.compile(r'[^a-zA-Z0-9]')
    _freebie_stopwords = frozenset((
        "free", "gift", "gifts", "bonus", "a", "an", "the", "with", "plus",
        "and", "of", "for", "to", "original", "new",
    ))

    def _extract_freebie_key(name):
        m = _freebie_re_compiled.search(str(name))
        if m:
            val = m.group(1) or m.group(2) or ""
            tokens = [
                w for w in _freebie_clean_re.sub(' ', val).lower().split()
                if w not in _freebie_stopwords
            ]
            return "-".join(sorted(tokens))
        return ""

    # Series used by .map() calls below — avoids a Python loop per row
    _name_words_s = d["NAME"].astype(str)

    def _mk(name):
        words = name.split()
        digit_words = [re.sub(r'[^a-zA-Z0-9]', '', w).lower()
                       for w in words if any(c.isdigit() for c in w)]
        if digit_words:
            return "-".join(digit_words)
        if words:
            return re.sub(r'[^a-zA-Z0-9]', '', words[0]).lower()
        return ""

    def _sk(name):
        words = name.split()
        digit_words = [re.sub(r'[^a-zA-Z0-9]', '', w).lower()
                       for w in words if any(c.isdigit() for c in w)]
        return "-".join(digit_words) if digit_words else ""

    d["_freebie_key"] = [_extract_freebie_key(n) for n in d["NAME"].values]
    d["_model_key"] = _name_words_s.map(_mk)
    d["_seo_model_key"] = _name_words_s.map(_sk)

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
                    + "|"
                    + hash_d["_freebie_key"]  # Products with different freebies are exempted
                )
                img_dup_mask = hash_d.duplicated(subset=["_img_key"], keep="first")
                if img_dup_mask.any():
                    _first_img = hash_d.drop_duplicates(
                        subset=["_img_key"], keep="first"
                    ).set_index("_img_key")["NAME"].to_dict()
                    _img_dups = hash_d[img_dup_mask]
                    for idx, k in zip(_img_dups.index, _img_dups["_img_key"]):
                        flagged_indices[idx] = f"Duplicate (same image): '{str(_first_img.get(k, ''))[:40]}'"
    if _progress_callback:
        _progress_callback("image duplicate keys complete")

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
        + "|"
        + d["_freebie_key"]
    )
    text_dup_mask = d.duplicated(subset=["_text_key"], keep="first")
    if text_dup_mask.any():
        _first_text = d.drop_duplicates(subset=["_text_key"], keep="first").set_index("_text_key")["NAME"].to_dict()
        _txt_dups = d[text_dup_mask]
        for idx, k in zip(_txt_dups.index, _txt_dups["_text_key"]):
            if idx not in flagged_indices: 
                flagged_indices[idx] = f"Duplicate: '{str(_first_text.get(k, ''))[:40]}'"
    if _progress_callback:
        _progress_callback("exact text duplicate keys complete")


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
        + "|"
        + d["_freebie_key"]
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
    if _progress_callback:
        _progress_callback("SEO duplicate keys complete")

    # ── Ring-seller strict duplicate check ───────────────────────────────────
    # Ring sellers are handled with a stricter exact composite key. The old
    # fuzzy all-pairs comparison was O(n²) and could dominate large uploads.
    if ring_sellers:
        ring_lower = {k.lower(): v for k, v in ring_sellers.items()}
        _seller_ring = d["_seller_lower"].map(ring_lower)
        ring_mask = _seller_ring.notna()
        if ring_mask.any():
            ring_d = d[ring_mask].copy()
            ring_d["_ring_id"] = _seller_ring[ring_mask]
            ring_d["_ring_key"] = (
                ring_d["_ring_id"].astype(str)
                + "|" + ring_d["_brand_lower"]
                + "|" + ring_d["_norm_name"]
                + "|" + ring_d["_color_key"]
                + "|" + ring_d["_size_key"]
                + "|" + ring_d["_model_key"]
                + "|" + ring_d["_freebie_key"]
            )
            ring_dup_mask = ring_d.duplicated(subset=["_ring_key"], keep="first")
            if ring_dup_mask.any():
                _first_ring = ring_d.drop_duplicates("_ring_key", keep="first").set_index("_ring_key")["NAME"].to_dict()
                for idx, key in zip(ring_d[ring_dup_mask].index, ring_d.loc[ring_dup_mask, "_ring_key"]):
                    if idx not in flagged_indices:
                        flagged_indices[idx] = f"Duplicate (strict ring seller): '{str(_first_ring.get(key, ''))[:40]}'"
    if _progress_callback:
        _progress_callback("strict ring-seller keys complete")

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


def check_ring_seller_duplicates(
    data: pd.DataFrame,
    ring_sellers: dict = None,
    full_data: pd.DataFrame = None,
    **kwargs,
) -> pd.DataFrame:
    """Convenience wrapper delegating to check_duplicate_products."""
    return check_duplicate_products(
        data=data,
        ring_sellers=ring_sellers,
        full_data=full_data,
        **kwargs,
    )



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
            "check_color_title_mismatch": check_color_title_mismatch,
            "check_weight_volume_in_name": check_weight_volume_in_name,
            "check_incomplete_smartphone_name": check_incomplete_smartphone_name,
            "check_specs_inconsistency": check_specs_inconsistency,
            "check_duplicate_products": check_duplicate_products,
            "check_ring_seller_duplicates": check_ring_seller_duplicates,
            "check_fda": check_fda,
            "check_refurbished_products": check_refurbished_products,
            "check_out_of_market_devices": check_out_of_market_devices,
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
    _pipeline_started = time.perf_counter()
    data = data.copy()
    # ROW_LEVEL_VALIDATORS map a check's result back onto `data` by index label,
    # which is only meaningful when labels are unique (a concat of per-file
    # frames can repeat them).
    if not data.index.is_unique:
        data = data.reset_index(drop=True)
    data["PRODUCT_SET_SID"] = data["PRODUCT_SET_SID"].astype(str).str.strip()

    # Build the review/validation lookup maps once per upload instead of
    # repeatedly scanning the full frame for each SID or seller. Validators
    # and the iframe can reuse these maps without changing verdict semantics.
    _lookup_source = full_batch if isinstance(full_batch, pd.DataFrame) and not full_batch.empty else data
    # The review UI can build its detail maps after validation.  Materialising
    # every row with ``to_dict('records')`` and grouping every seller here was
    # a large, repeated cost for each validation chunk, while none of those
    # maps are read by the validators themselves.
    st.session_state["_validation_lookup_maps"] = {
        "sid_to_report": {},
        "sid_to_flags": {},
        "sid_to_comment": {},
        "url_to_phash": dict(st.session_state.get("_image_phash_by_url", {})),
        "phash_to_sids": {},
        "seller_to_sids": {},
    }

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
    if "CATEGORY_CODE" not in data.columns:
        data["CATEGORY_CODE"] = ""
    if "_cat_clean" not in data.columns:
        data["_cat_clean"] = data["CATEGORY_CODE"].apply(clean_category_code)
    if "_sid_clean" not in data.columns:
        data["_sid_clean"] = data["PRODUCT_SET_SID"]
    
    if "_norm_name" not in data.columns:
        data["_norm_name"] = _normalize_series(data["NAME"])
    st.session_state.setdefault("validation_stage_timings", {})["Read and normalize"] = round(time.perf_counter() - _pipeline_started, 3)

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
            {
                "country_rules": support_files.get("restricted_brands_all", {}).get(country_validator.country, []),
                # Resolves a category code to its full path, so a rule can be
                # kept out of parts of the tree it has no business in.
                "code_to_path": support_files.get("code_to_path", {}),
            },
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
            "Out of market devices",
            check_out_of_market_devices,
            {"code_to_path": support_files.get("code_to_path", {})},
        ),
        (
            "Refurbished products rule",
            check_refurbished_products,
            {"code_to_path": support_files.get("code_to_path", {})},
        ),
        (
            "Seller Not approved to sell Refurb",
            check_refurb_seller_approval,
            {
                "refurb_data": support_files.get("refurb_data", {}),
                "country_code": country_validator.code,
                "code_to_path": support_files.get("code_to_path", {}),
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
            "Seller Not Approved to Sell Alcohol",
            check_seller_approved_for_alcohol,
            {
                "alcohol_data": support_files.get("alcohol_data", {}),
                "country_code": country_validator.code,
                "code_to_path": support_files.get("code_to_path", {}),
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
                "code_to_path": support_files.get("code_to_path", {}),
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
            "Color Mismatch: Title vs COLOR Column",
            check_color_title_mismatch,
            {
                "color_categories": support_files.get("color_categories", []),
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
                "ring_sellers": (
                    support_files.get("ring_sellers", {})
                    if country_validator.code == "KE"
                    else None
                ),
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
        (
            "Product Name Too Short",
            check_name_too_short,
            {
                "code_to_path": support_files.get("code_to_path", {}),
                "book_category_codes": support_files.get("book_category_codes", []),
            },
        ),
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
    if country_validator.code in ("KE", "UG"):
        validations.append(("Animal Health - Banned Products", check_animal_health_banned_products, {}))
        validations.append(("Animal Health - Prohibited Products", check_animal_health_prohibited_products, {}))
        validations.append(("Animal Health - Banned Category", check_animal_health_banned_category, {}))
        validations.append(("Animal Health - Prohibited Category", check_animal_health_prohibited_category, {}))
    # Invalid brand names are a global validation. Placeholder/category values
    # such as "Other" must be rejected consistently in every marketplace.
    validations.append(("Invalid Brand Name", check_invalid_brand_name, {"code_to_path": support_files.get("code_to_path", {})}))

    if country_validator.code == "KE":
        validations.append(("KEBS Banned Products", check_kebs_banned_products, {}))
        validations.append(("KEBS FDA", check_kebs_fda, {}))
        # Evasion check: pseudo-brand + restricted product-line in title — KE only
        # (signatures sourced exclusively from KE restricted brands catalog)
        validations.append((
            "Potential Restricted Brand",
            check_potential_restricted_brand,
            {
                "country_rules": support_files.get("restricted_brands_all", {}).get(country_validator.country, []),
                "code_to_path": support_files.get("code_to_path", {}),
            },
        ))

    if country_validator.code == "MA":
        _ma = load_morocco_qc_rules()
        validations = [v for v in validations if v[0] != "Restricted brands"]
        validations.insert(1, ("Restricted brands", check_restricted_brands, {"country_rules": _ma.get("restricted", []), "code_to_path": support_files.get("code_to_path", {})}))
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

    # Build the learned URL/pHash lookup once for this upload. Both the
    # fingerprint validator and report derivation reuse this immutable map;
    # rebuilding it for every chunk/pass made large catalogs look like a
    # second validation run.
    try:
        from learned_rules import RULES_PATH as _learned_rules_path
        _learned_rules_mtime = _learned_rules_path.stat().st_mtime_ns
    except OSError:
        _learned_rules_mtime = 0
    _learned_map_cache = st.session_state.get("_learned_image_rule_map_cache")
    if not isinstance(_learned_map_cache, dict) or _learned_map_cache.get("mtime") != _learned_rules_mtime:
        _learned_map_cache = {
            "mtime": _learned_rules_mtime,
            "map": merge_learned_image_rules({}),
        }
        st.session_state["_learned_image_rule_map_cache"] = _learned_map_cache
    _learned_image_rule_map = _learned_map_cache.get("map", {})

    # Fingerprint check runs LAST so it only touches products that cleared every
    # other check. It is also in EXPENSIVE_VALIDATORS so already-rejected SIDs
    # are filtered out before the URL/hash lookup loop runs.
    validations.append((
        "Poor images - Blocked Fingerprint",
        check_blocked_image_fingerprints,
        {
            "blocked_map": _learned_image_rule_map,
            "country_code": country_validator.code,
        },
    ))

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
        "fashion brands.xlsx", "duplicate_exempt.txt", "ring_sellers.xlsx", "unnecessary.txt",
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
        "Variation Name Mismatch",
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
    # Image preparation is deliberately deferred until after the cheap text and
    # category stage below.  Prefetching every image before those checks meant
    # a large upload downloaded and hashed products that had already been
    # rejected, which made the initial validation appear stuck at preparation.
    def _skipped(name: str, func) -> bool:
        if getattr(func, "_rule_id", "") or func is check_blocked_image_fingerprints:
            return country_validator.should_skip_validation(name)
        return name.lower() in _skip_set or country_validator.should_skip_validation(name)

    _image_validators = {check_image_stretched, check_image_blurry, check_blocked_image_fingerprints}
    _shared_image_cache = None

    total_tasks = len([v for v in validations if not _skipped(v[0], v[1])])
    processed_count = 0
    restricted_keys = {}
    validation_errors = []
    evaluated_sids_by_flag = {}
    validation_timings = {}
    _last_progress_t = 0.0

    # Duplicate detection is batch-wide, but it is deferred until after cheap
    # checks so a clearly rejected upload does not spend time building duplicate
    # groups before its primary validation results are available.
    _duplicate_precomputed = None

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
            future_started = {}
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

                evaluated_sids_by_flag[name] = set(
                    working_data["PRODUCT_SET_SID"].astype(str).str.strip()
                ) if "PRODUCT_SET_SID" in working_data.columns else set()

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
                _curr_ctx = get_script_run_ctx() if get_script_run_ctx else None
                if name == "Duplicate product" and _duplicate_precomputed is not None:
                    _future = executor.submit(lambda _result=_duplicate_precomputed: _result)
                else:
                    _future = executor.submit(_run_with_ctx, _curr_ctx, run_cached_check, func, cache_path, ckwargs)
                future_to_name[_future] = name
                future_started[_future] = time.perf_counter()

            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]
                validation_timings.setdefault(name, []).append(time.perf_counter() - future_started.get(future, time.perf_counter()))
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
                        if name == "Duplicate product":
                            _expanded = _sids
                        else:
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
                            # Propagate _blocked_flag so check_blocked_image_fingerprints
                            # results survive the rebuild from `data` and the post-
                            # processing guard at the end of run_batch can read it.
                            if "_blocked_flag" in res.columns:
                                _bf = res.set_index("PRODUCT_SET_SID")["_blocked_flag"].to_dict()
                                final_res["_blocked_flag"] = final_res["PRODUCT_SET_SID"].astype(str).map(_bf)
                            if "_blocked_source" in res.columns:
                                _bs = res.set_index("PRODUCT_SET_SID")["_blocked_source"].to_dict()
                                final_res["_blocked_source"] = final_res["PRODUCT_SET_SID"].astype(str).map(_bs)
                            if "_blocked_rule_url" in res.columns:
                                _bru = res.set_index("PRODUCT_SET_SID")["_blocked_rule_url"].to_dict()
                                final_res["_blocked_rule_url"] = final_res["PRODUCT_SET_SID"].astype(str).map(_bru)
                            if "_blocked_rule_phash" in res.columns:
                                _brp = res.set_index("PRODUCT_SET_SID")["_blocked_rule_phash"].to_dict()
                                final_res["_blocked_rule_phash"] = final_res["PRODUCT_SET_SID"].astype(str).map(_brp)

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
        # Futures complete in timing order, but report construction must stay
        # deterministic. Restore the declared validator order before the
        # caller merges cheap/expensive stages and chunk results.
        return {
            name: batch_results.get(name, pd.DataFrame(columns=data.columns))
            for name, _func, _kwargs in v_list
            if name in batch_results
        }

    cheap_v = [v for v in validations if v[0] not in EXPENSIVE_VALIDATORS]

    _validator_stage_started = time.perf_counter()
    results.update(run_batch(cheap_v, data))

    def _prepare_duplicate_precomputed():
        """Build the batch-wide duplicate result after image hashes are ready."""
        nonlocal _duplicate_precomputed
        _duplicate_source = full_batch if full_batch is not None else data
        _duplicate_rows = data
        # A product already rejected by a cheap validator cannot improve the
        # final status of a surviving product. Excluding those SIDs keeps the
        # expensive duplicate grouping focused on candidates while retaining
        # cross-file duplicate detection among the remaining products.
        if rejected_sids and isinstance(_duplicate_source, pd.DataFrame) and "PRODUCT_SET_SID" in _duplicate_source.columns:
            _duplicate_source = _duplicate_source[
                ~_duplicate_source["PRODUCT_SET_SID"].astype(str).isin(rejected_sids)
            ]
            _duplicate_rows = data[
                ~data["PRODUCT_SET_SID"].astype(str).isin(rejected_sids)
            ]
        if _duplicate_source.empty or _duplicate_rows.empty:
            return
        try:
            _duplicate_batch_sig = df_hash(_duplicate_source)
        except Exception:
            _duplicate_batch_sig = f"rows-{len(_duplicate_source)}"
        # Bump this when the duplicate stage's prerequisites change. The v2
        # key prevents an older cache entry (created before image preparation)
        # from hiding image-based duplicates.
        _duplicate_cache_key = f"dup-v3:{_duplicate_batch_sig}:{len(_duplicate_source)}"
        _duplicate_cache = st.session_state.setdefault("_duplicate_validation_cache", {})
        _duplicate_spec = next((item for item in validations if item[0] == "Duplicate product"), None)
        if not _duplicate_spec or _skipped(_duplicate_spec[0], _duplicate_spec[1]):
            return
        _duplicate_precomputed = _duplicate_cache.get(_duplicate_cache_key)
        if _duplicate_precomputed is not None:
            return
        _duplicate_started = time.perf_counter()
        _duplicate_name, _duplicate_func, _duplicate_kwargs = _duplicate_spec

        def _duplicate_detail_progress(_stage):
            st.session_state["duplicate_progress_detail"] = str(_stage)
            if on_progress:
                on_progress(f"Duplicate product · {_stage}", processed_count, total_tasks)

        _duplicate_call_kwargs = dict(_duplicate_kwargs)
        _duplicate_call_kwargs["full_data"] = _duplicate_source
        _duplicate_precomputed = _duplicate_func(
            _duplicate_rows,
            **_duplicate_call_kwargs,
            _progress_callback=_duplicate_detail_progress,
        )
        _duplicate_cache[_duplicate_cache_key] = _duplicate_precomputed
        while len(_duplicate_cache) > 4:
            _duplicate_cache.pop(next(iter(_duplicate_cache)))
        validation_timings.setdefault("Duplicate product", []).append(time.perf_counter() - _duplicate_started)

    # Only now prepare image dimensions and pHashes, and only for products that
    # survived the cheap stage. This preserves the same image verdicts while
    # avoiding network/decode work for products already rejected by text,
    # category, seller, or brand rules.
    _needs_image_cache = any(
        not _skipped(v[0], v[1]) and v[1] in _image_validators
        for v in validations
    )
    if _needs_image_cache:
        _image_stage_started = time.perf_counter()
        try:
            _image_candidates = data
            if rejected_sids and "PRODUCT_SET_SID" in data.columns:
                _image_candidates = data[
                    ~data["PRODUCT_SET_SID"].astype(str).isin(rejected_sids)
                ]

            def _image_progress_detail(_stage):
                st.session_state["image_validation_progress_detail"] = str(_stage)
                if on_progress:
                    on_progress(f"Image preparation · {_stage}", 0, 1)

            _shared_image_cache = _fetch_all_image_dimensions(
                _image_candidates, progress_callback=_image_progress_detail
            )
        except Exception as _img_err:
            logger.warning("Image dimension prefetch failed: %s", _img_err)
        st.session_state.setdefault("validation_stage_timings", {})["Image validation setup"] = round(time.perf_counter() - _image_stage_started, 3)
    if _shared_image_cache:
        validations = [
            (name, func, ({**kw, "_image_cache": _shared_image_cache} if func in _image_validators else kw))
            for name, func, kw in validations
        ]

    # Duplicate image keys depend on the same pHash cache as image validators.
    # Run this after image preparation so moving the duplicate stage later does
    # not silently lose image-based duplicate matches.
    _prepare_duplicate_precomputed()

    expensive_v = [v for v in validations if v[0] in EXPENSIVE_VALIDATORS]
    results.update(run_batch(expensive_v, data))
    st.session_state.setdefault("validation_stage_timings", {})["Text/category validation"] = round(time.perf_counter() - _validator_stage_started, 3)
    try:
        st.session_state["validation_timings"] = {
            name: {"seconds": round(sum(values), 3), "runs": len(values)}
            for name, values in validation_timings.items()
        }
    except Exception:
        pass

    # ── Post-process Blocked Image Fingerprint results ──────────────────────
    # check_blocked_image_fingerprints stores the real FLAG in _blocked_flag so
    # each row gets recorded under the correct validation key (Poor images,
    # Restricted brands, etc.) and inherits all downstream handling for that
    # flag — severity, cascade, comment formatting — automatically.
    _bif_res = results.pop("Poor images - Blocked Fingerprint", None)
    if _bif_res is None:
        _bif_res = results.pop("Blocked Image Fingerprint", None)
    if _bif_res is not None and not _bif_res.empty and "_blocked_flag" in _bif_res.columns:
        for _real_flag, _grp in _bif_res.groupby("_blocked_flag"):
            _grp = _grp.drop(columns=["_blocked_flag"], errors="ignore").copy()
            _prev = results.get(_real_flag)
            if _prev is not None and not _prev.empty:
                _grp = pd.concat([_prev, _grp], ignore_index=True)
                if "PRODUCT_SET_SID" in _grp.columns:
                    _grp = _grp.drop_duplicates(subset=["PRODUCT_SET_SID"])
            results[_real_flag] = _grp
            rejected_sids.update(_grp["PRODUCT_SET_SID"].astype(str).str.strip().unique())


    # Post-process Brand Image Mismatch: Apple detected on cases/covers accessories for phones/tablets/laptops
    # is overturned and auto-approved.
    if "Brand Image Mismatch" in results and not results["Brand Image Mismatch"].empty:
        bim = results["Brand Image Mismatch"]
        _APPLE_ACC_RE = re.compile(
            r"\b(?:case|cases|cover|covers|sleeve|sleeves|pouch|pouches|screen.?protector|housing|skin)\b",
            re.IGNORECASE,
        )
        _det_col = "Brand_Detected_On_Product" if "Brand_Detected_On_Product" in bim.columns else None
        _reason_col = "Brand_Image_Check_Reason" if "Brand_Image_Check_Reason" in bim.columns else None
        _cd_col = "Comment_Detail" if "Comment_Detail" in bim.columns else None

        _det_is_apple = pd.Series(False, index=bim.index)
        if _det_col:
            _det_is_apple |= bim[_det_col].fillna("").astype(str).str.strip().str.lower().eq("apple")
        if _reason_col:
            _det_is_apple |= bim[_reason_col].fillna("").astype(str).str.lower().str.contains(r"visible on the product is ['\"]?apple['\"]?", regex=True, na=False)
        if _cd_col:
            _det_is_apple |= bim[_cd_col].fillna("").astype(str).str.lower().str.contains(r"image shows ['\"]?apple['\"]?|restricted brand ['\"]?apple['\"]?", regex=True, na=False)

        _cat_col = "CATEGORY" if "CATEGORY" in bim.columns else None
        _name_col = "NAME" if "NAME" in bim.columns else None
        _is_acc = pd.Series(False, index=bim.index)
        if _cat_col:
            _is_acc |= bim[_cat_col].fillna("").astype(str).str.contains(_APPLE_ACC_RE, na=False)
        if _name_col:
            _is_acc |= bim[_name_col].fillna("").astype(str).str.contains(_APPLE_ACC_RE, na=False)

        overturn_mask = _det_is_apple & _is_acc
        if overturn_mask.any():
            overturned = bim[overturn_mask].copy()
            results["Brand Image Mismatch"] = bim[~overturn_mask].copy()
            overturned["Comment_Detail"] = "Rejection overturned: Apple brand detected on cases/covers accessory image -- product approved."
            results["Brand Image - Apple Acc Overturned"] = pd.concat(
                [results.get("Brand Image - Apple Acc Overturned", pd.DataFrame()), overturned],
                ignore_index=True
            ).drop_duplicates(subset=["PRODUCT_SET_SID"])

    # Drain Apple-accessories overturn staging (set by check_brand_image_mismatch).
    if _APPLE_ACC_OVERTURNED_STAGING:
        try:
            _ov_sids = list(_APPLE_ACC_OVERTURNED_STAGING.keys())
            _ov_df = data[data["PRODUCT_SET_SID"].astype(str).isin(_ov_sids)].copy()
            if not _ov_df.empty:
                _cd_map = {sid: r.get("Comment_Detail", "") for sid, r in _APPLE_ACC_OVERTURNED_STAGING.items()}
                _ov_df["Comment_Detail"] = _ov_df["PRODUCT_SET_SID"].astype(str).map(_cd_map).fillna("")
                results["Brand Image - Apple Acc Overturned"] = _ov_df.drop_duplicates(subset=["PRODUCT_SET_SID"])
        except Exception as _ov_err:
            logger.warning("Could not drain Apple-acc overturn staging: %s", _ov_err)
        finally:
            _APPLE_ACC_OVERTURNED_STAGING.clear()



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

    _learnable_flags = {
        "Poor images", "Image Stretched", "Image Blurry", "Image Mismatch",
        "Image Infringing", "Image Too Many things displayed",
        "Restricted brands", "Suspected Fake product", "Prohibited products",
        "FDA", "Brand Image Mismatch", "Counterfeit Sneakers",
        "Suspected counterfeit Jerseys",
    }

    # JSON-learned matches are evidence for future runs, not a reason to keep
    # themselves alive forever. If the underlying validator no longer rejects
    # the image, discard the JSON-only result now; Excel rules remain intact.
    for _self_healing_flag in _learnable_flags:
        _self_healing_res = results.get(_self_healing_flag)
        if not isinstance(_self_healing_res, pd.DataFrame) or _self_healing_res.empty:
            continue
        if "_blocked_source" not in _self_healing_res.columns:
            continue
        _source_json = _self_healing_res["_blocked_source"].astype(str).str.casefold().eq("json")
        if not _source_json.any():
            continue
        _non_json_sids = set(
            _self_healing_res.loc[~_source_json, "PRODUCT_SET_SID"].astype(str).str.strip()
        )
        _keep = (~_source_json) | _self_healing_res["PRODUCT_SET_SID"].astype(str).str.strip().isin(_non_json_sids)
        results[_self_healing_flag] = _self_healing_res[_keep].copy()

    # Stage confirmed validator findings. They are committed only when the
    # user clicks Generate Reports, so a preview/re-run cannot teach the
    # catalog before the result is final.
    _learn_hashes = st.session_state.get("_image_phash_by_url", {})
    _pending_learning = {}
    for _learn_flag in _learnable_flags:
        _learn_res = results.get(_learn_flag)
        if not isinstance(_learn_res, pd.DataFrame):
            continue
        _learn_sids = (
            _learn_res["PRODUCT_SET_SID"].astype(str).str.strip().unique()
            if "PRODUCT_SET_SID" in _learn_res.columns else []
        )
        _learn_reason = str((support_files.get("flags_mapping", {}).get(_learn_flag) or {}).get("reason", ""))
        _pending_learning[_learn_flag] = {
            "sids": list(_learn_sids),
            "evaluated_sids": list(evaluated_sids_by_flag.get(_learn_flag, [])),
            "reason": _learn_reason,
        }
    st.session_state["_pending_validation_learning"] = {
        "data": data,
        "hash_by_url": _learn_hashes,
        "flags": _pending_learning,
    }
    try:
        _maps = st.session_state.get("_validation_lookup_maps", {})
        _sid_flags, _sid_comments = {}, {}
        for _flag_name, _flag_frame in results.items():
            if not isinstance(_flag_frame, pd.DataFrame) or "PRODUCT_SET_SID" not in _flag_frame.columns:
                continue
            _flag_sid = _flag_frame["PRODUCT_SET_SID"].astype(str).str.strip()
            _flag_comment_by_sid = {}
            if "Comment_Detail" in _flag_frame.columns:
                _flag_comment_by_sid = (
                    pd.DataFrame({"_sid": _flag_sid, "_comment": _flag_frame["Comment_Detail"]})
                    .drop_duplicates("_sid", keep="first")
                    .set_index("_sid")["_comment"]
                    .astype(str)
                    .to_dict()
                )
            for _sid in _flag_sid.unique():
                _sid_flags.setdefault(_sid, []).append(_flag_name)
                _sid_comments.setdefault(_sid, []).append(_flag_comment_by_sid.get(_sid, ""))
        _maps["sid_to_flags"] = _sid_flags
        _maps["sid_to_comment"] = _sid_comments
        _maps["phash_to_sids"] = {}
        if "MAIN_IMAGE" in _lookup_source.columns:
            for _, _lookup_row in _lookup_source[["PRODUCT_SET_SID", "MAIN_IMAGE"]].drop_duplicates().iterrows():
                _phash = _maps.get("url_to_phash", {}).get(str(_lookup_row["MAIN_IMAGE"]).strip(), "")
                if _phash:
                    _maps["phash_to_sids"].setdefault(_phash, []).append(str(_lookup_row["PRODUCT_SET_SID"]).strip())
        st.session_state["_validation_lookup_maps"] = _maps
    except Exception:
        logger.debug("Could not finalize validation lookup maps", exc_info=True)

    _report_started = time.perf_counter()
    _derived = derive_status_report(data, results, support_files, country_validator)
    st.session_state.setdefault("validation_stage_timings", {})["Report generation"] = round(time.perf_counter() - _report_started, 3)
    st.session_state.setdefault("validation_stage_timings", {})["Pipeline total"] = round(time.perf_counter() - _pipeline_started, 3)
    return _derived


def commit_pending_validation_learning():
    """Commit staged validator findings after Generate Reports is clicked."""
    pending = st.session_state.pop("_pending_validation_learning", None)
    if not pending:
        return 0
    data = pending.get("data")
    hash_by_url = pending.get("hash_by_url", {})
    committed = 0
    _new_rule_batches = []
    for flag, item in pending.get("flags", {}).items():
        try:
            reconcile_image_rules(
                data,
                item.get("sids", []),
                flag,
                hash_by_url=hash_by_url,
                evaluated_sids=item.get("evaluated_sids", []),
            )
            if item.get("sids"):
                _new_rule_batches.append({
                    "data": data,
                    "sids": item.get("sids", []),
                    "flag": flag,
                    "reason": item.get("reason", ""),
                    "source": "validation",
                    "hash_by_url": hash_by_url,
                })
            committed += 1
        except Exception:
            logger.exception("Could not commit learned image rules for %s", flag)
    if _new_rule_batches:
        # The bulk writer filters URL/pHash/flag identities against the
        # existing catalog and writes only new findings from this file once.
        learn_image_rejections_bulk(_new_rule_batches)
    st.session_state["_validation_learning_committed"] = True
    return committed


def derive_status_report(data, results, support_files, country_validator):
    flags_mapping = support_files.get("flags_mapping", {})
    target_lang = "fr" if country_validator.country == "Morocco" else "en"

    # Only products that were actually in the uploaded ZIP/QC file can be
    # "overturned" — the system made a specific decision on them. Products
    # that were never in the ZIP have no prior decision to overturn.
    try:
        _zip_idx = st.session_state.get("_zip_sid_index")
        _zip_sids: set = set(_zip_idx.index.astype(str).str.strip()) if _zip_idx is not None else set()
    except Exception:
        _zip_sids = set()

    # Check all products in the upload for matches against learned blocked image catalog
    _blocked_img_matches = {}
    _learned_display_matches = {}
    _all_learned_matches = {}
    try:
        _bmap = st.session_state.get("_learned_image_rule_map_cache", {}).get("map")
        if not isinstance(_bmap, dict):
            _bmap = merge_learned_image_rules({})
        _all_learned_matches = get_blocked_image_matches(data, _bmap)
        # Keep the learned provenance visible for Uganda even when a
        # restricted-image match is being used only for brand consistency.
        _learned_display_matches = {
            _sid: _match for _sid, _match in _all_learned_matches.items()
            if _match.get("decision", "reject") == "reject"
        }
        if country_validator.code == "UG" and _learned_display_matches and "BRAND" in data.columns:
            _brand_by_sid = dict(zip(
                data["PRODUCT_SET_SID"].astype(str).str.strip(),
                data["BRAND"].fillna("").astype(str).str.strip(),
            ))
            _learned_display_matches = {
                _sid: _match for _sid, _match in _learned_display_matches.items()
                if _match.get("flag") != "Restricted brands"
                or not _learned_brands_agree(
                    _brand_by_sid.get(str(_sid).strip(), ""),
                    _match.get("brand", ""),
                )
            }
        _blocked_img_matches = {
            _sid: _match for _sid, _match in _all_learned_matches.items()
            if _match.get("decision", "reject") == "reject"
            and not (country_validator.code == "UG" and _match.get("flag") == "Restricted brands")
        }
        st.session_state["_learned_review_matches"] = {
            _sid: _match for _sid, _match in _all_learned_matches.items()
            if _match.get("decision") == "review"
        }
        _touch_key = tuple(sorted(str(_sid) for _sid in _all_learned_matches))
        if _touch_key and st.session_state.get("_learned_match_touch_key") != _touch_key:
            st.session_state["_learned_match_touch_key"] = _touch_key
            try:
                record_learned_image_matches_async(_all_learned_matches)
            except Exception:
                logger.debug("Could not update learned match timestamps", exc_info=True)
    except Exception as _e:
        logger.warning("Could not compute learned image matches in derive_status_report: %s", _e)

    rows = []
    processed_sids = set()
    # Collect overturned cases here so the targeted audit can surface them.
    _overturned_for_audit: list = []

    p_sku_map = data.set_index("PRODUCT_SET_SID")["PARENTSKU"].to_dict() if "PARENTSKU" in data.columns else {}
    s_name_map = data.set_index("PRODUCT_SET_SID")["SELLER_NAME"].to_dict() if "SELLER_NAME" in data.columns else {}
    
    known_flags = [
        "Brand Image - Apple Acc Overturned",
        "Refurbished Brand - Overturned",
        "Color - Overturned",
        "Wrong Category", "Restricted brands", "Potential Restricted Brand", "Suspected Fake product", 
        "Out of market devices", "Seller Not approved to sell Refurb", "Product Warranty", "Seller Approve to sell books",
        "Seller Not Approved to Sell Alcohol",
        "Seller Approved to Sell Perfume", "Perfume Tester", "Counterfeit Sneakers",
        "Suspected counterfeit Jerseys", "Prohibited products", "Unnecessary words in NAME",
        "Single-word NAME", "Generic BRAND Issues", "Fashion brand issues", "BRAND name repeated in NAME",
        "Wrong Variation", "Generic branded products with genuine brands", "Missing COLOR",
        "Color Mismatch: Title vs COLOR Column",
        "Missing Weight/Volume", "Incomplete Smartphone Name", "Specs Inconsistency", "Duplicate product", "Discount too high",
        "Suspicious Discount", "NG - Gift Card Seller", "NG - TV Brand Seller", "NG - HP Toners Seller",
        "NG - Apple Seller", "NG - Xmas Tree Seller", "NG - Rice Brand Seller", "GH - Smart Glasses with Camera",
        "MA - Marque Interdite", "Powerbank Not Authorized",
        "Poor images", "Image Stretched", "Image Blurry", "Image Mismatch", "Image Infringing", "Image Too Many things displayed",
        "Brand Image - Apple Acc Overturned"
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

            _OVERTURNED_FLAGS = {
                "Brand Image - Apple Acc Overturned": ("apple_acc", "Rejection overturned: Apple brand detected on cases/covers accessory image -- product approved."),
                "Color - Overturned": ("color", "Rejection overturned: Color accepted -- product approved."),
            }

            if name in _OVERTURNED_FLAGS:
                _ov_tag, _ov_default_cmt = _OVERTURNED_FLAGS[name]
                _cmt = det_str or _ov_default_cmt
                # Only flag as "Overturned" if this product was in the ZIP
                # (i.e. the system originally made a decision on it).
                # Products not in the ZIP are simply approved quietly.
                _is_zip_product = (not _zip_sids) or (sid in _zip_sids)
                if _is_zip_product:
                    rows.append({
                        "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""),
                        "Status": "Approved",
                        "Reason": "",
                        "Comment": _cmt,
                        "FLAG": name,
                        "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": "",
                        "zip_override": _ov_tag,
                        "overturn_direction": "to_approval",
                    })
                    # Record for the targeted audit.
                    _overturned_for_audit.append({
                        "sid": sid,
                        "flag": name,
                        "tag": _ov_tag,
                        "comment": _cmt,
                        "seller": s_name_map.get(sid, ""),
                    })
                else:
                    # Not a ZIP product — just mark approved without overturn badge.
                    rows.append({
                        "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""),
                        "Status": "Approved",
                        "Reason": "", "Comment": _cmt,
                        "FLAG": name, "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": "",
                    })
                continue

            comment_str = det_str if len(det_str) > 60 else (f"{base_comment} ({det_str})" if det_str else base_comment)
            if sid in _blocked_img_matches:
                _bim = _blocked_img_matches[sid]
                _match_note = f"Image fingerprint matched known blocked image ({_bim['brand']})" if _bim.get("brand") else "Image fingerprint matched known blocked image"
                if "image fingerprint" not in comment_str.lower() and "known blocked image" not in comment_str.lower():
                    comment_str = f"{comment_str} | {_match_note}"

            rows.append({
                "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Rejected",
                "Reason": row_reason if row_reason else rinfo["reason"], "Comment": comment_str,
                "FLAG": name, "SellerName": s_name_map.get(sid, ""),
                "CAT_MAX_PRICE": mx_prc if name == "Category Max Price Exceeded" else ""
            })

    all_sids = data["PRODUCT_SET_SID"].astype(str).str.strip().unique()
    approved_sids = [s for s in all_sids if s not in processed_sids]
    
    for sid in approved_sids:
        if sid in _blocked_img_matches:
            _bim = _blocked_img_matches[sid]
            _flag = _bim.get("flag", "Poor images")
            _rinfo = flags_mapping.get(
                _flag,
                {"reason": "1000007 - Poor image quality", "en": "Poor quality image", "fr": "Image de mauvaise qualité", "ar": "صورة ذات جودة رديئة"}
            )
            _cmt = f"{_bim['detail']} (image fingerprint match)"
            rows.append({
                "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Rejected",
                "Reason": _rinfo.get("reason", "1000007 - Poor image quality"),
                "Comment": _cmt,
                "FLAG": _flag, "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": ""
            })
        else:
            rows.append({
                "ProductSetSid": sid, "ParentSKU": p_sku_map.get(sid, ""), "Status": "Approved",
                "Reason": "", "Comment": "", "FLAG": "", "SellerName": s_name_map.get(sid, ""), "CAT_MAX_PRICE": ""
            })

    final_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["ProductSetSid", "Status", "Reason", "Comment", "FLAG", "SellerName", "CAT_MAX_PRICE"])

    # A learned image match is a rejection regardless of whether another
    # validator matched this SID first. Make the learned issue the primary
    # flag/reason while retaining the earlier validator's explanation.
    if _blocked_img_matches and "ProductSetSid" in final_df.columns:
        _report_sids = final_df["ProductSetSid"].astype(str).str.strip()
        for _sid, _match in _blocked_img_matches.items():
            _row_mask = _report_sids.eq(str(_sid).strip())
            if not _row_mask.any():
                continue
            _learned_flag = _match.get("flag", "Poor images")
            _learned_info = flags_mapping.get(
                _learned_flag,
                {"reason": "1000007 - Poor image quality"},
            )
            _learned_comment = f"{_match.get('detail', 'Image matched known blocked image.')} (image fingerprint match)"
            _prior_row = final_df.loc[_row_mask].iloc[0]
            _prior_flag = str(_prior_row.get("FLAG", "") or "").strip()
            _prior_comment = str(_prior_row.get("Comment", "") or "").strip()
            if _prior_flag and _prior_flag != _learned_flag and _prior_comment:
                _learned_comment += f" | Other validation ({_prior_flag}): {_prior_comment}"
            final_df.loc[_row_mask, "Comment"] = _learned_comment
            final_df.loc[_row_mask, "Status"] = "Rejected"
            final_df.loc[_row_mask, "FLAG"] = _learned_flag
            final_df.loc[_row_mask, "Reason"] = _learned_info.get(
                "reason", "1000007 - Poor image quality"
            )

    final_df["PRODUCT_SET_SID"] = final_df["ProductSetSid"]
    # Display-only provenance used by the validation expanders. A true value
    # means the product was rejected by an active learned JSON fingerprint.
    final_df["Learned Match"] = final_df["ProductSetSid"].astype(str).str.strip().isin(
        {str(_sid).strip() for _sid in _learned_display_matches}
    )
    final_df["Learned Match Method"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): str(_match.get("match_method", "")) for _sid, _match in _learned_display_matches.items()}
    ).fillna("")
    final_df["Learned Match Distance"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): _match.get("phash_distance") for _sid, _match in _learned_display_matches.items()}
    )
    final_df["Learned Rule URL"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): str(_match.get("matched_rule_url", "") or "").strip() for _sid, _match in _learned_display_matches.items()}
    ).fillna("")
    final_df["Learned Rule pHash"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): str(_match.get("matched_rule_phash", "") or "").strip() for _sid, _match in _learned_display_matches.items()}
    ).fillna("")
    final_df["Learned Confidence"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): _match.get("confidence") for _sid, _match in _learned_display_matches.items()}
    )
    final_df["Learned Decision"] = final_df["ProductSetSid"].astype(str).str.strip().map(
        {str(_sid).strip(): _match.get("decision", "reject") for _sid, _match in _learned_display_matches.items()}
    ).fillna("")
    _review_matches = {
        str(_sid).strip(): _match
        for _sid, _match in _all_learned_matches.items()
        if _match.get("decision") == "review"
    }
    _review_match_sids = set(_review_matches)
    final_df["Learned Review Match"] = final_df["ProductSetSid"].astype(str).str.strip().isin(
        {str(_sid).strip() for _sid in _review_match_sids}
    )
    def _review_map(key, default=""):
        _series = final_df["ProductSetSid"].astype(str).str.strip().map(
            {sid: match.get(key, default) for sid, match in _review_matches.items()}
        )
        return _series if default is None else _series.fillna(default)
    final_df["Learned Review Detail"] = _review_map("detail")
    final_df["Learned Review Method"] = _review_map("match_method")
    final_df["Learned Review Distance"] = _review_map("phash_distance", None)
    final_df["Learned Review Brand"] = _review_map("brand")

    for _bool_col in ("Is_Zip", "Is_Manual"):
        if _bool_col not in final_df.columns:
            final_df[_bool_col] = False

    # Publish overturned cases so the targeted audit can surface them as
    # False Rejections. This is written on every derive_status_report call
    # so stale data from a previous batch never survives a re-run.
    try:
        st.session_state["_pipeline_overturned_cases"] = _overturned_for_audit
    except Exception:
        pass

    return country_validator.ensure_status_column(final_df), results


def cached_validate_products(
    data_hash: str,
    _data: pd.DataFrame,
    _support_files: Dict,
    country_code: str,
    data_has_warranty_cols: bool,
    learned_rules_revision: float = 0.0,
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


def validate_in_chunks(data, support_files, country_validator, data_has_warranty,
                      *, cache_prefix, skip_validators=None, on_progress=None,
                      full_batch=None, rules_revision=0.0, manifest=None,
                      batch_prefix="batch", chunk_size=1000):
    """Run validation in bounded SID chunks and checkpoint each completed chunk."""
    if data is None or data.empty:
        return pd.DataFrame(), {}
    sids = data["PRODUCT_SET_SID"].astype(str).str.strip().drop_duplicates().tolist()
    frames, result_parts = [], []
    pending_flags = {}
    aggregate_timings = {}
    aggregate_stages = {}
    if manifest is not None:
        try:
            if on_progress:
                on_progress(f"Starting validation chunk {batch_prefix}…", 0, 1)
            # Keep the normalized input available independently of validator
            # artifacts so a restart does not need to rebuild the frame before
            # resuming completed chunks.
            _stage_path = manifest.get("stage_paths", {}).get("normalized")
            if not _stage_path or not os.path.exists(_stage_path):
                save_stage_frame(manifest, "normalized", data)
            manifest["validation_name"] = "initial_validation"
            manifest["row_count"] = int(len(data))
            manifest["updated_at"] = datetime.now().isoformat()
            from processing_automation import save_manifest as _save_validation_manifest
            _save_validation_manifest(manifest)
        except Exception:
            logger.debug("Could not persist normalized stage artifact", exc_info=True)
    pending_data = data
    pending_hashes = st.session_state.get("_image_phash_by_url", {})
    # Larger chunks reduce repeated validator setup and dataframe hashing for
    # large uploads while retaining checkpoint recovery for smaller batches.
    if len(sids) > 20_000 and chunk_size < 10_000:
        chunk_size = 10_000
    elif len(sids) > 5_000 and chunk_size < 5_000:
        chunk_size = 5_000
    total = max(1, (len(sids) + chunk_size - 1) // chunk_size)
    done = completed_batches(manifest or {})
    for index in range(total):
        batch_id = f"{batch_prefix}-{index + 1}"
        chunk_sids = set(sids[index * chunk_size:(index + 1) * chunk_size])
        chunk = data[data["PRODUCT_SET_SID"].astype(str).str.strip().isin(chunk_sids)].copy()
        # A manifest is only resumable when its completed artifact is loaded
        # back into the merge. If the JSON says complete but a Parquet file is
        # missing/corrupt, fall through and recompute that chunk safely.
        if batch_id in done and manifest is not None:
            saved = load_chunk_results(manifest, batch_id)
            if saved is not None:
                saved_report, saved_results = saved
                frames.append(saved_report)
                result_parts.append(saved_results)
                # Rebuild staged learning candidates from the restored
                # result frames as well; a process restart has no in-memory
                # _pending_validation_learning state to reuse.
                for _flag, _saved_frame in saved_results.items():
                    if not isinstance(_saved_frame, pd.DataFrame) or _saved_frame.empty or "PRODUCT_SET_SID" not in _saved_frame.columns:
                        continue
                    _candidate = pending_flags.setdefault(_flag, {"sids": [], "evaluated_sids": [], "reason": ""})
                    _candidate["sids"] = list(dict.fromkeys(_candidate["sids"] + _saved_frame["PRODUCT_SET_SID"].astype(str).str.strip().tolist()))
                    _candidate["evaluated_sids"] = list(dict.fromkeys(_candidate["evaluated_sids"] + list(chunk_sids)))
                continue
        if manifest is not None:
            mark_batch(manifest, batch_id, status="running", rows=len(chunk), index=index + 1, total=total)
        try:
            if on_progress:
                on_progress(f"Preparing validation chunk {index + 1}/{total}", index, total)
            fr_chunk, result_chunk = cached_validate_products(
                f"{cache_prefix}|{batch_id}|{df_hash(chunk)}",
                chunk, support_files, country_validator.code, data_has_warranty,
                learned_rules_revision=rules_revision,
                skip_validators=skip_validators,
                _on_progress=on_progress,
                _full_batch=full_batch,
            )
            for _v_name, _v_info in st.session_state.get("validation_timings", {}).items():
                aggregate_timings.setdefault(_v_name, {"seconds": 0.0, "runs": 0})
                aggregate_timings[_v_name]["seconds"] += float(_v_info.get("seconds", 0))
                aggregate_timings[_v_name]["runs"] += int(_v_info.get("runs", 0))
            for _stage_name, _stage_seconds in st.session_state.get("validation_stage_timings", {}).items():
                aggregate_stages[_stage_name] = aggregate_stages.get(_stage_name, 0.0) + float(_stage_seconds)
            frames.append(fr_chunk)
            result_parts.append(result_chunk)
            _pending = st.session_state.get("_pending_validation_learning", {})
            for _flag, _item in (_pending.get("flags", {}) if isinstance(_pending, dict) else {}).items():
                _existing = pending_flags.setdefault(_flag, {"sids": [], "evaluated_sids": [], "reason": _item.get("reason", "")})
                _existing["sids"] = list(dict.fromkeys(_existing["sids"] + list(_item.get("sids", []))))
                _existing["evaluated_sids"] = list(dict.fromkeys(_existing["evaluated_sids"] + list(_item.get("evaluated_sids", []))))
            if manifest is not None:
                save_chunk_results(manifest, batch_id, fr_chunk, result_chunk)
                mark_batch(manifest, batch_id, status="complete", rows=len(chunk))
        except Exception:
            if manifest is not None:
                mark_batch(manifest, batch_id, status="failed", rows=len(chunk))
            raise
    combined = {}
    for part in result_parts:
        for flag, frame in part.items():
            combined[flag] = frame if flag not in combined else pd.concat([combined[flag], frame], ignore_index=True)
    st.session_state["_pending_validation_learning"] = {"data": pending_data, "hash_by_url": pending_hashes, "flags": pending_flags}
    st.session_state["validation_timings"] = {
        _name: {"seconds": round(_info["seconds"], 3), "runs": _info["runs"]}
        for _name, _info in aggregate_timings.items()
    }
    st.session_state["validation_stage_timings"] = {
        _name: round(_seconds, 3) for _name, _seconds in aggregate_stages.items()
    }
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), combined


try:
    register_learning_commit(commit_pending_validation_learning)
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
    st.session_state.grid_items_per_page = 200
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
        /* Prevent flag selector iframe container collapse and flicker during processing/reruns */
        div.st-key-country_flag_bar_container div[data-testid="stElementContainer"]:has(iframe) {{
            min-height: 85px !important;
            contain: layout paint;
        }}
        div.st-key-country_flag_bar_container,
        div.st-key-country_flag_bar_container div[data-testid="stElementContainer"] {{
            min-height: 85px !important;
            height: 85px !important;
            overflow: hidden !important;
        }}
        div.st-key-country_flag_bar_container iframe {{
            min-height: 85px !important;
            height: 85px !important;
        }}
        [data-stale="true"]:has(iframe),
        [data-stale="true"] iframe {{
            opacity: 1 !important;
            visibility: visible !important;
            transition: none !important;
        }}
        /* Keep the visual review dialog visible during Streamlit reruns
           (e.g. after Batch Reject). Streamlit marks its elements
           data-stale="true" while Python re-executes, which can flash
           the dialog blank. Pinning opacity + visibility here keeps the
           grid in view throughout the processing window. */
        /* Smooth fade during in-modal processing, without pinning closing dialogs */
        [data-testid="stDialog"]:has(iframe)[data-stale="true"] {{
            opacity: 0.95;
            transition: opacity 0.15s ease;
        }}
        [data-testid="stDialogContent"] {{
            min-height: 300px;
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
    # ── Blocked Image Fingerprints status ─────────────────────────────────
    with st.expander("Blocked image fingerprints", icon=":material/fingerprint:"):
        try:
            # The catalog shown to reviewers is the JSON catalog. Excel is
            # only a migration/legacy input and is not used for these counts.
            _json_rules = [
                r for r in load_learned_image_rules()
                if str(r.get("status", "active")).casefold() == "active"
            ]
            _json_fingerprints = {
                (str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip())
                for r in _json_rules
                if str(r.get("image_url", "")).strip() or str(r.get("phash", "")).strip()
            }
            _bfp_loaded = len(_json_fingerprints)
            import json as _bfp_json
            _bfp_cache: dict = {}
            if os.path.exists(_BLOCKED_IMG_CACHE):
                try:
                    with open(_BLOCKED_IMG_CACHE, "r", encoding="utf-8") as _bfp_f:
                        _bfp_cache = _bfp_json.load(_bfp_f)
                    _bfp_cache.pop("_excel_mtime", None)
                except Exception:
                    pass
            _bfp_failed = []
            _bfp_skipped = []
            _bfp_total_rows = len(_json_rules)
            _bc1, _bc2 = st.columns(2)
            _bc1.metric("Loaded ✅", _bfp_loaded,
                help="Unique image fingerprints successfully hashed and ready for matching")
            _bc2.metric("Failed ⚠️", len(_bfp_failed),
                help="URLs that could not be downloaded — fingerprint matching won't work for them")
            if _bfp_total_rows == 0:
                st.info("No active JSON image rules yet. Generate a report or reject an image in the iframe to teach one.", icon="ℹ️")
            else:
                st.caption(
                    f"{_bfp_total_rows} JSON rules · "
                    f"{_bfp_loaded} unique fingerprints · "
                    f"{len(_bfp_failed)} fetch-failed · "
                    f"{len(_bfp_skipped)} skipped"
                )
            if _bfp_failed:
                st.warning("These URLs could not be downloaded:", icon="⚠️")
                for _u in _bfp_failed[:8]:
                    st.code(_u, language=None)
                if len(_bfp_failed) > 8:
                    st.caption(f"… and {len(_bfp_failed) - 8} more")
                if st.button("Retry failed URLs", key="retry_blocked_fp", icon=":material/refresh:",
                        help="Clears the failure cache so they are re-attempted next validation run"):
                    try:
                        if os.path.exists(_BLOCKED_IMG_CACHE):
                            os.remove(_BLOCKED_IMG_CACHE)
                        st.cache_resource.clear()
                        st.toast("Cache cleared — failed URLs will be retried on next run.", icon=":material/refresh:")
                        st.rerun()
                    except Exception as _re:
                        st.error(f"Could not clear cache: {_re}")
        except Exception as _bfp_e:
            st.error(f"Could not read fingerprint status: {_bfp_e}")
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
        st.header("AI Learning", anchor=False)
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
@st.fragment
def _render_flag_selector_bar():
    with st.container(key="country_flag_bar_container"):
        st.iframe(_flag_selector_html, height=85)

_render_flag_selector_bar()

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
        # Model-name perfume claims for the grid badge. Per batch, and written
        # by the check rather than the uploader, so it survives a new upload
        # unless it is cleared here.
        "_perfume_model_claims",
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

uploaded_files = st.file_uploader("Dpload files", type=["csv", "xlsx", "zip"], accept_multiple_files=True, key=f"daily_files_{st.session_state.uploader_key}", label_visibility="collapsed")

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

# The visual review was just closed, so this run is rebuilding the whole page.
# Cover it before that starts — emitted here because it is the earliest point
# after the page chrome exists, and the overlay must paint before the flag
# expanders and exports begin redrawing.
if st.session_state.get("_grid_closing"):
    render_grid_closing_overlay()

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
            st.session_state["_manual_journal_mtime"] = manual_decisions_mtime(process_signature)
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

                    _file_read_t0 = time.perf_counter()
                    all_dfs: list = []
                    file_sids_sets: list = []
                    has_zip_source = False
                    st.session_state.zip_image_store = {}
                    _accumulated_zip_qc_dfs = []
                    _accumulated_zip_img_index = {}
                    _accumulated_zip_source_bytes = []
                    _accumulated_pim_verdicts = []
                    _accumulated_rejection_reasons = []
                    st.session_state.zip_qc_results = pd.DataFrame()
                    st.session_state.pop("_zip_sid_index", None)
                    st.session_state.pop("_zip_status_cols", None)
                    st.session_state.pop("_zip_prefetch_map", None)
                    _sid_col_qc: str | None = None

                    # Independent plain uploads can be parsed concurrently.
                    # ZIP/QC files stay on the main thread because their
                    # indexes and PIM verdicts are merged into shared session
                    # state below. Parsing is bounded so several large Excel
                    # files do not exhaust memory or disk handles.
                    _plain_uploads = [
                        (_idx, _uf) for _idx, _uf in enumerate(_files_for_processing)
                        if not _uf["name"].lower().endswith(".zip")
                        and not any(k in _uf["name"].lower() for k in ("qc_results", "qc_result"))
                    ]

                    def _read_plain_upload(item):
                        _idx, _uf = item
                        _buf = BytesIO(_uf["bytes"])
                        _name = _uf["name"].lower()
                        if _name.endswith((".xlsx", ".xls")):
                            _frame = pd.read_excel(_buf, engine="openpyxl" if _name.endswith(".xlsx") else None, dtype=str)
                        else:
                            _frame = _detect_and_read_csv(_buf)
                        return _idx, _frame

                    _plain_frames = {}
                    if len(_plain_uploads) > 1:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(_plain_uploads))) as _read_pool:
                            for _idx, _frame in _read_pool.map(_read_plain_upload, _plain_uploads):
                                _plain_frames[_idx] = _frame
                    elif _plain_uploads:
                        _idx, _frame = _read_plain_upload(_plain_uploads[0])
                        _plain_frames[_idx] = _frame

                    for _uf_index, uf in enumerate(_files_for_processing):
                        _buf = BytesIO(uf["bytes"])
                        raw_data = pd.DataFrame()
                        if uf["name"].lower().endswith(".zip"):
                            has_zip_source = True
                            with zipfile.ZipFile(_buf) as zf:
                                members = zf.infolist()
                                qc_files = [info for info in members if "qc_results" in info.filename.lower() and info.filename.lower().endswith((".xlsx", ".xls", ".csv"))]
                                if qc_files:
                                    for qcf in qc_files:
                                        qc_data = zf.read(qcf)
                                        qdf = (_detect_and_read_csv(BytesIO(qc_data)) if qcf.filename.lower().endswith(".csv") else pd.read_excel(BytesIO(qc_data), dtype=str))
                                        
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
                                        
                                        _accumulated_zip_qc_dfs.append(qdf)

                                # ── PIM_QC_Result.xlsx ────────────────────
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
                                            _accumulated_pim_verdicts.append(_verd)
                                        if "RejectionReasons" in _xl.sheet_names:
                                            _rr = _xl.parse("RejectionReasons", dtype=str).fillna("")
                                            _rr_list = (
                                                _rr.iloc[:, 0].astype(str).str.strip()
                                                .loc[lambda s: s.ne("")].tolist()
                                            )
                                            _accumulated_rejection_reasons.extend(_rr_list)
                                    except Exception as _pim_err:
                                        logger.warning("PIM_QC_Result read failed: %s", _pim_err)

                                _zip_idx = _index_zip_images(zf)
                                if _zip_idx:
                                    _accumulated_zip_img_index.update(_zip_idx)
                                    _accumulated_zip_source_bytes.append(uf["bytes"])
                        elif any(k in uf["name"].lower() for k in ("qc_results", "qc_result")):
                            has_zip_source = True
                            qdf_direct = _detect_and_read_csv(_buf) if uf["name"].lower().endswith(".csv") else pd.read_excel(_buf, engine="openpyxl", dtype=str)
                            _accumulated_zip_qc_dfs.append(qdf_direct)
                        else:
                            raw_data = _plain_frames.get(_uf_index, pd.DataFrame())
                        if not raw_data.empty:
                            raw_data = _repair_mojibake(raw_data)
                            all_dfs.append(raw_data)

                    if _accumulated_zip_qc_dfs:
                        st.session_state.zip_qc_results = pd.concat(_accumulated_zip_qc_dfs, ignore_index=True)
                        _build_zip_sid_index(st.session_state.zip_qc_results)
                        zip_raw_data = _repair_mojibake(st.session_state.zip_qc_results.copy())
                        all_dfs.append(zip_raw_data)

                    if _accumulated_pim_verdicts:
                        st.session_state.zip_pim_verdicts = pd.concat(_accumulated_pim_verdicts, ignore_index=True)
                    if _accumulated_rejection_reasons:
                        # Keep unique ordered reasons
                        st.session_state.zip_rejection_reasons = list(dict.fromkeys(_accumulated_rejection_reasons))

                    st.session_state.zip_image_index = _accumulated_zip_img_index
                    st.session_state.zip_image_source_bytes = _accumulated_zip_source_bytes if _accumulated_zip_source_bytes else None

                    st.session_state.no_computation_zip = has_zip_source
                    if not all_dfs: raise ValueError("No data could be read from the uploaded file(s).")
                    st.session_state.setdefault("validation_stage_timings", {})["File reading"] = round(time.perf_counter() - _file_read_t0, 3)

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
                        _rules_revision = os.path.getmtime("learned_image_rules.json") if os.path.exists("learned_image_rules.json") else 0.0
                        _validation_manifest = load_manifest(sig_hash)

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
                            fr_non_zip, res_non_zip = validate_in_chunks(data_non_zip, support_files, country_validator, data_has_warranty, cache_prefix=sig_hash + "|nonzip", skip_validators=fast_skip_list, on_progress=_on_flag_done, full_batch=data, rules_revision=_rules_revision, manifest=_validation_manifest, batch_prefix="nonzip")
                            _prog.empty()
                            final_report_parts.append(fr_non_zip)
                            results_parts.append(res_non_zip)

                        if zip_sids:
                            data_zip = data[data["PRODUCT_SET_SID"].isin(zip_sids)].copy()
                            skip_list = sorted(set(_derive_prefetched_skip_list(qc_zip)) | set(fast_skip_list))
                            st.caption(
                                f"ZIP validators skipped from QC: {len(skip_list):,}"
                                + (f" · {', '.join(skip_list)}" if skip_list else " · none detected")
                            )
                            _prog_zip = st.progress(0, text="Preparing ZIP validation...")
                            def _on_flag_done_zip(flag_name: str, i: int, total: int): _prog_zip.progress(int(i / total * 100), text=f"Checking (ZIP): {flag_name}")
                            fr_zip, res_zip = validate_in_chunks(data_zip, support_files, country_validator, data_has_warranty, cache_prefix=sig_hash + "|zip", skip_validators=skip_list, on_progress=_on_flag_done_zip, full_batch=data, rules_revision=_rules_revision, manifest=_validation_manifest, batch_prefix="zip")
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
                            _data_by_sid = {sid: grp for sid, grp in data.groupby(data["PRODUCT_SET_SID"].astype(str).str.strip(), sort=False)}
                            _zip_result_rows: dict = {}
                            _rej_in_zip = 0
                            _learned_count = 0
                            engine = _get_cat_matcher_engine()

                            status_cols = [c for c in qc_zip.columns if "status" in c.lower()]
                            melted = qc_zip[[_sid_col_qc] + status_cols].melt(id_vars=_sid_col_qc, var_name="col", value_name="val")
                            melted["val_lower"] = melted["val"].astype(str).str.lower().str.strip()
                            rejected_entries = melted[melted["val_lower"].isin(["rejected", "block"])]
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
                                    _cat_rej_sids = set(rejected_entries.loc[(rejected_entries["col"] == "Category_Check_Status") & (rejected_entries["val_lower"] == "rejected"), _sid_col_qc])
                                    if _cat_rej_sids:
                                        _rej_rows = qc_zip[qc_zip[_sid_col_qc].isin(_cat_rej_sids)]
                                        _rsn_series = (_rej_rows["Category_Check_Rejection_Reason"]
                                                       if "Category_Check_Rejection_Reason" in qc_zip.columns
                                                       else pd.Series([""] * len(_rej_rows), index=_rej_rows.index))
                                        for _name, _cat, _rsn in zip(_rej_rows["NAME"], _rej_rows["CATEGORY"], _rsn_series):
                                            engine.add_negative_correction(str(_name).strip(), str(_cat).strip(), str(_rsn).strip(), auto_save=False)
                                            _learned_count += 1

                            _APPLE_ACC_RE = re.compile(
                                r"\b(?:case|cases|cover|covers|sleeve|sleeves|pouch|pouches|screen.?protector|housing|skin)\b",
                                re.IGNORECASE,
                            )
                            _zip_valid_colors = load_valid_colors()
                            _MULTICOLOR_VARIANTS_ZIP = {
                                "multicolor", "multicolour", "multicolored", "multicoloured",
                                "multi colour", "multi color", "multi-colour", "multi-color",
                                "multicolors", "multicolours",
                            }
                            _SPLIT_COMPOSITE_RE = re.compile(r"[,/&|]|\s+and\s+|\s+or\s+|\s+with\s+")

                            def _zip_color_recognised(color_val: str, valid_set: set) -> bool:
                                """Return True only if color_val is in colors.txt (or a multicolor variant)."""
                                c = color_val.strip().lower()
                                if c in _MULTICOLOR_VARIANTS_ZIP:
                                    return True
                                if not valid_set:
                                    return False
                                parts = _SPLIT_COMPOSITE_RE.split(c)
                                for part in parts:
                                    part = part.strip()
                                    if not part:
                                        continue
                                    if part in valid_set or part in _MULTICOLOR_VARIANTS_ZIP:
                                        return True
                                    tokens = part.split()
                                    for token in tokens:
                                        if token in valid_set:
                                            return True
                                return False

                            _sid_to_color = {}
                            if "PRODUCT_SET_SID" in data.columns and "COLOR" in data.columns:
                                _c_valid = data[["PRODUCT_SET_SID", "COLOR"]].dropna()
                                for _s, _c in zip(_c_valid["PRODUCT_SET_SID"].astype(str).str.strip(), _c_valid["COLOR"].astype(str).str.strip()):
                                    if _s not in _sid_to_color and _c.lower() not in {"nan", "none", "", "n/a", "-", "null"}:
                                        _sid_to_color[_s] = _c.lower()

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
                                _reason_code = _mapped.get("reason", "1000033 - Keywords in your content/ Product name / description has been blacklisted" if "restricted" in _base_key else "1000007 - Other Reason")
                                _default_cmt = _mapped.get("comment", "Listing contains restricted or blacklisted keywords." if "restricted" in _base_key else "Rejected")
                                _final_cmt = _comment if (_comment and _comment.lower() not in ("rejected", "block", "nan")) else _default_cmt
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

                                if _flag in fmap:
                                    _mapped = fmap.get(_flag, {})
                                    _reason_code = _mapped.get("reason", _reason_code)
                                    _default_cmt = _mapped.get("comment", _default_cmt)
                                    if not _final_cmt or _final_cmt.lower() in ("rejected", "block", "nan"):
                                        _final_cmt = _default_cmt

                                _fidx = _fr_sid_to_idx.get(_sid)
                                if _fidx is not None:
                                    if "brand_image" in _base_key.lower():
                                        _det_b = str(_r.get("Brand_Detected_On_Product", "")).strip().lower()
                                        _r_reason = str(_r.get("Brand_Image_Check_Reason", "")).lower()
                                        _name_val = str(_r.get("NAME", "")).lower()
                                        _cat_val = str(_r.get("CATEGORY", "")).lower()
                                        _is_apple_detected = (
                                            _det_b == "apple"
                                            or "visible on the product is 'apple'" in _r_reason
                                            or "visible on the product is \"apple\"" in _r_reason
                                            or "brand detected on product: apple" in _r_reason
                                        )
                                        _is_case_cover = bool(_APPLE_ACC_RE.search(_cat_val) or _APPLE_ACC_RE.search(_name_val))
                                        if _is_apple_detected and _is_case_cover:
                                            _clear_zip_complaint(
                                                _fidx, _sid,
                                                "Rejection overturned: Apple brand detected on cases/covers accessory image -- product approved.",
                                                "apple_acc",
                                            )
                                            continue

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
                                        missing_col_df = res_zip.get("Missing COLOR") if res_zip else None
                                        mismatch_col_df = res_zip.get("Color Mismatch: Title vs COLOR Column") if res_zip else None

                                        # Never overturn if the zip rejected for color mismatch, or our check flagged a mismatch!
                                        _col_rej_reason = str(_r.get("Color_Rejection_Reason", "")).lower()
                                        _is_mismatch_in_zip = (
                                            "mismatch" in _col_rej_reason
                                            or "title says" in _col_rej_reason
                                            or "please make them match" in _col_rej_reason
                                            or (mismatch_col_df is not None and not mismatch_col_df.empty and _sid in mismatch_col_df["PRODUCT_SET_SID"].astype(str).values)
                                        )
                                        if _is_mismatch_in_zip:
                                            pass
                                        else:
                                            # To override a color rejection, the product must pass the main
                                            # validation AND have a COLOR value that is recognised in colors.txt.
                                            passed_main_validation = missing_col_df is None or missing_col_df.empty or _sid not in missing_col_df["PRODUCT_SET_SID"].astype(str).values

                                            _explicit_color_value = _sid_to_color.get(_sid, "")
                                            _has_explicit_color = bool(_explicit_color_value)

                                            _color_is_valid = (
                                                _has_explicit_color
                                                and _zip_color_recognised(_explicit_color_value, _zip_valid_colors)
                                            )

                                            if passed_main_validation and _color_is_valid:
                                                _clear_zip_complaint(
                                                    _fidx, _sid, "Rejection overturned: Color accepted -- product approved.", "color",
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
                                    # Mark as overturn-to-rejection ONLY if product was in the ZIP file and AI previously approved it
                                    if has_zip_source and zip_sids:
                                        _ov_mask = update_mask & fr_sids.isin(zip_sids)
                                        final_report.loc[_ov_mask, "overturn_direction"] = "to_rejection"

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
                        st.session_state["_manual_journal_mtime"] = manual_decisions_mtime(process_signature)
                        if _restored:
                            st.write(f"Restored {_restored} manual decision(s) from a previous session.")

                        # The ZIP/report merge above starts from a compact
                        # status frame, so provenance columns produced by the
                        # learned-image matcher can otherwise disappear before
                        # the validation expanders build their labels.
                        _learned_source_report = final_report_subset
                        if isinstance(_learned_source_report, pd.DataFrame) and not _learned_source_report.empty:
                            _learned_sid = _learned_source_report["ProductSetSid"].astype(str).str.strip()
                            _learned_maps = {
                                _col: dict(zip(_learned_sid, _learned_source_report[_col]))
                                for _col in ("Learned Match", "Learned Match Method", "Learned Match Distance")
                                if _col in _learned_source_report.columns
                            }
                            _final_sid = final_report["ProductSetSid"].astype(str).str.strip()
                            final_report["Learned Match"] = _final_sid.map(_learned_maps.get("Learned Match", {})).fillna(False).map(
                                lambda value: value is True or str(value).strip().casefold() in {"true", "1", "yes", "y"}
                            )
                            final_report["Learned Match Method"] = _final_sid.map(_learned_maps.get("Learned Match Method", {})).fillna("")
                            final_report["Learned Match Distance"] = _final_sid.map(_learned_maps.get("Learned Match Distance", {}))
                        else:
                            final_report["Learned Match"] = False
                            final_report["Learned Match Method"] = ""
                            final_report["Learned Match Distance"] = pd.Series(index=final_report.index, dtype="float64")

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


# ── Cross-tab manual decision refresh ──────────────────────────────────────
# Each browser tab has its own Streamlit session, but manual decisions are
# journalled by the upload signature on disk.  The normal processing gate is
# intentionally skipped when the same file is uploaded again, so without this
# small mtime check a second tab would keep showing stale statuses until the
# file was changed.  Re-apply only when the journal changed; validation,
# image work, and report rebuilding are not repeated.
if (
    process_signature != "empty"
    and st.session_state.get("last_processed_files") == process_signature
    and isinstance(st.session_state.get("final_report"), pd.DataFrame)
    and not st.session_state.final_report.empty
):
    _journal_mtime = manual_decisions_mtime(process_signature)
    _seen_journal_mtime = int(st.session_state.get("_manual_journal_mtime", 0) or 0)
    if _journal_mtime != _seen_journal_mtime:
        _external_decisions = load_manual_decisions(process_signature)
        _external_count = apply_manual_decisions(
            st.session_state.final_report, _external_decisions
        )
        st.session_state["_manual_journal_mtime"] = _journal_mtime
        if _external_count:
            st.toast(
                f"Updated {_external_count:,} manual decision(s) from another tab",
                icon=":material/sync:",
            )


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


@st.fragment(run_every="0.5s")
def _drain_review_batch_queue():
    """Commit a visual-review batch in short slices.

    The iframe can keep handling page navigation between slices.  The worker
    deliberately runs on Streamlit's script thread (rather than mutating
    session state from a Python thread), but each slice is bounded so a large
    batch never monopolises the bridge request.
    """
    _queue = st.session_state.get("_review_batch_queue")
    if not isinstance(_queue, list) or not _queue:
        return

    st.caption("Saving the previous batch in the background. Page navigation remains available.")

    _job = _queue[0]
    _groups = _job.get("groups", []) if isinstance(_job, dict) else []
    _cursor = int(_job.get("cursor", 0) or 0) if isinstance(_job, dict) else 0
    _offset = int(_job.get("offset", 0) or 0) if isinstance(_job, dict) else 0
    _slice_size = 180
    _processed = 0

    while _cursor < len(_groups) and _processed < _slice_size:
        _group = _groups[_cursor]
        _sids = list(_group.get("sids", [])) if isinstance(_group, dict) else []
        _flag = str(_group.get("flag", "Other Reason (Custom)"))
        _code = str(_group.get("code", "1000007 - Other Reason"))
        _comment = str(_group.get("comment", ""))
        _remaining = _slice_size - _processed
        _part = _sids[_offset:_offset + _remaining]
        if _part:
            # The queue is already grouped by comment.  One dataframe update
            # per group is substantially cheaper than one update per SKU.
            apply_status_change(
                _part,
                status="Rejected",
                reason=_code,
                comment=_comment,
                flag=_flag,
                is_manual=True,
                is_zip=False,
                snapshot=False,
                checkpoint=False,
                clear_caches=False,
                # Run sibling propagation once at the start of each reason
                # group; later slices only perform the cheap dataframe write.
                propagate_siblings=(_offset == 0),
            )
            _processed += len(_part)
            _offset += len(_part)
        if _offset >= len(_sids):
            _cursor += 1
            _offset = 0
        elif not _part:
            # Defensive guard for malformed queue entries.
            _cursor += 1
            _offset = 0

    if isinstance(_job, dict):
        _job["cursor"] = _cursor
        _job["offset"] = _offset

    if _cursor >= len(_groups):
        # Checkpoint once for the complete batch, rather than once for every
        # selected group.  This is the expensive disk/report operation.
        checkpoint_final_report(st.session_state.final_report)
        _clear_result_caches(clear_streamlit_cache=False)
        _fr = st.session_state.get("final_report")
        if isinstance(_fr, pd.DataFrame):
            _fr.attrs.pop("__pim_hash__", None)
            _fr.attrs.pop("__pim_hash_stamp__", None)
        _queue.pop(0)
        st.session_state["_grid_pending_report_sync"] = True
        st.session_state.setdefault("main_toasts", []).append(
            f"Rejected {int(_job.get('total', _processed)):,} product(s)"
        )
        st.session_state["_review_batch_active"] = bool(_queue)
    else:
        st.session_state["_review_batch_active"] = True


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
                _raw_payload = _msg.get("payload", {})
                if isinstance(_raw_payload, dict) and "sids" in _raw_payload:
                    _payload = _raw_payload.get("sids", {})
                    _auto_comments = _raw_payload.get("comments", {})
                else:
                    _payload = _raw_payload
                    _auto_comments = st.session_state.pop("pending_auto_comments", {})
                if isinstance(_payload, dict) and _payload:
                    _rgroups = {}
                    for _sid, _rkey in _payload.items(): _rgroups.setdefault(_rkey, []).append(_sid)
                    _queue_groups = []
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
                        # Keep comments exact while grouping all SIDs that can
                        # share one dataframe update.
                        _sids_by_comment: dict = {}
                        for _sid in _sids:
                            _sid_cmt = _auto_comments.get(_sid, _cmt)
                            _sids_by_comment.setdefault(_sid_cmt, []).append(_sid)
                        for _cmt_val, _sid_group in _sids_by_comment.items():
                            _queue_groups.append({
                                "sids": list(_sid_group),
                                "flag": _flag,
                                "code": _code,
                                "comment": _cmt_val,
                            })
                    _total = sum(len(g["sids"]) for g in _queue_groups)
                    # Reflect the decision in the server-side review state
                    # immediately.  If the reviewer turns the page before the
                    # next queue tick, the new page still receives the same
                    # committed indicator instead of briefly showing the old
                    # status.
                    _quick = st.session_state.setdefault("quick_rejects", {})
                    for _group in _queue_groups:
                        for _sid in _group["sids"]:
                            _quick[str(_sid).strip()] = _group["flag"]
                    _queue = st.session_state.setdefault("_review_batch_queue", [])
                    _queue.append({"groups": _queue_groups, "cursor": 0, "total": _total})
                    st.session_state["_review_batch_active"] = True
                    # The optimistic iframe update has already happened.  Do
                    # not synchronously write 500+ rows or rebuild the grid;
                    # the short polling fragment drains the queue in slices.
                    st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                    st.session_state.do_scroll_top = False
                    # Intentionally no st.rerun(): it would destroy/recreate
                    # the iframe and block page navigation during batching.
            elif _msg.get("action") == "undo":
                _payload = _msg.get("payload", {})
                _total_restored = 0
                if isinstance(_payload, dict):
                    for _sid in _payload.keys():
                        restore_single_item(_sid)
                        _total_restored += 1
                if _total_restored > 0:
                    _fr_jt = st.session_state.get("final_report")
                    if isinstance(_fr_jt, pd.DataFrame):
                        _fr_jt.attrs.pop("__pim_hash__", None)
                        _fr_jt.attrs.pop("__pim_hash_stamp__", None)
                    # The validation caches are independent of a reviewer
                    # undo. Clearing Streamlit's entire cache here forced the
                    # next interaction to rebuild file reads, category data,
                    # and validator results for the whole batch.
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
            elif _msg.get("action") == "grid_load_more":
                _fr_more = st.session_state.get("final_report", pd.DataFrame())
                _ipp_more = min(500, int(st.session_state.get("grid_items_per_page", 200) or 200))
                _total_more = len(_fr_more) if isinstance(_fr_more, pd.DataFrame) else 0
                _max_page_more = max(0, (_total_more - 1) // max(1, _ipp_more))
                _current_page_more = int(st.session_state.get("grid_page", 0) or 0)
                if _current_page_more < _max_page_more:
                    st.session_state.grid_page = _current_page_more + 1
                    st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                    st.rerun()
            elif _msg.get("action") == "remove_learned_rule":
                _sid = str(_msg.get("payload", "")).strip()
                _dm = st.session_state.get("all_data_map", pd.DataFrame())
                _removed_rules = []
                if isinstance(_dm, pd.DataFrame) and not _dm.empty and "PRODUCT_SET_SID" in _dm.columns:
                    _row = _dm[_dm["PRODUCT_SET_SID"].astype(str).str.strip().eq(_sid)]
                    if not _row.empty:
                        _img = str(_row.iloc[0].get("MAIN_IMAGE", "")).strip()
                        _removed_rules = [r for r in load_learned_image_rules() if str(r.get("image_url", "")).strip() == _img]
                if _removed_rules:
                    _deleted = delete_learned_image_rules(_removed_rules)
                    _undo_history = st.session_state.setdefault("_learned_rule_undo_history", [])
                    _undo_history.append(_removed_rules)
                    del _undo_history[:-10]
                    # Keep the old single-batch key for compatibility with
                    # the Dashboard while exposing the full ten-step stack.
                    st.session_state["_learned_rule_undo"] = _removed_rules
                    st.session_state.main_toasts.append(f"Removed {_deleted:,} learned rule(s). Undo is available on the Learned Rules page.")
                    # Learned-rule deletion is reflected through the rule
                    # revision/cache key; do not flush every unrelated cache.
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
            elif _msg.get("action") == "close_review":
                st.session_state.show_review_modal = False
                st.session_state.pop("_grid_pending_report_sync", None)
                st.session_state["main_bridge_counter"] = st.session_state.get("main_bridge_counter", 0) + 1
                st.session_state["_grid_closing"] = True
                _fr_jt = st.session_state.get("final_report")
                if isinstance(_fr_jt, pd.DataFrame):
                    _fr_jt.attrs.pop("__pim_hash__", None)
                    _fr_jt.attrs.pop("__pim_hash_stamp__", None)
                # Closing only changes the review visibility. Keep validation
                # caches warm; clearing the global cache here made closing a
                # large batch trigger unnecessary recomputation on the next
                # page render.
                st.rerun()
        except Exception as _e:
            logger.error(f"Bridge parse error: {_e}")


@st.cache_data(show_spinner=False, hash_funcs={pd.DataFrame: df_hash})
def get_enriched_results(fr_df, data_df):
    if fr_df.empty: 
        return pd.DataFrame()
    # Explicitly pull ONLY required columns to prevent cloning wide datasets
    needed_cols = [c for c in ["PRODUCT_SET_SID", "SELLER_NAME", "BRAND"] if c in data_df.columns]
    return pd.merge(
        fr_df, 
        data_df[needed_cols], 
        left_on="ProductSetSid", 
        right_on="PRODUCT_SET_SID", 
        how="left"
    )


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
    _timing_rows = []
    for _stage_name, _stage_seconds in sorted(
        st.session_state.get("validation_stage_timings", {}).items(),
        key=lambda item: float(item[1]), reverse=True,
    ):
        _timing_rows.append({"Validator": f"Stage · {_stage_name}", "Seconds": float(_stage_seconds), "Runs": 1})
    for _name, _info in sorted(
        st.session_state.get("validation_timings", {}).items(),
        key=lambda item: float(item[1].get("seconds", 0)), reverse=True,
    ):
        _timing_rows.append({
            "Validator": _name,
            "Seconds": float(_info.get("seconds", 0)),
            "Runs": int(_info.get("runs", 0)),
        })
    if _timing_rows:
        with st.expander("Validation diagnostics", expanded=False):
            st.caption("Measured validator time for the current validation run.")
            st.dataframe(pd.DataFrame(_timing_rows), hide_index=True, width="stretch")
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
                    df_flagged = rej_df[rej_df["FLAG"] == title].copy()
                    is_zip = "(Prefetched)" in title

                    # =========================================================
                    # MEMORY OPTIMIZATION FIX
                    # =========================================================
                    # 1. Downcast object types & convert repetitive strings to category
                    df_flagged = df_flagged.infer_objects()
                    for col in df_flagged.select_dtypes(include=["object"]).columns:
                        if df_flagged[col].nunique() / max(len(df_flagged), 1) < 0.5:
                            df_flagged[col] = df_flagged[col].astype("category")

                    # 2. Keep only essential columns to stop passing 6,000+ wide columns
                    keep_cols = [
                        c for c in [
                            "ProductSetSid", "PRODUCT_SET_SID", "ParentSKU", "Status", 
                            "FLAG", "Comment", "Comment_Detail", "Reason", "SellerName", 
                            "SELLER_NAME", "BRAND", "NAME", "Is_Zip", "Is_Manual", 
                            "overturn_direction", "zip_override", "Learned Match",
                            "Learned Match Method", "Learned Match Distance",
                            "Learned Rule URL", "Learned Rule pHash"
                        ] if c in df_flagged.columns
                    ]
                    if keep_cols:
                        df_flagged = df_flagged[keep_cols]

                    # 3. Force garbage collection before the UI fragment renders
                    gc.collect()
                    # =========================================================

                    # Build the expander label showing severity mark, validation title, and product count
                    _n_flagged = len(df_flagged)
                    _mark = SEVERITY.get(_level, {}).get("mark", "•")
                    _prod_label = f"{_n_flagged:,} {'product' if _n_flagged == 1 else 'products'}"
                    exp_label = f"{_mark}  {title} ({_prod_label})"

                    _ov_cnt = 0
                    if "overturn_direction" in df_flagged.columns:
                        _ov_raw = df_flagged["overturn_direction"]
                        if isinstance(_ov_raw, pd.DataFrame):  # duplicate column name guard
                            _ov_raw = _ov_raw.iloc[:, 0]
                        _ov_series = _ov_raw.astype(str).replace({"nan": "", "None": "", "<NA>": ""})
                        _ov_cnt = int((_ov_series == "to_rejection").sum())
                    if _ov_cnt > 0:
                        exp_label += f"  •  🔓 {_ov_cnt:,} Overturned"
                    _learned_cnt = 0
                    if "Learned Match" in df_flagged.columns:
                        _learned_cnt = int(df_flagged["Learned Match"].fillna(False).astype(bool).sum())
                    if _learned_cnt > 0:
                        exp_label += f"  •  🧠 {_learned_cnt:,} Learned"
                    if is_zip:
                        exp_label += "  (Prefetched)  :material/archive: ZIP"

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

    if "final_report" in st.session_state and not st.session_state.final_report.empty:
        _fr_all = st.session_state.final_report

        # Overturned-to-Rejection: AI approved but system re-rejected
        _ov_rej_mask = (
            _fr_all.get("overturn_direction", pd.Series("", index=_fr_all.index))
            .fillna("").astype(str).eq("to_rejection")
        )
        _df_ov_rej = _fr_all[_ov_rej_mask].copy()

        # Overturned-to-Approval: AI rejected but policy approved
        _ov_app_mask = (
            _fr_all.get("overturn_direction", pd.Series("", index=_fr_all.index))
            .fillna("").astype(str).eq("to_approval")
            | (
                _fr_all.get("zip_override", pd.Series("", index=_fr_all.index)).fillna("").astype(str).ne("")
                & _fr_all.get("overturn_direction", pd.Series("", index=_fr_all.index)).fillna("").astype(str).ne("to_rejection")
            )
        )
        _df_ov_app = _fr_all[_ov_app_mask].copy()

        # Show overturned-to-rejection in summary expander
        if not _df_ov_rej.empty:
            with st.expander(f"\U0001f6a8 Overturned to Rejection ({len(_df_ov_rej):,} products — AI False Approvals)", expanded=False):
                st.markdown(
                    "The following products were **approved by AI** but **overturned to rejection** "
                    "by QC validation rules. They also appear in their respective validation sections above."
                )
                _ov_rej_disp = _df_ov_rej[[
                    c for c in ["ProductSetSid", "ParentSKU", "SellerName", "FLAG", "Comment"]
                    if c in _df_ov_rej.columns
                ]].copy()
                _ov_rej_disp.rename(columns={
                    "ProductSetSid": "Product Set SID",
                    "ParentSKU": "Parent SKU",
                    "SellerName": "Seller",
                    "FLAG": "Overturn Reason",
                    "Comment": "Details",
                }, inplace=True)
                try:
                    if "PRODUCT_SET_SID" in data.columns:
                        _lookup_dedup = data.drop_duplicates(subset=["PRODUCT_SET_SID"])
                        _extra_cols = [c for c in ["NAME", "COLOR", "BRAND"] if c in _lookup_dedup.columns]
                        if _extra_cols:
                            _merge_src = _lookup_dedup[["PRODUCT_SET_SID"] + _extra_cols].rename(
                                columns={"PRODUCT_SET_SID": "Product Set SID"}
                            )
                            _ov_rej_disp = _ov_rej_disp.merge(_merge_src, on="Product Set SID", how="left")
                            _preferred_order = ["Product Set SID", "Parent SKU", "NAME", "BRAND", "COLOR", "Overturn Reason", "Details", "Seller"]
                            _ov_rej_disp = _ov_rej_disp[[c for c in _preferred_order if c in _ov_rej_disp.columns]]
                except Exception:
                    pass
                st.dataframe(_ov_rej_disp, width='stretch', hide_index=True)

        # Note about overturned-to-approval (shown in main iframe)
        if not _df_ov_app.empty:
            st.info(
                f"\U0001f513 **{len(_df_ov_app):,} product(s) overturned to Approved** "
                "(AI rejected \u2192 policy approved) are shown in the main results table above "
                "with their overturn reason in the comment."
            )

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

handle_jtbridge()

# Keep batch persistence in its own short-lived fragment.  This fragment is
# independent from the review iframe, so Next/Previous can be used while the
# prior page is still being committed.
_drain_review_batch_queue()

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

# Page is rendered; drop the closing overlay. A later stylesheet wins, so this
# needs no rerun — see end_grid_closing_overlay().
if st.session_state.pop("_grid_closing", False):
    end_grid_closing_overlay()
