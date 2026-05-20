"""
loaders.py - All file loading functions for support/config data
"""

import json
import logging
import os
import re
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from data_utils import clean_category_code

logger = logging.getLogger(__name__)

COUNTRY_TABS = ["KE", "UG", "NG", "GH", "MA"]
COUNTRY_NAME_TO_TAB = {
    "Kenya": "KE",
    "Uganda": "UG",
    "Nigeria": "NG",
    "Ghana": "GH",
    "Morocco": "MA",
}


# -------------------------------------------------
# LOW-LEVEL FILE READERS
# -------------------------------------------------


def load_txt_file(filename: str) -> List[str]:
    try:
        if not os.path.exists(os.path.abspath(filename)):
            return []
        with open(filename, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.warning(f"load_txt_file({filename}): {e}")
        return []


@st.cache_data(ttl=3600)
def load_excel_file(filename: str, column: Optional[str] = None):
    try:
        if not os.path.exists(filename):
            return [] if column else pd.DataFrame()
        df = pd.read_excel(filename, engine="openpyxl", dtype=str)
        df.columns = df.columns.str.strip()
        if column and column in df.columns:
            return df[column].apply(clean_category_code).tolist()
        return df
    except Exception as e:
        logger.warning(f"load_excel_file({filename}, col={column}): {e}")
        return [] if column else pd.DataFrame()


def safe_excel_read(filename: str, sheet_name, usecols=None) -> pd.DataFrame:
    if not os.path.exists(filename):
        return pd.DataFrame()
    try:
        df = pd.read_excel(
            filename,
            sheet_name=sheet_name,
            usecols=usecols,
            engine="openpyxl",
            dtype=str,
        )
        return df.dropna(how="all")
    except Exception as e:
        logger.error(f"safe_excel_read: tab='{sheet_name}' file={filename}: {e}")
        return pd.DataFrame()


# -------------------------------------------------
# SUPPORT DATA LOADERS
# -------------------------------------------------


@st.cache_data(ttl=3600)
def load_prohibited_from_local() -> Dict[str, List[Dict]]:
    FILE_NAME = "Prohibbited.xlsx"
    prohibited_by_country = {}
    for tab in COUNTRY_TABS:
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab)
            if df.empty:
                prohibited_by_country[tab] = []
                continue
            df.columns = [str(c).strip().lower() for c in df.columns]
            keyword_col = next(
                (
                    c
                    for c in df.columns
                    if "keyword" in c or "prohibited" in c or "name" in c
                ),
                df.columns[0],
            )
            category_col = next((c for c in df.columns if "cat" in c), None)
            kw_series = df[keyword_col].astype(str).str.strip().str.lower()
            valid_mask = kw_series.ne("") & ~kw_series.isin(("nan", "keywords"))
            df_v = df[valid_mask].copy()
            kw_series = kw_series[valid_mask]
            if category_col:
                def _parse_cats(raw):
                    raw = str(raw).strip()
                    if not raw or raw.lower() == "nan":
                        return set()
                    return {clean_category_code(c.strip()) for c in re.split(r"[,\n]+", raw) if c.strip()}
                cats_series = df_v[category_col].map(_parse_cats)
            else:
                cats_series = pd.Series([set()] * len(df_v), index=df_v.index)
            prohibited_by_country[tab] = [
                {"keyword": kw, "categories": cats}
                for kw, cats in zip(kw_series, cats_series)
            ]
        except Exception as e:
            logger.warning(f"load_prohibited_from_local tab={tab}: {e}")
            prohibited_by_country[tab] = []
    return prohibited_by_country


@st.cache_data(ttl=3600)
def load_restricted_brands_from_local() -> Dict[str, List[Dict]]:
    FILE_NAME = "Restricted_Brands.xlsx"
    config_by_country = {}
    for country_name, tab_name in COUNTRY_NAME_TO_TAB.items():
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab_name)
            if df.empty:
                config_by_country[country_name] = []
                continue
            df.columns = [str(c).strip().lower() for c in df.columns]
            brand_col_vals = df.get("brand", pd.Series(dtype=str)).astype(str).str.strip()
            valid = brand_col_vals.str.lower().ne("nan") & brand_col_vals.ne("")
            df = df[valid].copy()
            df["_b_lower"] = brand_col_vals[valid].str.lower().values

            def _split_set(series, sep=","):
                return series.astype(str).str.strip().apply(
                    lambda x: set() if not x or x.lower() == "nan"
                    else {v.strip() for v in x.split(sep) if v.strip()}
                )

            def _split_cats(series):
                return series.astype(str).str.strip().apply(
                    lambda x: None if (not x or x.lower() == "nan")
                    else {clean_category_code(c.strip()) for c in x.split(",") if c.strip()}
                )

            sellers_s = _split_set(df.get("approved sellers", pd.Series([""] * len(df), index=df.index)), ",")
            cats_s = _split_cats(df.get("categories", pd.Series([""] * len(df), index=df.index)))
            vars_s = _split_set(df.get("variations", pd.Series([""] * len(df), index=df.index)), ",")

            brand_dict: dict = {}
            for b_lower, brand_raw, sellers, cats, variations in zip(
                df["_b_lower"], brand_col_vals[valid], sellers_s, cats_s, vars_s
            ):
                if b_lower not in brand_dict:
                    brand_dict[b_lower] = {
                        "brand_raw": brand_raw,
                        "sellers": set(),
                        "categories": set(),
                        "variations": set(),
                        "has_blank_category": False,
                    }
                brand_dict[b_lower]["sellers"].update(sellers)
                if cats is None:
                    brand_dict[b_lower]["has_blank_category"] = True
                else:
                    brand_dict[b_lower]["categories"].update(cats)
                brand_dict[b_lower]["variations"].update(variations)

            country_rules = [
                {
                    "brand": b_lower,
                    "brand_raw": data["brand_raw"],
                    "sellers": data["sellers"],
                    "categories": set() if data["has_blank_category"] else data["categories"],
                    "variations": list(data["variations"]),
                }
                for b_lower, data in brand_dict.items()
            ]
            config_by_country[country_name] = country_rules
        except Exception as e:
            logger.warning(f"load_restricted_brands tab={tab_name}: {e}")
            config_by_country[country_name] = []
    return config_by_country


@st.cache_data(ttl=3600)
def load_refurb_data_from_local() -> dict:
    FILE_NAME = "Refurb.xlsx"
    result = {
        "sellers": {},
        "categories": {"Phones": set(), "Laptops": set()},
        "keywords": set(),
    }
    for tab in COUNTRY_TABS:
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab, usecols=[0, 1])
            if not df.empty:
                df.columns = [str(c).strip() for c in df.columns]
                phones_set = set(
                    df.iloc[:, 0].dropna().astype(str).str.strip().str.lower()
                ) - {"", "nan", "phones", "phone"}
                laptops_set = set(
                    df.iloc[:, 1].dropna().astype(str).str.strip().str.lower()
                ) - {"", "nan", "laptops", "laptop"}
                result["sellers"][tab] = {"Phones": phones_set, "Laptops": laptops_set}
        except Exception as e:
            logger.warning(f"load_refurb_data tab={tab}: {e}")
            result["sellers"][tab] = {"Phones": set(), "Laptops": set()}
    try:
        df_cats = safe_excel_read(FILE_NAME, sheet_name="Categories", usecols=[0, 1])
        if df_cats.empty:
            df_cats = safe_excel_read(FILE_NAME, sheet_name="Categries", usecols=[0, 1])
        if not df_cats.empty:
            df_cats.columns = [str(c).strip() for c in df_cats.columns]
            result["categories"]["Phones"] = {
                clean_category_code(c)
                for c in df_cats.iloc[:, 0].dropna().astype(str)
                if c.strip() and c.strip().lower() not in ("phones", "phone", "nan")
            }
            result["categories"]["Laptops"] = {
                clean_category_code(c)
                for c in df_cats.iloc[:, 1].dropna().astype(str)
                if c.strip() and c.strip().lower() not in ("laptops", "laptop", "nan")
            }
    except Exception as e:
        logger.warning(f"load_refurb_data categories: {e}")
    try:
        df_names = safe_excel_read(FILE_NAME, sheet_name="Name", usecols=[0])
        if not df_names.empty:
            first_col = df_names.columns[0]
            result["keywords"] = {
                k
                for k in df_names[first_col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.lower()
                if k and k not in ("name", "keyword", "keywords", "words", "nan")
            }
    except Exception as e:
        logger.warning(f"load_refurb_data keywords: {e}")
        result["keywords"] = {"refurb", "refurbished", "renewed"}
    return result


@st.cache_data(ttl=3600)
def load_perfume_data_from_local() -> Dict:
    FILE_NAME = "Perfume.xlsx"
    result = {"sellers": {}, "keywords": set(), "category_codes": set()}
    for tab in COUNTRY_TABS:
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab)
            if not df.empty:
                df.columns = [str(c).strip() for c in df.columns]
                seller_col = next(
                    (c for c in df.columns if "seller" in c.lower()), df.columns[0]
                )
                result["sellers"][tab] = set(
                    df[seller_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .pipe(
                        lambda s: s[
                            ~s.isin(["", "nan", "sellername", "seller name", "seller"])
                        ]
                    )
                )
        except Exception as e:
            logger.warning(f"load_perfume_data tab={tab}: {e}")
            result["sellers"][tab] = set()
    try:
        df_kw = safe_excel_read(FILE_NAME, sheet_name="Keywords")
        if not df_kw.empty:
            df_kw.columns = [str(c).strip() for c in df_kw.columns]
            kw_col = next(
                (
                    c
                    for c in df_kw.columns
                    if "brand" in c.lower() or "keyword" in c.lower()
                ),
                df_kw.columns[0],
            )
            result["keywords"] = set(
                df_kw[kw_col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.lower()
                .pipe(lambda s: s[~s.isin(["", "nan", "brand", "keyword", "keywords"])])
            )
    except Exception as e:
        logger.warning(f"load_perfume_data keywords: {e}")
    try:
        df_cats = safe_excel_read(FILE_NAME, sheet_name="Categories")
        if not df_cats.empty:
            df_cats.columns = [str(c).strip() for c in df_cats.columns]
            cat_col = next(
                (c for c in df_cats.columns if "cat" in c.lower()), df_cats.columns[0]
            )
            result["category_codes"] = set(
                df_cats[cat_col]
                .dropna()
                .astype(str)
                .apply(clean_category_code)
                .pipe(lambda s: s[~s.isin(["", "nan", "categories", "category"])])
            )
    except Exception as e:
        logger.warning(f"load_perfume_data categories: {e}")
    return result


@st.cache_data(ttl=3600)
def load_books_data_from_local() -> Dict:
    FILE_NAME = "Books_sellers.xlsx"
    result = {"sellers": {}, "category_codes": set()}
    for tab in COUNTRY_TABS:
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab)
            if not df.empty:
                df.columns = [str(c).strip() for c in df.columns]
                seller_col = next(
                    (c for c in df.columns if "seller" in c.lower()), df.columns[0]
                )
                result["sellers"][tab] = set(
                    df[seller_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .pipe(
                        lambda s: s[
                            ~s.isin(["", "nan", "sellername", "seller name", "seller"])
                        ]
                    )
                )
        except Exception as e:
            logger.warning(f"load_books_data tab={tab}: {e}")
            result["sellers"][tab] = set()
    try:
        df_cats = safe_excel_read(FILE_NAME, sheet_name="Categories")
        if not df_cats.empty:
            df_cats.columns = [str(c).strip() for c in df_cats.columns]
            cat_col = next(
                (c for c in df_cats.columns if "cat" in c.lower()), df_cats.columns[0]
            )
            result["category_codes"] = set(
                df_cats[cat_col]
                .dropna()
                .astype(str)
                .apply(clean_category_code)
                .pipe(lambda s: s[~s.isin(["", "nan", "categories", "category"])])
            )
    except Exception as e:
        logger.warning(f"load_books_data categories: {e}")
    return result


@st.cache_data(ttl=3600)
def load_jerseys_from_local() -> Dict:
    FILE_NAME = "Jersey_validation.xlsx"
    result: Dict = {
        "keywords": {tab: set() for tab in COUNTRY_TABS},
        "exempted": {tab: set() for tab in COUNTRY_TABS},
        "categories": set(),
    }
    for tab in COUNTRY_TABS:
        try:
            df = safe_excel_read(FILE_NAME, sheet_name=tab)
            if not df.empty:
                df.columns = [str(c).strip() for c in df.columns]
                kw_col = next(
                    (c for c in df.columns if "keyword" in c.lower()), df.columns[0]
                )
                result["keywords"][tab] = set(
                    df[kw_col]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .pipe(lambda s: s[~s.isin(["", "nan", "keywords", "keyword"])])
                )
                ex_col = next(
                    (
                        c
                        for c in df.columns
                        if "exempt" in c.lower() or "seller" in c.lower()
                    ),
                    None,
                )
                if ex_col:
                    result["exempted"][tab] = set(
                        df[ex_col]
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .str.lower()
                        .pipe(
                            lambda s: s[
                                ~s.isin(["", "nan", "exempted sellers", "seller"])
                            ]
                        )
                    )
        except Exception as e:
            logger.warning(f"load_jerseys tab={tab}: {e}")
    try:
        df_cats = safe_excel_read(FILE_NAME, sheet_name="categories")
        if not df_cats.empty:
            df_cats.columns = [str(c).strip().lower() for c in df_cats.columns]
            cat_col = next(
                (c for c in df_cats.columns if "cat" in c), df_cats.columns[0]
            )
            result["categories"] = set(
                df_cats[cat_col]
                .dropna()
                .astype(str)
                .apply(clean_category_code)
                .pipe(lambda s: s[~s.isin(["", "nan", "categories", "category"])])
            )
    except Exception as e:
        logger.warning(f"load_jerseys categories: {e}")
    return result


# ---------------------------------------------------------------------------
# perfume_catalog.xlsx loader
# ---------------------------------------------------------------------------
# Cache strategy: keyed on the file's modification-time (_mtime).
# As soon as you save the Excel file the mtime changes, so the NEXT app
# run (or page refresh) automatically fetches fresh data — no manual
# “Clear Cache” needed.  The cache is otherwise permanent (no TTL).
# ---------------------------------------------------------------------------


def _file_mtime(path: str) -> float:
    """Return last-modified timestamp of *path*, or 0.0 if it doesn’t exist."""
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


@st.cache_data  # no TTL — cache busted automatically by mtime argument
def load_perfume_catalog_from_local(_mtime: float = 0.0) -> Dict:
    """
    Reads perfume_catalog.xlsx and returns:

      fake_brands       – set of lowercase brand names sellers use for fakes
                          (from the ‘Brands’ sheet — add rows freely)
      legit_brand_terms – lowercase canonical brand names + aliases
                          (from the ‘brand’ + ‘also_known_as’ columns)
      model_terms       – lowercase individual model names
                          (from the ‘sample_models’ column, ‘;’-separated)
      term_to_brand     – maps any term → canonical brand string (for comments)

    Column matching is CASE-INSENSITIVE so ‘Brand’, ‘BRAND’, ‘brand’ all work.
    Adding new rows to either sheet is picked up automatically on next load.
    """
    FILE_NAME = "perfume_catalog.xlsx"
    result: Dict = {
        "fake_brands": set(),
        "legit_brand_terms": set(),
        "model_terms": set(),
        "term_to_brand": {},
    }

    # ── Sheet 1: enriched catalog (brands + aliases + models) ───────────────
    try:
        df = safe_excel_read(FILE_NAME, sheet_name="enriched_perfume_catalog")
        if not df.empty:
            # ─ case-insensitive column finder ──────────────────────────────
            col_map = {str(c).strip().lower(): str(c).strip() for c in df.columns}
            brand_col = col_map.get("brand") or col_map.get("brands")
            aka_col = (
                col_map.get("also_known_as")
                or col_map.get("alias")
                or col_map.get("aliases")
            )
            model_col = (
                col_map.get("sample_models")
                or col_map.get("models")
                or col_map.get("sample models")
            )

            if not brand_col:
                logger.warning(
                    "load_perfume_catalog: 'brand' column not found in enriched_perfume_catalog"
                )
            else:
                brands = df[brand_col].astype(str).str.strip()
                valid_brands = ~brands.str.lower().isin(("", "nan"))
                df_v = df[valid_brands].copy()
                brands_v = brands[valid_brands]
                brands_low = brands_v.str.lower()
                result["legit_brand_terms"].update(brands_low)
                result["term_to_brand"].update(dict(zip(brands_low, brands_v)))

                if aka_col:
                    _skip = {"", "nan", "-", "none"}
                    for brand, aka_raw in zip(brands_v, df_v[aka_col].astype(str).str.strip()):
                        if aka_raw.lower() in _skip:
                            continue
                        for alias in re.split(r"[/,]", aka_raw):
                            alias = alias.strip().lower()
                            if alias and alias not in _skip:
                                result["legit_brand_terms"].add(alias)
                                result["term_to_brand"].setdefault(alias, brand)

                if model_col:
                    _skip = {"", "nan", "-", "none"}
                    for brand, models_raw in zip(brands_v, df_v[model_col].astype(str).str.strip()):
                        if models_raw.lower() in _skip:
                            continue
                        for model in models_raw.split(";"):
                            model = model.strip().lower()
                            if model:
                                result["model_terms"].add(model)
                                result["term_to_brand"].setdefault(model, brand)
    except Exception as e:
        logger.warning(f"load_perfume_catalog enriched_perfume_catalog: {e}")

    # ── Sheet 2: fake / suspicious seller-brand names ──────────────────────
    try:
        df_brands = safe_excel_read(FILE_NAME, sheet_name="Brands")
        if not df_brands.empty:
            # Accept any column name — just read the first non-empty column
            col = df_brands.columns[0]
            result["fake_brands"] = set(
                df_brands[col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.lower()
                .pipe(
                    lambda s: s[~s.isin(["", "nan", "brand", "brands", "fake brands"])]
                )
            )
    except Exception as e:
        logger.warning(f"load_perfume_catalog Brands: {e}")

    logger.info(
        f"load_perfume_catalog: {len(result['legit_brand_terms'])} brand terms, "
        f"{len(result['model_terms'])} models, "
        f"{len(result['fake_brands'])} fake brands loaded."
    )
    return result


@st.cache_data(ttl=3600)
def load_suspected_fake_from_local() -> Dict:
    if not os.path.exists("suspected_fake.xlsx"):
        logger.warning("suspected_fake.xlsx not found")
        return {code: pd.DataFrame() for code in COUNTRY_TABS}
    result = {}
    for code in COUNTRY_TABS:
        try:
            result[code] = pd.read_excel(
                "suspected_fake.xlsx", sheet_name=code, engine="openpyxl", dtype=str
            )
        except Exception as e:
            logger.warning(f"load_suspected_fake country={code}: {e}")
            result[code] = pd.DataFrame()
    return result


@st.cache_data(ttl=3600)
def load_flags_mapping(filename="reason.xlsx") -> Dict[str, dict]:
    raw_default = {
        "Restricted brands": (
            "1000024 - Product does not have a license to be sold via Jumia (Not Authorized)",
            "Missing license for this item. Raise a claim via Vendor Center.",
        ),
        "Suspected Fake product": (
            "1000023 - Confirmation of counterfeit product by Jumia technical team (Not Authorized)",
            "Product confirmed counterfeit.",
        ),
        "Seller Not approved to sell Refurb": (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim",
            "Contact Seller Support for Refurbished approval.",
        ),
        "Product Warranty": (
            "1000013 - Kindly Provide Product Warranty Details",
            "Valid warranty required in Description/Warranty tabs.",
        ),
        "Seller Approve to sell books": (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim",
            "Contact Seller Support for Book category approval.",
        ),
        "Seller Approved to Sell Perfume": (
            "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim",
            "Contact Seller Support for Perfume approval.",
        ),
        "Counterfeit Sneakers": (
            "1000023 - Confirmation of counterfeit product by Jumia technical team (Not Authorized)",
            "Sneaker confirmed counterfeit.",
        ),
        "Suspected counterfeit Jerseys": (
            "1000023 - Confirmation of counterfeit product by Jumia technical team (Not Authorized)",
            "Jersey confirmed counterfeit.",
        ),
        "Suspected Fake Perfume": (
            "1000023 - Confirmation of counterfeit product by Jumia technical team (Not Authorized)",
            "Perfume confirmed counterfeit — genuine brand/model detected in product name.",
        ),
        "Prohibited products": (
            "1000007 - Other Reason",
            "Listing of this product is prohibited.",
        ),
        "Unnecessary words in NAME": (
            "1000008 - Kindly Improve Product Name Description",
            "Avoid unnecessary words in title.",
        ),
        "Single-word NAME": (
            "1000008 - Kindly Improve Product Name Description",
            "Product name is too short. Kindly update the product title using this format: Name – Type of the Products – Color. If available, please also add key details such as weight, capacity, type, and warranty to make the title clear and complete for customers.",
        ),
        "Generic BRAND Issues": (
            "1000007 - Other Reason",
            "Use correct brand instead of Generic/Fashion. Apply for brand approval if needed.",
        ),
        "Fashion brand issues": (
            "1000007 - Other Reason",
            "Use correct brand instead of Fashion. Apply for brand approval if needed.",
        ),
        "BRAND name repeated in NAME": (
            "1000007 - Other Reason",
            "Brand name should not be repeated in product name.",
        ),
        "Generic branded products with genuine brands": (
            "1000007 - Other Reason",
            "Use the displayed brand on the product instead of Generic.",
        ),
        "Missing COLOR": (
            "1000005 - Kindly confirm the actual product colour",
            "Product color must be mentioned in title/color tab.",
        ),
        "Duplicate product": ("1000007 - Other Reason", "This product is a duplicate."),
        "Wrong Variation": (
            "1000039 - Product Poorly Created. Each Variation Of This Product Should Be Created Uniquely (Not Authorized)",
            "Create different SKUs instead of variations (variations only for sizes).",
        ),
        "Missing Weight/Volume": (
            "1000008 - Kindly Improve Product Name Description",
            "Include weight or volume (e.g., '1kg', '500ml').",
        ),
        "Incomplete Smartphone Name": (
            "1000008 - Kindly Improve Product Name Description",
            "Include memory/storage details (e.g., '128GB').",
        ),
        "Wrong Category": (
            "1000004 - Wrong Category",
            "Assigned to Wrong Category. Please use correct category.",
        ),
        "Poor images": (
            "1000042 - Kindly follow our product image upload guideline.",
            "Poor Image Quality",
        ),
        "Perfume Tester": (
            "1000007 - Other Reason",
            "Sale of perfume testers is not permitted on Jumia.",
        ),
        "NG - Gift Card Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell Gift Cards in these categories.",
        ),
        "NG - Books Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell this book title, or no seller is approved for it.",
        ),
        "NG - TV Brand Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell this TV brand.",
        ),
        "NG - HP Toners Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell HP Ink/Toners in these categories.",
        ),
        "NG - Apple Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell Apple products.",
        ),
        "NG - Xmas Tree Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell Christmas Tree products.",
        ),
        "NG - Rice Brand Seller": (
            "1000003 - Restricted Brand",
            "Seller not authorised to sell this rice brand.",
        ),
        "NG - Powerbank Capacity": (
            "1000007 - Other Reason",
            "Only approved brands may list powerbanks with 20,000mAh or above capacity.",
        ),
    }

    default_mapping = {
        k: {"reason": v[0], "en": v[1], "fr": v[1], "ar": v[1]}
        for k, v in raw_default.items()
    }

    # Pricing flags
    pricing_reason_code = "1000031 - Kindly Review & Update This Product's Price or Confirm The Price Is Correct By Raising A Claim"
    pricing_en = (
        "The current price of your product differs significantly from the market average.\n"
        "Please review and update the price accordingly, or if you believe the current price is correct, raise a claim with supporting justification.\n\n"
        "Also, keep in mind:\n"
        "- Promotional periods must not exceed 90 days.\n"
        "- Misleading promotions are strictly prohibited.\n"
        "- The original (pre-discount) price must be accurate and should not be inflated before applying a discount."
    )
    pricing_fr = (
        "Le prix actuel de votre produit diffère fortement de la moyenne du marché.\n"
        "Veuillez le revoir et le mettre à jour en conséquence. Si vous estimez que le prix est justifié, vous pouvez soumettre une réclamation accompagnée de preuves.\n\n"
        "À noter également :\n"
        "- Les périodes promotionnelles ne doivent pas dépasser 90 jours.\n"
        "- Les promotions trompeuses sont strictement interdites.\n"
        "- Le prix d'origine (avant remise) doit être exact et ne doit pas être artificiellement gonflé avant l'application de la réduction."
    )
    pricing_ar = (
        "سعر المنتج الحالي يختلف بشكل ملحوظ عن متوسط السوق.\n"
        "يرجى مراجعة السعر وتحديثه، أو في حال كنت ترى أن السعر صحيح، يمكنك تقديم طلب مراجعة (Claim) مع تقديم ما يثبت ذلك."
    )
    for flag_key in ("Wrong Price", "Category Max Price Exceeded"):
        default_mapping[flag_key] = {
            "reason": pricing_reason_code,
            "en": pricing_en,
            "fr": pricing_fr,
            "ar": pricing_ar,
        }

    # Morocco flags
    default_mapping["MA - Marque Interdite"] = {
        "reason": "1000028 - Kindly Contact Jumia Seller Support To Confirm Possibility Of Sale Of This Product By Raising A Claim",
        "en": "Please contact Jumia Seller Support and raise a claim to confirm whether this product is eligible for listing.",
        "fr": (
            "Veuillez contacter le Support Vendeur de Jumia et soumettre une réclamation "
            "afin de confirmer si ce produit est éligible à la mise en ligne."
        ),
        "ar": "يرجى التواصل مع فريق دعم بائعين جوميا وتقديم طلب مراجعة (Claim) للتأكد من إمكانية عرض هذا المنتج على المنصة.",
    }
    default_mapping["MA - Produit Interdit"] = {
        "reason": "1000033 - Keywords in your content/ Product name / description has been blacklisted",
        "en": "Your product name or description includes unauthorized or blacklisted keywords.",
        "fr": (
            "Le nom ou la description de votre produit contient des mots-clés non autorisés ou interdits.\n"
            "Veuillez relire attentivement le contenu et supprimer ou remplacer tout mot-clé interdit."
        ),
        "ar": "اسم المنتج أو وصفه يحتوي على كلمات غير مصرح بها. يرجى مراجعة المحتوى بعناية.",
    }

    try:
        if os.path.exists(filename):
            df = pd.read_excel(filename, engine="openpyxl", dtype=str)
            df.columns = df.columns.str.strip().str.lower()
            if (
                "flag" in df.columns
                and "reason" in df.columns
                and "comment" in df.columns
            ):
                flags = df["flag"].astype(str).str.strip()
                reasons = df["reason"].astype(str).str.strip()
                en_col = df["comment"].astype(str).str.strip()
                fr_col = df["french"].astype(str).str.strip() if "french" in df.columns else en_col.copy()
                ar_col = df["arabic"].astype(str).str.strip() if "arabic" in df.columns else en_col.copy()
                fr_col = fr_col.where(fr_col.str.lower().ne("nan") & fr_col.ne(""), en_col)
                ar_col = ar_col.where(ar_col.str.lower().ne("nan") & ar_col.ne(""), en_col)
                valid = flags.str.lower().ne("nan") & flags.ne("")
                custom_mapping = {
                    flag: {"reason": reason, "en": en, "fr": fr, "ar": ar}
                    for flag, reason, en, fr, ar in zip(
                        flags[valid], reasons[valid], en_col[valid], fr_col[valid], ar_col[valid]
                    )
                }
                if custom_mapping:
                    ng_keys = {
                        k: v
                        for k, v in default_mapping.items()
                        if k.startswith("NG - ")
                    }
                    return {**custom_mapping, **ng_keys}
    except Exception as e:
        logger.warning(f"load_flags_mapping({filename}): {e}")

    return default_mapping


@st.cache_data(ttl=3600)
def load_all_support_files() -> Dict:
    """Load all support/config files into a single dictionary."""
    from nigeria_rules import load_nigeria_qc_rules

    def safe_txt(f):
        return load_txt_file(f) if os.path.exists(f) else []

    support = {
        "blacklisted_words": safe_txt("blacklisted.txt"),
        "book_category_codes": safe_txt("Books_cat.txt"),
        "books_data": load_books_data_from_local(),
        "perfume_category_codes": safe_txt("Perfume_cat.txt"),
        "perfume_data": load_perfume_data_from_local(),
        "perfume_catalog": load_perfume_catalog_from_local(
            _mtime=_file_mtime("perfume_catalog.xlsx")
        ),
        "sneaker_category_codes": safe_txt("Sneakers_Cat.txt"),
        "sneaker_sensitive_brands": [
            b.lower() for b in safe_txt("Sneakers_Sensitive.txt")
        ],
        "sensitive_words": [w.lower() for w in safe_txt("sensitive_words.txt")],
        "unnecessary_words": [w.lower() for w in safe_txt("unnecessary.txt")],
        "colors": [c.lower() for c in safe_txt("colors.txt")],
        "color_categories": safe_txt("color_cats.txt"),
        "category_fas": safe_txt("Fashion_cat.txt"),
        "reasons": load_excel_file("reasons.xlsx"),
        "flags_mapping": load_flags_mapping(),
        "jerseys_data": load_jerseys_from_local(),
        "warranty_category_codes": safe_txt("warranty.txt"),
        "suspected_fake": load_suspected_fake_from_local(),
        "duplicate_exempt_codes": safe_txt("duplicate_exempt.txt"),
        "restricted_brands_all": load_restricted_brands_from_local(),
        "prohibited_words_all": load_prohibited_from_local(),
        "known_brands": safe_txt("brands.txt"),
        "variation_allowed_codes": safe_txt("variation.txt"),
        "weight_category_codes": safe_txt("weight.txt"),
        "smartphone_category_codes": safe_txt("smartphones.txt"),
        "refurb_data": load_refurb_data_from_local(),
        "ng_qc_rules": load_nigeria_qc_rules(),
    }

    # Category map
    _cat_names, _cat_path_to_code, _code_to_path = [], {}, {}
    _cm_path = "category_map.xlsx"
    try:
        if os.path.exists(_cm_path):
            _cm_df = pd.read_excel(_cm_path, engine="openpyxl", dtype=str)
            _cm_df.columns = [c.strip() for c in _cm_df.columns]
            _path_col = next(
                (c for c in _cm_df.columns if c.lower() == "category path"), None
            ) or next((c for c in _cm_df.columns if "path" in c.lower()), None)
            _code_col = next((c for c in _cm_df.columns if "code" in c.lower()), None)
            if _path_col:
                _valid = _cm_df[_path_col].dropna().astype(str)
                _valid = _valid[_valid.str.strip().ne("")]
                _cat_names = _valid.tolist()
                if _code_col:
                    for _, _row in _cm_df[[_path_col, _code_col]].dropna().iterrows():
                        _p = str(_row[_path_col]).strip()
                        _c = str(_row[_code_col]).strip().split(".")[0]
                        if _p and _c:
                            _cat_path_to_code[_p.lower()] = _c
                            _code_to_path[_c] = _p
        else:
            logger.warning(f"[CategoryMap] {_cm_path} not found.")
    except Exception as _ce:
        logger.error(f"[CategoryMap] Failed to load {_cm_path}: {_ce}")

    support["categories_names_list"] = _cat_names
    support["cat_path_to_code"] = _cat_path_to_code
    support["code_to_path"] = _code_to_path
    support["category_map"] = {}

    # JSON weighted rules
    support["compiled_json_rules"] = load_and_compile_json_rules()
    return support


@st.cache_resource(ttl=3600)
def load_and_compile_json_rules(json_path="category_qc_weighted.json") -> dict:
    import re as _re

    if not os.path.exists(json_path):
        logger.warning(f"{json_path} not found.")
        return {}
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            raw_rules = json.load(f)
    except Exception as e:
        logger.warning(f"Could not load JSON rules: {e}")
        return {}

    if isinstance(raw_rules, list):
        fixed = {}
        for item in raw_rules:
            if isinstance(item, dict):
                cat = (
                    item.get("category")
                    or item.get("Category Path")
                    or item.get("name")
                    or item.get("category_name")
                )
                kws = (
                    item.get("keywords") or item.get("weights") or item.get("positive")
                )
                if cat and isinstance(kws, dict):
                    fixed[cat] = kws
        raw_rules = fixed

    if not isinstance(raw_rules, dict):
        logger.warning("JSON rules file has unrecognizable format.")
        return {}

    compiled_rules = {}
    for cat_path, keywords_dict in raw_rules.items():
        if not isinstance(keywords_dict, dict) or not keywords_dict:
            continue
        try:
            safe_kws = {str(k): float(w) for k, w in keywords_dict.items()}
            sorted_kws = sorted(safe_kws.keys(), key=len, reverse=True)
            if not sorted_kws:
                continue
            pattern_str = r"\b(" + "|".join(_re.escape(k) for k in sorted_kws) + r")\b"
            compiled_rules[str(cat_path)] = {
                "pattern": _re.compile(pattern_str, _re.IGNORECASE),
                "weights": {k.lower(): w for k, w in safe_kws.items()},
            }
        except Exception as e:
            logger.warning(f"Skipping bad JSON rule for {cat_path}: {e}")
    return compiled_rules


@st.cache_data(ttl=3600)
def load_support_files_lazy():
    return load_all_support_files()


@st.cache_data(ttl=3600)
def compile_regex_patterns(words: List[str]) -> re.Pattern:
    if not words:
        return None
    pattern = "|".join(
        r"\b" + re.escape(w) + r"\b" for w in sorted(words, key=len, reverse=True)
    )
    return re.compile(pattern, re.IGNORECASE)
