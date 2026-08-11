"""
loaders.py - All file loading functions for support/config data
"""

import ast
import json
import logging
import os
import re
import uuid
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from data_utils import clean_category_code, normalize_text

logger = logging.getLogger(__name__)

# Module-level cache for compiled regex patterns (avoids re-compilation cost)
_REGEX_CACHE: dict = {}


def _atomic_to_parquet(df: pd.DataFrame, pq_path: str) -> None:
    """Write a parquet cache file atomically (temp file + rename) so concurrent
    thread-pool workers rebuilding the same cache can't corrupt it for each other."""
    tmp_path = f"{pq_path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    df.to_parquet(tmp_path)
    os.replace(tmp_path, pq_path)

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
            
        from data_utils import PARQUET_CACHE_DIR
        pq_name = os.path.basename(filename) + ".parquet"
        pq_path = os.path.join(PARQUET_CACHE_DIR, pq_name)
        
        if os.path.exists(pq_path) and os.path.getmtime(pq_path) > os.path.getmtime(filename):
            try:
                df = pd.read_parquet(pq_path)
            except Exception:
                df = pd.read_excel(filename, engine="openpyxl", dtype=str)
                df.columns = df.columns.astype(str).str.strip()
                os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
                _atomic_to_parquet(df, pq_path)
        else:
            df = pd.read_excel(filename, engine="openpyxl", dtype=str)
            df.columns = df.columns.astype(str).str.strip()
            os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
            _atomic_to_parquet(df, pq_path)
            
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
        from data_utils import PARQUET_CACHE_DIR
        usecols_str = str(usecols).replace(' ', '').replace('[', '').replace(']', '').replace(',', '_') if usecols else 'all'
        pq_name = f"{os.path.basename(filename)}_{sheet_name}_{usecols_str}.parquet"
        pq_name = pq_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        pq_path = os.path.join(PARQUET_CACHE_DIR, pq_name)
        
        if os.path.exists(pq_path) and os.path.getmtime(pq_path) > os.path.getmtime(filename):
            try:
                return pd.read_parquet(pq_path)
            except Exception:
                pass
                
        df = pd.read_excel(
            filename,
            sheet_name=sheet_name,
            usecols=usecols,
            engine="openpyxl",
            dtype=str,
        )
        df = df.dropna(how="all")
        
        try:
            os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
            df.columns = df.columns.astype(str)
            _atomic_to_parquet(df, pq_path)
        except Exception as cache_e:
            logger.warning(f"Failed to cache {pq_path}: {cache_e}")
            
        return df
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
            kw_series = df[keyword_col].fillna("").astype(str).str.strip().str.lower()
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
            # fillna before astype: under pandas' new string dtype astype(str)
            # no longer renders missing values as "nan", so a blank brand cell
            # stayed NaN and reached .lower() below as a float. That took out the
            # whole sheet for KE/NG/GH/MA and, because the loader swallows the
            # exception and returns [], the restricted-brand checks then passed
            # every product silently instead of failing loudly.
            brand_col_vals = df.get("brand", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
            valid = brand_col_vals.str.lower().ne("nan") & brand_col_vals.ne("")
            df = df[valid].copy()
            df["_b_lower"] = brand_col_vals[valid].str.lower().values
            df["_b_norm"] = brand_col_vals[valid].map(normalize_text).values

            def _split_set(series, sep=","):
                # NaN is truthy, so `not x` does not catch it — without the
                # fillna the lambda calls .lower() on a float.
                return series.fillna("").astype(str).str.strip().apply(
                    lambda x: set() if not x or x.lower() in ("nan", "none")
                    else {v.strip().lower() for v in x.split(sep) if v.strip()}
                )

            def _split_cats(series):
                return series.fillna("").astype(str).str.strip().apply(
                    lambda x: None if (not x or x.lower() in ("nan", "none"))
                    else {clean_category_code(c.strip()) for c in x.split(",") if c.strip()}
                )

            sellers_s = _split_set(df.get("approved sellers", pd.Series([""] * len(df), index=df.index)), ",")
            cats_s = _split_cats(df.get("categories", pd.Series([""] * len(df), index=df.index)))
            vars_s = _split_set(df.get("variations", pd.Series([""] * len(df), index=df.index)), ",")
            def _parse_expanded(val):
                s = str(val).strip()
                if not s or s.lower() == "nan":
                    return set()
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, list):
                        return {str(v).strip().lower() for v in parsed if str(v).strip()}
                except Exception:
                    pass
                return {v.strip().lower() for v in s.split(",") if v.strip()}

            exp_vars_col = "expanded variations"
            exp_vars_s = df.get(exp_vars_col, pd.Series([""] * len(df), index=df.index)).apply(_parse_expanded)

            brand_dict: dict = {}
            for b_lower, b_norm, brand_raw, sellers, cats, variations, exp_vars in zip(
                df["_b_lower"], df["_b_norm"], brand_col_vals[valid], sellers_s, cats_s, vars_s, exp_vars_s
            ):
                if b_norm not in brand_dict:
                    brand_dict[b_norm] = {
                        "brand": b_norm,
                        "brand_raw": brand_raw,
                        "sellers": set(),
                        "categories": set(),
                        "variations": set(),
                        "has_blank_category": False,
                    }
                brand_dict[b_norm]["sellers"].update(normalize_text(s) for s in sellers)
                if cats is None:
                    brand_dict[b_norm]["has_blank_category"] = True
                else:
                    brand_dict[b_norm]["categories"].update(cats)
                brand_dict[b_norm]["variations"].update(normalize_text(v) for v in variations)
                brand_dict[b_norm]["variations"].update(normalize_text(v) for v in exp_vars)

            country_rules = [
                {
                    "brand": data["brand"],
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

    # Sheet names in this workbook are not stable — the file in the repo has a
    # single sheet called "Sheet1" carrying exactly the enriched-catalog
    # columns (brand / also_known_as / sample_models). Asking for the sheet by
    # name logged a warning on every load and silently produced an empty
    # catalog, which turns the perfume checks into no-ops without saying so.
    #
    # Resolve by shape instead: take the named sheet if it exists, otherwise
    # the first sheet that has a "brand"-ish column.
    def _resolve_sheet(preferred: str, must_have: tuple) -> str:
        try:
            names = pd.ExcelFile(FILE_NAME).sheet_names
        except Exception:
            return preferred
        if preferred in names:
            return preferred
        for _n in names:
            try:
                _cols = {
                    str(c).strip().lower()
                    for c in pd.read_excel(FILE_NAME, sheet_name=_n, nrows=0).columns
                }
            except Exception:
                continue
            if any(m in _cols for m in must_have):
                logger.info(
                    "load_perfume_catalog: sheet '%s' not found, using '%s' "
                    "(it has the expected columns)", preferred, _n,
                )
                return _n
        return preferred

    # ── Sheet 1: enriched catalog (brands + aliases + models) ───────────────
    try:
        df = safe_excel_read(
            FILE_NAME,
            sheet_name=_resolve_sheet("enriched_perfume_catalog", ("brand", "brands")),
        )
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
                    for brand, aka_raw in zip(brands_v, df_v[aka_col].fillna("").astype(str).str.strip()):
                        if aka_raw.lower() in _skip:
                            continue
                        for alias in re.split(r"[/,]", aka_raw):
                            alias = alias.strip().lower()
                            if alias and alias not in _skip:
                                result["legit_brand_terms"].add(alias)
                                result["term_to_brand"].setdefault(alias, brand)

                if model_col:
                    _skip = {"", "nan", "-", "none"}
                    for brand, models_raw in zip(brands_v, df_v[model_col].fillna("").astype(str).str.strip()):
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
    # Genuinely optional — the repo's workbook has no such sheet. Absence is
    # logged once at info level rather than warned about on every load; a
    # warning that fires every time for a file that is simply not there trains
    # people to ignore the log.
    _has_brands_sheet = True
    try:
        _has_brands_sheet = "Brands" in pd.ExcelFile(FILE_NAME).sheet_names
    except Exception:
        pass
    if not _has_brands_sheet:
        logger.info(
            "load_perfume_catalog: no 'Brands' sheet in %s — the fake-brand "
            "list is empty, which is expected unless you have added one.",
            FILE_NAME,
        )
    try:
        df_brands = (
            safe_excel_read(FILE_NAME, sheet_name="Brands")
            if _has_brands_sheet else pd.DataFrame()
        )
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
            "1000030 - Suspected Counterfeit/Fake Product.Please Contact Seller Support By Raising A Claim , For Questions & Inquiries (Not Authorized)",
            "This product is suspected to be a counterfeit perfume. A genuine brand name or model was detected in the product title while the listed brand is a known evasion label.",
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
        # Prefetch-only validations (sourced from QC ZIP file)
        "FDA": (
            "1000007 - Other Reason",
            "Kindly Provide Product's Health/Food Regulation Registration Number.",
        ),
        "Title Language Check": (
            "1000008 - Kindly Improve Product Name Description",
            "Include weight or volume (e.g., '1kg', '500ml').",
        ),
        "Brand Image Check": (
            "1000042 - Kindly follow our product image upload guideline.",
            "Brand detected on product image does not match the declared brand.",
        ),
        "Image Quality Check": (
            "1000042 - Kindly follow our product image upload guideline.",
            "Poor Image Quality",
        ),
        "Variation Check": (
            "1000039 - Product Poorly Created. Each Variation Of This Product Should Be Created Uniquely (Not Authorized)",
            "Create different SKUs instead of variations (variations only for sizes).",
        ),
        "Color Check": (
            "1000005 - Kindly confirm the actual product colour",
            "Product color must be mentioned in title/color tab.",
        ),
        "Category Check": (
            "1000004 - Wrong Category",
            "Assigned to Wrong Category. Please use correct category.",
        ),
        "Warranty Check": (
            "1000013 - Kindly Provide Product Warranty Details",
            "Valid warranty required in Description/Warranty tabs.",
        ),
        "Product Name Brand Name": (
            "1000007 - Other Reason",
            "Brand name should not be repeated in product name.",
        ),
        "Brand Image Mismatch": (
            "1000042 - Kindly follow our product image upload guideline.",
            "The brand detected on the product image does not match the declared brand. Kindly confirm the correct brand or update the product image.",
        ),
        "Off-Platform Contact": (
            "1000033 - Keywords in your content/ Product name / description has been blacklisted",
            "The product name/description appears to contain a phone number, WhatsApp link, website, or wording directing buyers off-platform. Kindly remove all off-platform contact details.",
        ),
        "Specs Inconsistency": (
            "1000008 - Kindly Improve Product Name Description",
            "The RAM/Storage spec stated in the product title does not match what's stated in the description. Kindly correct the description to match the exact variant being sold.",
        ),
    }

    default_mapping = {
        k: {"reason": v[0], "en": v[1], "fr": v[1], "ar": v[1]}
        for k, v in raw_default.items()
    }

    # Override FDA with proper French translation (for Senegal / Ivory Coast)
    default_mapping["FDA"] = {
        "reason": "1000007 - Other Reason",
        "en": "Kindly Provide Product's Health/Food Regulation Registration Number.",
        "fr": (
            "Veuillez fournir le numéro d'enregistrement réglementaire "
            "sanitaire/alimentaire du produit (numéro FDA ou équivalent)."
        ),
        "ar": "يرجى تقديم رقم تسجيل المنتج الصحي/الغذائي (رقم FDA أو ما يعادله).",
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
    for flag_key in ("Wrong Price", "Category Max Price Exceeded", "Discount too high", "Suspicious Discount"):
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
                    # Merge on top of default_mapping instead of replacing it —
                    # reason.xlsx has historically covered only a subset of the
                    # flags the app can actually produce (e.g. it was missing
                    # "Perfume Tester", "Suspected Fake Perfume", "Title Language
                    # Check", and used "Discount too High" where the code looks
                    # up "Discount too high"). Any flag the spreadsheet doesn't
                    # cover now keeps its code-defined default instead of
                    # silently falling back to generic "Other Reason" text.
                    # Case-insensitive match so spreadsheet casing typos (like
                    # the "High"/"high" example above) still land on the
                    # correct canonically-cased key the app looks up by.
                    merged = dict(default_mapping)
                    lower_to_canonical = {k.lower(): k for k in merged}
                    for flag, info in custom_mapping.items():
                        canonical = lower_to_canonical.get(flag.lower(), flag)
                        merged[canonical] = info
                    return merged
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
            # Parquet mirror. This is a 30,085-row workbook and openpyxl takes
            # 5.4s to parse it — the single largest item in a ~14.5s support
            # load, paid on every cold start including every Cloud container.
            # Parquet reads the same frame in a fraction of that.
            #
            # load_excel_file() already does this for the files it handles;
            # this read never went through it. Same invalidation rule: the
            # mirror is used only while it is newer than the workbook, so
            # editing category_map.xlsx still takes effect immediately.
            from data_utils import PARQUET_CACHE_DIR
            _cm_pq = os.path.join(PARQUET_CACHE_DIR, "category_map.xlsx.parquet")
            _cm_df = None
            if os.path.exists(_cm_pq) and os.path.getmtime(_cm_pq) > os.path.getmtime(_cm_path):
                try:
                    _cm_df = pd.read_parquet(_cm_pq)
                except Exception as _pq_err:
                    logger.warning("category_map parquet unreadable (%s), re-reading xlsx", _pq_err)
                    _cm_df = None
            if _cm_df is None:
                _cm_df = pd.read_excel(_cm_path, engine="openpyxl", dtype=str)
                _cm_df.columns = [str(c).strip() for c in _cm_df.columns]
                try:
                    os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
                    _atomic_to_parquet(_cm_df, _cm_pq)
                except Exception as _w_err:
                    logger.warning("Could not write category_map parquet: %s", _w_err)
            _cm_df.columns = [str(c).strip() for c in _cm_df.columns]
            _path_col = next(
                (c for c in _cm_df.columns if c.lower() == "category path"), None
            ) or next((c for c in _cm_df.columns if "path" in c.lower()), None)
            _code_col = next((c for c in _cm_df.columns if "code" in c.lower()), None)
            if _path_col:
                _valid = _cm_df[_path_col].dropna().astype(str)
                _valid = _valid[_valid.str.strip().ne("")]
                _cat_names = _valid.tolist()
                if _code_col:
                    _sub = _cm_df[[_path_col, _code_col]].dropna()
                    _sub = _sub.copy()
                    _sub[_code_col] = _sub[_code_col].astype(str).str.strip().str.split('.').str[0]
                    _sub[_path_col] = _sub[_path_col].astype(str).str.strip()
                    _valid_mask = _sub[_path_col].ne('') & _sub[_code_col].ne('')
                    _sub = _sub[_valid_mask]
                    _cat_path_to_code = dict(zip(_sub[_path_col].str.lower(), _sub[_code_col]))
                    _code_to_path     = dict(zip(_sub[_code_col], _sub[_path_col]))
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


# cache_resource, not cache_data: this dict is read-only reference data shared
# by every rerun and every session. cache_data stores the value pickled and
# hands back a fresh deep copy on each access — ~97ms per rerun for this ~6.5MB
# payload, paid on every click. cache_resource returns the same object for free.
# Callers only ever read from it (verified: no writes to support_files anywhere).
@st.cache_resource(ttl=3600)
def load_support_files_lazy():
    return load_all_support_files()


def compile_regex_patterns(words: List[str], flags=re.IGNORECASE) -> re.Pattern:
    if not words:
        return None
    key = (tuple(sorted(words)), flags)
    if key in _REGEX_CACHE:
        return _REGEX_CACHE[key]
    pattern = "|".join(
        r"\b" + re.escape(w) + r"\b" for w in sorted(words, key=len, reverse=True)
    )
    compiled = re.compile(pattern, flags)
    _REGEX_CACHE[key] = compiled
    return compiled
