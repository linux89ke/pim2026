"""
data_utils.py - Data loading, cleaning, transformation and validation helpers
"""

import re
import hashlib
import logging
import os
import pandas as pd
from io import BytesIO
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

from constants import NEW_FILE_MAPPING, COLOR_VARIANT_TO_BASE, MULTI_COUNTRY_VALUES, PARQUET_CACHE_DIR

logger = logging.getLogger(__name__)

def save_df_parquet(df, filename):
    try:
        os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
        df.to_parquet(os.path.join(PARQUET_CACHE_DIR, filename))
    except Exception as e:
        logger.warning(f"Failed to save parquet {filename}: {e}")


def load_df_parquet(filename):
    path = os.path.join(PARQUET_CACHE_DIR, filename)
    if os.path.exists(path):
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logger.warning(f"Failed to load parquet {filename}: {e}")
    return None


# -------------------------------------------------
# TEXT & KEY HELPERS
# -------------------------------------------------

def clean_category_code(code) -> str:
    try:
        if pd.isna(code):
            return ""
        s = str(code).strip()
        if '.' in s:
            s = s.split('.')[0]
        return s
    except:
        return str(code).strip()


def normalize_text(text: str) -> str:
    if pd.isna(text):
        return ""
    text = str(text).lower().strip()
    noise = r'\b(new|sale|original|genuine|authentic|official|premium|quality|best|hot|2024|2025)\b'
    text = re.sub(noise, '', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text


def create_match_key(row: pd.Series) -> str:
    name = normalize_text(row.get('NAME', ''))
    brand = normalize_text(row.get('BRAND', ''))
    color = normalize_text(row.get('COLOR', ''))
    return f"{brand}|{name}|{color}"


def _normalize_series(s: pd.Series) -> pd.Series:
    _noise = r'\b(new|sale|original|genuine|authentic|official|premium|quality|best|hot|2024|2025)\b'
    return (
        s.astype(str).str.lower().str.strip()
        .str.replace(_noise, '', regex=True)
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.replace(r'\s+', '', regex=True)
    )


def create_match_key_vectorized(df: pd.DataFrame) -> pd.Series:
    """Vectorized equivalent of create_match_key — ~10x faster on large DataFrames."""
    brand = _normalize_series(df.get("BRAND", pd.Series("", index=df.index)))
    name = _normalize_series(df.get("NAME", pd.Series("", index=df.index)))
    color = _normalize_series(df.get("COLOR", pd.Series("", index=df.index)))
    return brand + "|" + name + "|" + color


def df_hash(df: pd.DataFrame) -> str:
    """Fast fingerprint: full content hash. Result is cached in df.attrs to avoid recomputation."""
    cached = df.attrs.get('__pim_hash__')
    if cached is not None:
        return cached
    try:
        if df.empty:
            result = "empty"
        else:
            # Use pandas built-in hashing for fast, accurate full-content hashing
            import hashlib
            result = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values.tobytes()).hexdigest()
    except Exception as e:
        logger.warning(f"df_hash primary failed, using fallback: {e}")
        fallback_str = str(df.shape) + str(df.columns.tolist())
        result = hashlib.md5(fallback_str.encode()).hexdigest()
    df.attrs['__pim_hash__'] = result
    return result


# -------------------------------------------------
# COLOR EXTRACTION HELPERS
# -------------------------------------------------

# Pre-compiled at module load — avoids rebuilding the pattern on every call
_COLOR_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(k) for k in sorted(COLOR_VARIANT_TO_BASE.keys(), key=len, reverse=True)) + r')\b',
    re.IGNORECASE
)


def extract_colors(text: str, explicit_color: Optional[str] = None) -> Set[str]:
    colors = set()
    text_lower = str(text).lower() if text else ""
    if explicit_color and pd.notna(explicit_color):
        color_lower = str(explicit_color).lower().strip()
        for variant, base in COLOR_VARIANT_TO_BASE.items():
            if variant in color_lower:
                colors.add(base)
    for m in _COLOR_PATTERN.finditer(text_lower):
        base = COLOR_VARIANT_TO_BASE.get(m.group(1).lower())
        if base:
            colors.add(base)
    return colors


def remove_attributes(text: str) -> str:
    base = str(text).lower() if text else ""
    base = _COLOR_PATTERN.sub('', base)
    base = re.sub(r'\b(?:xxs|xs|small|medium|large|xl|xxl|xxxl)\b', '', base)
    base = re.sub(r'\b\d+\s*(?:gb|tb|inch|inches|"|ram|memory|ddr|pack|piece|pcs)\b', '', base)
    for word in ['new', 'original', 'genuine', 'authentic', 'official', 'premium', 'quality', 'best', 'hot', 'sale', 'promo', 'deal']:
        base = re.sub(r'\b' + word + r'\b', '', base)
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', ' ', base)).strip()


@dataclass
class ProductAttributes:
    base_name: str
    colors: Set[str]
    sizes: Set[str]
    storage: Set[str]
    memory: Set[str]
    quantities: Set[str]
    raw_name: str


def extract_product_attributes(name: str, explicit_color: Optional[str] = None, brand: Optional[str] = None) -> ProductAttributes:
    name_str = str(name).strip() if pd.notna(name) else ""
    attrs = ProductAttributes(
        base_name="",
        colors=extract_colors(name_str, explicit_color),
        sizes=set(), storage=set(), memory=set(), quantities=set(),
        raw_name=name_str
    )
    base_name = remove_attributes(name_str)
    if brand and pd.notna(brand):
        brand_lower = str(brand).lower().strip()
        if brand_lower not in base_name and brand_lower not in ['generic', 'fashion']:
            base_name = f"{brand_lower} {base_name}"
    attrs.base_name = base_name.strip()
    return attrs


# -------------------------------------------------
# FILE READING HELPERS
# -------------------------------------------------

def _detect_and_read_csv(buf) -> pd.DataFrame:
    _ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1252', 'iso-8859-1']
    raw_bytes = buf.read()
    for enc in _ENCODINGS:
        for sep in [',', ';', '\t']:
            try:
                df = pd.read_csv(BytesIO(raw_bytes), sep=sep, encoding=enc, dtype=str)
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
    return pd.read_csv(BytesIO(raw_bytes), sep=None, engine='python', encoding='utf-8', dtype=str)


def _repair_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    _ILLEGAL_XML = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

    def _fix(val):
        if not isinstance(val, str):
            return val
        for enc in ('cp1252', 'latin-1'):
            try:
                fixed = val.encode(enc).decode('utf-8')
                if fixed != val and '\ufffd' not in fixed:
                    val = fixed
                    break
            except (UnicodeDecodeError, UnicodeEncodeError):
                continue
        return _ILLEGAL_XML.sub('', val)

    for col in df.select_dtypes(include='object').columns:
        try:
            # Vectorized path: encode as latin-1 bytes then decode as utf-8
            repaired = (
                df[col].astype(str)
                .str.encode('latin-1', errors='replace')
                .str.decode('utf-8', errors='replace')
            )
            # Only accept the vectorized result where it actually changed something
            # and didn't introduce replacement chars where original had none
            mask = repaired != df[col].astype(str)
            no_replacement = ~repaired.str.contains('\ufffd', na=False)
            df[col] = df[col].astype(str).where(~(mask & no_replacement), repaired)
            # Strip illegal XML control chars vectorized
            df[col] = df[col].str.replace(_ILLEGAL_XML.pattern, '', regex=True)
        except Exception:
            # Fallback to row-by-row for any column that fails vectorization
            df[col] = df[col].apply(_fix)
    return df


# -------------------------------------------------
# SCHEMA & TRANSFORMATION
# -------------------------------------------------

def standardize_input_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    map_lower = {k.lower(): v for k, v in NEW_FILE_MAPPING.items()}
    renamed = {}
    for col in df.columns:
        col_lower = col.lower()
        renamed[col] = map_lower[col_lower] if col_lower in map_lower else col.upper()
    df = df.rename(columns=renamed)
    # dtype=str is already set at read time in _detect_and_read_csv; .astype(str) is
    # still applied here as a safety net for DataFrames produced by other paths
    for col in ['ACTIVE_STATUS_COUNTRY', 'CATEGORY_CODE', 'BRAND', 'TAX_CLASS', 'NAME', 'SELLER_NAME']:
        if col in df.columns and df[col].dtype != object:
            df[col] = df[col].astype(str)
    if 'MAIN_IMAGE' not in df.columns:
        df['MAIN_IMAGE'] = ''
    return df


def validate_input_schema(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    required = ['PRODUCT_SET_SID', 'NAME', 'BRAND', 'CATEGORY_CODE', 'ACTIVE_STATUS_COUNTRY']
    errors = [f"Missing: {f}" for f in required if f not in df.columns]
    return len(errors) == 0, errors


def filter_by_country(df: pd.DataFrame, country_validator) -> Tuple[pd.DataFrame, List[str]]:
    if 'ACTIVE_STATUS_COUNTRY' not in df.columns:
        return df, []
    s = df['ACTIVE_STATUS_COUNTRY'].astype(str).str.strip().str.upper().str.replace(r'^JUMIA-', '', regex=True)
    df['ACTIVE_STATUS_COUNTRY'] = s
    if country_validator.code == 'NG':
        is_ng = df['ACTIVE_STATUS_COUNTRY'] == 'NG'
        is_multi = df['ACTIVE_STATUS_COUNTRY'].isin(MULTI_COUNTRY_VALUES)
        filtered = df[is_ng | is_multi].copy()
        filtered['_IS_MULTI_COUNTRY'] = is_multi[filtered.index]
    else:
        filtered = df[df['ACTIVE_STATUS_COUNTRY'] == country_validator.code].copy()
        filtered['_IS_MULTI_COUNTRY'] = False
    # Detect all countries present in the file
    prefix_map = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana", "MA": "Morocco"}

    detected_codes = set()
    if 'ACTIVE_STATUS_COUNTRY' in df.columns:
        # Prefer the explicit country column — SKU prefix scanning produces false positives
        # (e.g. seller SKUs like "MA-D1502W2ME" or "MAX 90" wrongly match Morocco)
        detected_codes.update(df['ACTIVE_STATUS_COUNTRY'].dropna().unique())
    else:
        # Fallback: infer from SKU prefixes only when the country column is absent
        sku_cols = [c for c in df.columns if 'SKU' in c.upper() or 'SID' in c.upper()]
        for col in sku_cols:
            vals = df[col].dropna().astype(str).str.strip().str.upper()
            for prefix in prefix_map.keys():
                if vals.str.startswith(prefix).any():
                    detected_codes.add(prefix)
    
    emoji_map = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana", "MA": "Morocco"}
    detected_names = sorted(list(set(emoji_map.get(c, str(c)) for c in detected_codes if str(c).strip() and str(c).strip().lower() != 'nan')))
    
    return filtered, detected_names


def propagate_metadata(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    meta_cols = ['COLOR_FAMILY', 'PRODUCT_WARRANTY', 'WARRANTY_DURATION',
                 'WARRANTY_ADDRESS', 'WARRANTY_TYPE', 'COUNT_VARIATIONS', 'LIST_VARIATIONS']
    for col in meta_cols:
        if col not in df.columns:
            df[col] = pd.NA
    grouped = df.groupby('PRODUCT_SET_SID')
    for col in meta_cols:
        df[col] = grouped[col].transform(lambda x: x.ffill().bfill())
    return df


# -------------------------------------------------
# EXCHANGE RATE & PRICE FORMATTING
# -------------------------------------------------

import streamlit as st

@st.cache_data(ttl=3600)
def fetch_exchange_rate(country: str) -> float:
    from constants import COUNTRY_CURRENCY
    cfg = COUNTRY_CURRENCY.get(country)
    if not cfg:
        return 1.0
    try:
        import urllib.request, json as _json
        with urllib.request.urlopen("https://open.er-api.com/v6/latest/USD", timeout=3) as r:
            data = _json.loads(r.read())
        return float(data["rates"].get(cfg["code"], 1.0))
    except Exception as e:
        logger.warning(f"Exchange rate fetch failed for {country}: {e}")
        fallbacks = {"Kenya": 128.0, "Uganda": 3750.0, "Nigeria": 1550.0, "Ghana": 15.5, "Morocco": 10.1}
        return fallbacks.get(country, 1.0)


def format_local_price(usd_price, country: str) -> str:
    from constants import COUNTRY_CURRENCY
    try:
        price = float(usd_price)
        if price <= 0:
            return ""
        cfg = COUNTRY_CURRENCY.get(country, {})
        rate = fetch_exchange_rate(country)
        local = price * rate
        symbol = cfg.get("symbol", "$")
        if cfg.get("code") in ("KES", "UGX", "NGN"):
            return f"{symbol} {local:,.0f}"
        else:
            return f"{symbol} {local:,.2f}"
    except (ValueError, TypeError):
        return ""

# -------------------------------------------------
# ZIP IMAGE LAZY LOADING (CACHED BASE64)
# -------------------------------------------------

def _basename_lower(value) -> str:
    name = str(value).strip().replace("\\", "/").split("/")[-1].lower()
    return name if name and name != "nan" else ""

def _load_zip_image_by_key(key: str) -> Optional[str]:
    import streamlit as st
    import zipfile
    import base64
    from io import BytesIO
    key = _basename_lower(key)
    if not key:
        return None
    store = st.session_state.setdefault('zip_image_store', {})
    if key in store:
        return store[key]
    member = st.session_state.get('zip_image_index', {}).get(key)
    source_bytes = st.session_state.get('zip_image_source_bytes')
    if not member or not source_bytes:
        return None
    try:
        with zipfile.ZipFile(BytesIO(source_bytes)) as zf:
            img_bytes = zf.read(member)
            encoded = base64.b64encode(img_bytes).decode('utf-8')
            mime = "image/jpeg"
            if key.endswith(".png"): mime = "image/png"
            elif key.endswith(".webp"): mime = "image/webp"
            elif key.endswith(".gif"): mime = "image/gif"
            data_uri = f"data:{mime};base64,{encoded}"
            store[key] = data_uri
            return data_uri
    except Exception as e:
        logger.warning(f"Failed lazy-loading ZIP image {member}: {e}")
        return None

IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.webp', '.gif')

def _get_image_from_zip(name, brand, image_name=None) -> Optional[str]:
    """Try to find image in zip store by product name-Brand or explicit filename."""
    if image_name:
        img_data = _load_zip_image_by_key(image_name)
        if img_data:
            return img_data
    # Product name-Brand
    key = f"{str(name).strip()}-{str(brand).strip()}".lower()
    # Also try variations of extensions
    for ext in [*IMAGE_EXTENSIONS, '']:
        img_data = _load_zip_image_by_key(key + ext)
        if img_data:
            return img_data
    return None
