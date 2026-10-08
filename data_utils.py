"""
data_utils.py - Data loading, cleaning, transformation and validation helpers
"""

import json
import re
import csv
import warnings
import hashlib
import logging
import os
import time
import unicodedata
import uuid
import pandas as pd
from io import BytesIO
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass

from constants import NEW_FILE_MAPPING, COLOR_VARIANT_TO_BASE, MULTI_COUNTRY_VALUES, PARQUET_CACHE_DIR

# ---------------------------------------------------------------------------
# Load mojibake substitution map once at import time
# ---------------------------------------------------------------------------
_MOJIBAKE_MAP: Dict[str, str] = {}
try:
    _mj_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mojibake_map.json")
    if os.path.exists(_mj_path):
        with open(_mj_path, "r", encoding="utf-8") as _f:
            _MOJIBAKE_MAP = json.load(_f)
except Exception:
    pass

logger = logging.getLogger(__name__)

def save_df_parquet(df, filename):
    try:
        os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
        pq_path = os.path.join(PARQUET_CACHE_DIR, filename)
        # Write to a temp file then rename, so concurrent readers/writers never see a partial file.
        tmp_path = f"{pq_path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        df.to_parquet(tmp_path)
        os.replace(tmp_path, pq_path)
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

def list_cached_sessions():
    sessions = []
    if not os.path.exists(PARQUET_CACHE_DIR):
        return sessions
    for f in os.listdir(PARQUET_CACHE_DIR):
        if f.endswith("_report.parquet"):
            sig_hash = f.replace("_report.parquet", "")
            path = os.path.join(PARQUET_CACHE_DIR, f)
            mtime = os.path.getmtime(path)
            try:
                # Just get shape without full load if possible, or load it
                df = load_df_parquet(f)
                if df is not None:
                    total = len(df)
                    rej = len(df[df["Status"] == "Rejected"])
                    sessions.append({
                        "sig_hash": sig_hash,
                        "mtime": mtime,
                        "total": total,
                        "rejected": rej
                    })
            except:
                pass
    sessions.sort(key=lambda x: x["mtime"], reverse=True)
    return sessions


# -------------------------------------------------
# MANUAL DECISION JOURNAL
# -------------------------------------------------
# Manual approve/reject decisions are the only part of a review that cannot be
# recomputed, so they are persisted twice:
#   • inside {sig_hash}_report.parquet, which the startup fast path reloads, and
#   • in the journal below, keyed on the uploaded FILE CONTENT alone.
# The second key is what makes recovery reliable. sig_hash also folds in the
# category-learning row count and PROCESSING_CACHE_VERSION, and either can
# change between sessions; when that happens the checkpointed report is
# orphaned under a hash nothing looks up again, and the decisions are lost even
# though they sit on disk. The journal survives it: same files in ⇒ same key ⇒
# decisions re-applied on top of a freshly validated report.

MANUAL_DECISION_COLS = [
    "ProductSetSid", "Status", "Reason", "Comment", "FLAG", "Is_Manual", "Is_Zip",
]
MANUAL_DECISION_PREFIX = "manual_"

# The journal key is content-addressed over the WHOLE uploaded set, which is
# what makes it reliable for reopening the same batch — and what breaks it the
# moment the set grows. Upload the CSV, review 900 products, then add the image
# ZIP: the signature changes, the lookup asks for a key that has never existed,
# and a full day of decisions sits on disk unreachable. (That is not
# hypothetical — it is how this was found.)
#
# The filename is md5(signature), so nothing can be recovered from the file
# alone; there is no way to ask "which files did this belong to?". This index
# records that, so a journal written for a subset of the current upload can be
# found and offered back.
MANUAL_INDEX_FILE = "manual_index.json"


def _manual_index_path() -> str:
    return os.path.join(PARQUET_CACHE_DIR, MANUAL_INDEX_FILE)


def _read_manual_index() -> dict:
    try:
        with open(_manual_index_path(), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        # A corrupt or absent index must never block a review. Worst case the
        # offer does not appear; the journals themselves are untouched.
        return {}


def _write_manual_index(index: dict) -> None:
    try:
        os.makedirs(PARQUET_CACHE_DIR, exist_ok=True)
        path = _manual_index_path()
        tmp = f"{path}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(index, fh)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"Failed to write manual index: {e}")


def manual_decisions_filename(process_signature: str) -> str:
    """Journal filename for an uploaded file set (content-addressed, stable)."""
    return f"{MANUAL_DECISION_PREFIX}{hashlib.md5(str(process_signature).encode()).hexdigest()}.parquet"


def save_manual_decisions(
    process_signature: str,
    final_report,
    *,
    file_tokens=None,
    country: str = "",
) -> int:
    """Persist every manually-decided row. Returns how many were written.

    file_tokens is the per-file "name+content-digest" list the signature was
    built from. It is recorded in the index so a later, larger upload can find
    this journal; without it the entry is still written, just unmatchable.
    """
    if not process_signature or process_signature == "empty":
        return 0
    if not isinstance(final_report, pd.DataFrame) or final_report.empty:
        return 0
    if not {"Is_Manual", "ProductSetSid"}.issubset(final_report.columns):
        return 0

    fname = manual_decisions_filename(process_signature)
    decided = final_report[final_report["Is_Manual"] == True]  # noqa: E712
    cols = [c for c in MANUAL_DECISION_COLS if c in decided.columns]
    decided = decided[cols].drop_duplicates(subset=["ProductSetSid"], keep="last")

    index = _read_manual_index()
    if decided.empty:
        # Undoing back to zero must clear the journal, or the next load would
        # resurrect decisions the user deliberately removed. The index entry
        # goes with it, or the offer would point at a file that is gone.
        try:
            os.remove(os.path.join(PARQUET_CACHE_DIR, fname))
        except OSError:
            pass
        if index.pop(fname, None) is not None:
            _write_manual_index(index)
        return 0

    save_df_parquet(decided, fname)
    index[fname] = {
        "files": sorted(str(t) for t in (file_tokens or [])),
        "country": str(country or ""),
        "saved_at": time.time(),
        "n": int(len(decided)),
    }
    _write_manual_index(index)
    return len(decided)


def find_predecessor_decisions(file_tokens, country: str, current_signature: str = ""):
    """Find a journal written for a strict subset of the current upload.

    That subset relation is the whole point: it is what "I started with the
    product file and added the image ZIP later" looks like on disk. An equal
    set is deliberately excluded — that is the same batch reopened, which the
    normal same-key load already handles and which must not prompt.

    Returns the best candidate as a dict with its loaded decisions, or None.
    Best means most overlap with the current upload, then most recent.
    """
    tokens = {str(t) for t in (file_tokens or [])}
    if not tokens:
        return None

    best = None
    for fname, meta in _read_manual_index().items():
        if not isinstance(meta, dict):
            continue
        if fname == manual_decisions_filename(current_signature):
            continue
        # Country is part of the signature, so a journal from a different
        # market would apply the wrong rules' verdicts onto this report.
        if str(meta.get("country") or "") != str(country or ""):
            continue
        old = {str(t) for t in (meta.get("files") or [])}
        if not old or not old < tokens:      # strict subset only
            continue
        if not os.path.exists(os.path.join(PARQUET_CACHE_DIR, fname)):
            continue                          # index outlived its journal
        rank = (len(old), float(meta.get("saved_at") or 0))
        if best is None or rank > best["_rank"]:
            best = {
                "_rank": rank,
                "filename": fname,
                "files": sorted(old),
                "added": sorted(tokens - old),
                "saved_at": float(meta.get("saved_at") or 0),
                "n": int(meta.get("n") or 0),
            }
    if best is None:
        return None

    decisions = load_df_parquet(best["filename"])
    if decisions is None or decisions.empty:
        return None
    best["decisions"] = decisions
    return best


def preview_decision_merge(final_report, decisions) -> dict:
    """Describe what applying `decisions` would do, without doing it.

    The counts here are the whole basis of the confirmation prompt, so they are
    computed against the real report rather than estimated from row totals.
    """
    out = {"total": 0, "matched": 0, "missing": 0, "conflicts": 0, "unchanged": 0}
    if not isinstance(decisions, pd.DataFrame) or decisions.empty:
        return out
    out["total"] = len(decisions)
    if not isinstance(final_report, pd.DataFrame) or final_report.empty:
        out["missing"] = out["total"]
        return out
    if "ProductSetSid" not in final_report.columns or "ProductSetSid" not in decisions.columns:
        out["missing"] = out["total"]
        return out

    dec = decisions.drop_duplicates(subset=["ProductSetSid"], keep="last").copy()
    dec["ProductSetSid"] = dec["ProductSetSid"].fillna("").astype(str).str.strip()
    dec = dec.set_index("ProductSetSid")

    fr_sids = final_report["ProductSetSid"].fillna("").astype(str).str.strip()
    present = fr_sids.isin(dec.index)
    out["matched"] = int(present.sum())
    out["missing"] = out["total"] - len(set(fr_sids[present]) & set(dec.index))

    if "Status" in final_report.columns and "Status" in dec.columns:
        cur = final_report.loc[present, "Status"].fillna("").astype(str)
        want = fr_sids[present].map(dec["Status"]).fillna("").astype(str)
        differs = (cur.values != want.values)
        out["conflicts"] = int(differs.sum())
        out["unchanged"] = int(out["matched"] - out["conflicts"])
    return out


def load_manual_decisions(process_signature: str):
    """Return the journalled decisions for an uploaded file set, or None."""
    if not process_signature or process_signature == "empty":
        return None
    return load_df_parquet(manual_decisions_filename(process_signature))


def manual_decisions_mtime(process_signature: str) -> int:
    """Return the journal's nanosecond mtime for cross-session refreshes.

    Manual decisions are written to a content-addressed parquet journal.  A
    Streamlit tab can therefore cheaply detect that another tab changed the
    same upload without re-reading or re-running validation on every rerun.
    ``0`` also represents a journal that does not exist yet.
    """
    if not process_signature or process_signature == "empty":
        return 0
    try:
        return int(os.stat(os.path.join(PARQUET_CACHE_DIR, manual_decisions_filename(process_signature))).st_mtime_ns)
    except OSError:
        return 0


def apply_manual_decisions(final_report, decisions) -> int:
    """Re-apply journalled decisions onto a report in place. Returns rows changed."""
    if not isinstance(final_report, pd.DataFrame) or final_report.empty:
        return 0
    if not isinstance(decisions, pd.DataFrame) or decisions.empty:
        return 0
    if "ProductSetSid" not in final_report.columns or "ProductSetSid" not in decisions.columns:
        return 0

    fr_sids = final_report["ProductSetSid"].astype(str).str.strip()
    dec = decisions.copy()
    dec["ProductSetSid"] = dec["ProductSetSid"].astype(str).str.strip()
    dec = dec.drop_duplicates(subset=["ProductSetSid"], keep="last").set_index("ProductSetSid")

    mask = fr_sids.isin(dec.index)
    if not mask.any():
        return 0

    target = fr_sids[mask]
    for col in ("Status", "Reason", "Comment", "FLAG", "Is_Manual", "Is_Zip"):
        if col in dec.columns and col in final_report.columns:
            final_report.loc[mask, col] = target.map(dec[col])
    return int(mask.sum())


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
    if str(text).strip().lower() in ("nan", "none"):
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
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


# Pre-compiled noise pattern shared by normalize_text and _normalize_series
_NOISE_PATTERN = re.compile(
    r'\b(new|sale|original|genuine|authentic|official|premium|quality|best|hot|2024|2025)\b',
    re.IGNORECASE,
)


def _normalize_series(s: pd.Series) -> pd.Series:
    return (
        # fillna first: astype(str) stops rendering NaN as "nan" under
        # pandas' new string dtype, and the lambda below would then call
        # unicodedata.normalize() and .lower() on a float. This helper
        # normalises brand and name columns, so it fails broadly.
        s.fillna("").astype(str)
        .map(lambda x: unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("ascii") if x and x.lower() not in ("nan", "none") else "")
        .str.lower().str.strip()
        .str.replace(_NOISE_PATTERN, '', regex=True)
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
    """Fast fingerprint: full content hash. Result is memoised in df.attrs.

    The memo is validated against the frame's shape and column list before it is
    trusted. DataFrame.copy() propagates .attrs, so without that check a frame
    copied from an already-hashed one — then given extra columns, which is
    exactly what validate_products() does — kept reporting the ORIGINAL frame's
    hash, silently serving caches keyed on the wrong content.

    Note this still cannot detect an in-place edit that leaves shape and columns
    unchanged (e.g. df.loc[0, "COLOR"] = "teal"). Callers that must be exact
    about value-level changes should hash the columns they care about directly;
    see _ColumnDigests in streamlit_app.py.
    """
    _stamp = (df.shape, tuple(df.columns))
    cached = df.attrs.get('__pim_hash__')
    if cached is not None and df.attrs.get('__pim_hash_stamp__') == _stamp:
        return cached
    try:
        if df.empty:
            result = "empty"
        else:
            # Use pandas built-in hashing for fast, accurate full-content hashing
            result = hashlib.md5(pd.util.hash_pandas_object(df, index=False).values.tobytes()).hexdigest()
    except Exception as e:
        logger.warning(f"df_hash primary failed, using fallback: {e}")
        fallback_str = str(df.shape) + str(df.columns.tolist())
        result = hashlib.md5(fallback_str.encode()).hexdigest()
    df.attrs['__pim_hash__'] = result
    df.attrs['__pim_hash_stamp__'] = _stamp
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


# Pre-compiled patterns for remove_attributes — eliminates 12 separate re.sub calls per invocation
_ATTR_NOISE_RE = re.compile(
    r'\b(new|original|genuine|authentic|official|premium|quality|best|hot|sale|promo|deal)\b',
    re.IGNORECASE,
)
_SIZE_RE = re.compile(r'\b(?:xxs|xs|small|medium|large|xl|xxl|xxxl)\b', re.IGNORECASE)
_SPEC_RE = re.compile(
    r'\b\d+\s*(?:gb|tb|inch|inches|"|ram|memory|ddr|pack|piece|pcs)\b', re.IGNORECASE
)


def remove_attributes(text: str) -> str:
    base = str(text).lower() if text else ""
    base = _COLOR_PATTERN.sub('', base)
    base = _SIZE_RE.sub('', base)
    base = _SPEC_RE.sub('', base)
    base = _ATTR_NOISE_RE.sub('', base)
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
    # latin1 never raises UnicodeDecodeError so it acts as the final safe fallback
    _ENCODINGS = ['utf-8-sig', 'utf-8', 'cp1252', 'iso-8859-1', 'latin1']
    if hasattr(buf, 'seek'):
        try:
            buf.seek(0)
        except Exception:
            pass
    raw_bytes = buf.read()
    if not raw_bytes:
        return pd.DataFrame()

    # 1. Fast sniff using Python's csv.Sniffer on the initial chunk
    sample_bytes = raw_bytes[:16384]
    detected_enc = None
    sample_text = None
    for enc in _ENCODINGS:
        try:
            sample_text = sample_bytes.decode(enc)
            detected_enc = enc
            break
        except UnicodeDecodeError:
            continue
    if sample_text is None:
        detected_enc = 'latin1'
        sample_text = sample_bytes.decode('latin1', errors='replace')

    candidate_seps = [',', ';', '\t']
    if sample_text:
        try:
            sniffer = csv.Sniffer()
            sniffed_dialect = sniffer.sniff(sample_text, delimiters=[',', ';', '\t', '|'])
            if sniffed_dialect.delimiter in [',', ';', '\t', '|']:
                candidate_seps = [sniffed_dialect.delimiter] + [s for s in [',', ';', '\t'] if s != sniffed_dialect.delimiter]
        except Exception:
            pass

    # 2. Score candidate encodings and separators without leaking ParserWarnings
    best_enc = detected_enc or 'latin1'
    best_sep = candidate_seps[0]
    best_score = (-1, -1)
    found = False

    enc_order = [detected_enc] + [e for e in _ENCODINGS if e != detected_enc] if detected_enc else _ENCODINGS

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        for enc in enc_order:
            for sep in candidate_seps:
                try:
                    df_chunk = pd.read_csv(
                        BytesIO(raw_bytes), sep=sep, encoding=enc,
                        dtype=str, nrows=20, on_bad_lines='skip'
                    )
                    cols_count = len(df_chunk.columns)
                    rows_count = len(df_chunk)
                    if cols_count > 1:
                        score = (cols_count, rows_count)
                        if score > best_score:
                            best_score = score
                            best_enc = enc
                            best_sep = sep
                            found = True
                            if sep == candidate_seps[0] and cols_count > 1 and rows_count >= 5:
                                break
                except Exception:
                    continue
            if found and best_sep == candidate_seps[0]:
                break

    # 3. Read the full file with detected parameters
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', pd.errors.ParserWarning)
        try:
            return pd.read_csv(
                BytesIO(raw_bytes), sep=best_sep, encoding=best_enc,
                dtype=str, on_bad_lines='skip'
            )
        except UnicodeDecodeError:
            pass

    # 4. Hard fallback: latin1 + replace — never raises on any byte sequence
    for sep in candidate_seps:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', pd.errors.ParserWarning)
                df = pd.read_csv(
                    BytesIO(raw_bytes), sep=sep, encoding='latin1',
                    encoding_errors='replace', dtype=str, on_bad_lines='skip'
                )
            if len(df.columns) > 1:
                return df
        except Exception:
            continue

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', pd.errors.ParserWarning)
        return pd.read_csv(
            BytesIO(raw_bytes), sep=',', encoding='latin1',
            encoding_errors='replace', dtype=str, on_bad_lines='skip'
        )


_ILLEGAL_XML_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')


def _repair_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix mojibake (double-encoded UTF-8) and known substitution sequences.

    Strategy (per column, vectorized):
      1. Apply _MOJIBAKE_MAP literal substitutions first — handles known
         sequences like 'â€"' -> '-' and 'â€™' -> "'" without any encoding
         round-trips.
      2. Attempt a vectorized latin-1 -> utf-8 heuristic decode using
         errors='ignore' so characters outside latin-1 (en-dashes U+2013,
         em-dashes U+2014, smart quotes U+2018/9, etc.) are silently
         preserved rather than converted to '?' (the previous bug with
         errors='replace').
      3. Strip illegal XML control characters.
      4. Per-row fallback for any column where vectorization fails.
    """

    def _fix_row(val: str) -> str:
        if not isinstance(val, str):
            return val
        # Step 1: literal map
        for bad, good in _MOJIBAKE_MAP.items():
            val = val.replace(bad, good)
        # Step 2: encoding heuristic
        for enc in ('cp1252', 'latin-1'):
            try:
                fixed = val.encode(enc).decode('utf-8')
                if fixed != val and '\ufffd' not in fixed:
                    val = fixed
                    break
            except (UnicodeDecodeError, UnicodeEncodeError):
                continue
        # Step 3: strip illegal XML control chars
        return _ILLEGAL_XML_RE.sub('', val)

    for col in df.select_dtypes(include='object').columns:
        s = df[col].astype(str)
        # Mojibake only exists in values containing non-ASCII characters, and
        # the XML-control-char strip only matters for values containing them.
        # One vectorized regex scan per column finds both, so the per-cell
        # Python repair runs only on the (usually tiny) subset of rows that
        # actually need it instead of every cell of every column.
        non_ascii = s.str.contains(r'[^\x00-\x7F]', regex=True, na=False)
        has_ctrl = s.str.contains(_ILLEGAL_XML_RE, na=False)
        if non_ascii.any():
            s.loc[non_ascii] = s.loc[non_ascii].map(_fix_row)
        ctrl_only = has_ctrl & ~non_ascii
        if ctrl_only.any():
            s.loc[ctrl_only] = s.loc[ctrl_only].str.replace(_ILLEGAL_XML_RE, '', regex=True)
        df[col] = s
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
    
    # 👇 FIX ADDED HERE: Drop any duplicate columns created by the rename step 👇
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    # dtype=str is already set at read time in _detect_and_read_csv; .astype(str) is
    # still applied here as a safety net for DataFrames produced by other paths
    for col in ['ACTIVE_STATUS_COUNTRY', 'CATEGORY_CODE', 'BRAND', 'TAX_CLASS', 'NAME', 'SELLER_NAME']:
        if col in df.columns and df[col].dtype != object:
            df[col] = df[col].astype(str)
    if 'MAIN_IMAGE' not in df.columns:
        df['MAIN_IMAGE'] = ''

    # Restore leading zeros in PARENTSKU from PRODUCT_SET_SID when they represent
    # the same integer but SID has more leading zeros (e.g. '7' -> '00007').
    # Fully vectorized — eliminates the df.apply(axis=1) row loop.
    if 'PARENTSKU' in df.columns and 'PRODUCT_SET_SID' in df.columns:
        psku = df['PARENTSKU'].fillna('').astype(str).str.strip()
        sid  = df['PRODUCT_SET_SID'].fillna('').astype(str).str.strip()

        # Treat explicit 'nan' strings as empty
        psku = psku.where(~psku.str.lower().isin({'nan', ''}), '')
        sid  = sid.where(~sid.str.lower().isin({'nan', ''}), '')

        # Extract leading digit group and suffix from each column
        p_extract = psku.str.extract(r'^(\d+)(.*)', expand=True)
        p_digits = p_extract[0]
        p_suffix = p_extract[1].fillna('')
        
        s_digits = sid.str.extract(r'^(\d+)', expand=False)

        # Eligible rows: both have leading digits, SID is longer (more zeros),
        # and they represent the same integer (lstrip '0' to compare numerically)
        both_have = p_digits.notna() & s_digits.notna() & psku.ne('') & sid.ne('')
        sid_longer = s_digits.str.len() > p_digits.str.len()
        same_int   = (
            p_digits.str.lstrip('0').fillna('') ==
            s_digits.str.lstrip('0').fillna('')
        )
        mask = both_have & sid_longer & same_int

        df['PARENTSKU'] = psku  # normalise to stripped string
        if mask.any():
            df.loc[mask, 'PARENTSKU'] = s_digits[mask] + p_suffix[mask]

    if 'MAIN_IMAGE' not in df.columns:
        df['MAIN_IMAGE'] = ''
    return df


def validate_input_schema(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    required = ['PRODUCT_SET_SID', 'NAME', 'BRAND', 'CATEGORY_CODE', 'ACTIVE_STATUS_COUNTRY']
    errors = [f"Missing: {f}" for f in required if f not in df.columns]
    return len(errors) == 0, errors


_COUNTRY_PREFIXES = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana",
                     "MA": "Morocco", "EG": "Egypt", "SN": "Senegal", "CI": "Ivory Coast"}


def _detect_countries_from_skus(df: pd.DataFrame) -> List[str]:
    """Infer which markets a file belongs to from SKU/SID prefixes.

    Only used when ACTIVE_STATUS_COUNTRY is absent. Prefix matching is
    deliberately anchored with a separator — a bare startswith("MA") also
    matches seller SKUs like "MAX 90", which is why the country column is
    preferred whenever it exists.
    """
    found = set()
    sku_cols = [c for c in df.columns if 'SKU' in str(c).upper() or 'SID' in str(c).upper()]
    for col in sku_cols:
        vals = df[col].dropna().astype(str).str.strip().str.upper()
        if vals.empty:
            continue
        for prefix, name in _COUNTRY_PREFIXES.items():
            hits = vals.str.match(rf"{prefix}[-_ ]")
            # A stray match is not a market. Require a real share of the file
            # before claiming the batch belongs somewhere else.
            if hits.mean() >= 0.5:
                found.add(name)
    return sorted(found)


def filter_by_country(df: pd.DataFrame, country_validator) -> Tuple[pd.DataFrame, List[str]]:
    if 'ACTIVE_STATUS_COUNTRY' not in df.columns:
        # Without the column every row used to pass through as whatever country
        # was selected: a Uganda file processed under Kenya, with no filtering,
        # no detection and no warning, because the SKU-prefix fallback below
        # sat after this return and could never run. Fall back to it properly
        # and report what the file looks like, so the caller can refuse.
        return df, _detect_countries_from_skus(df)
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
    prefix_map = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana", "MA": "Morocco", "EG": "Egypt", "SN": "Senegal", "CI": "Ivory Coast"}

    # The column is present by this point — the no-column case returned above
    # with _detect_countries_from_skus(). Prefer it over prefix scanning, which
    # produces false positives (seller SKUs like "MAX 90" match Morocco).
    detected_codes = set(df['ACTIVE_STATUS_COUNTRY'].dropna().unique())

    emoji_map = {"KE": "Kenya", "UG": "Uganda", "NG": "Nigeria", "GH": "Ghana", "MA": "Morocco", "EG": "Egypt", "SN": "Senegal", "CI": "Ivory Coast"}
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
            
    # Vectorized group forward/backward fill (orders of magnitude faster than lambda)
    df[meta_cols] = df.groupby('PRODUCT_SET_SID')[meta_cols].ffill()
    df[meta_cols] = df.groupby('PRODUCT_SET_SID')[meta_cols].bfill()
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
        price = pd.to_numeric(usd_price, errors="coerce")
        if pd.isna(price) or price <= 0:
            return ""
        cfg = COUNTRY_CURRENCY.get(country, {})
        rate = fetch_exchange_rate(country)
        local = float(price) * rate
        if pd.isna(local):
            return ""
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
_ZIP_FILE_CACHE = None
_ZIP_FILE_BYTES_ID = None

# Cap on how many decoded ZIP images are held in memory at once. Each entry is a
# base64 data URI — roughly 1.33x the original file — so an unbounded store grew
# without limit as a reviewer paged through a large ZIP, and never shrank. 300
# images covers several pages of history at a few hundred MB worst case.
_ZIP_IMAGE_CACHE_MAX = 300


def _bounded_store_set(store: dict, key: str, value: str) -> None:
    """Insert into a plain dict used as an LRU-ish cache, evicting oldest first.

    st.session_state stores a plain dict here (it must stay picklable and
    session-scoped), so eviction is done explicitly rather than via a subclass.
    """
    store[key] = value
    if len(store) > _ZIP_IMAGE_CACHE_MAX:
        for _stale in list(store.keys())[: len(store) - _ZIP_IMAGE_CACHE_MAX]:
            store.pop(_stale, None)

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
        global _ZIP_FILE_CACHE, _ZIP_FILE_BYTES_ID
        source_list = source_bytes if isinstance(source_bytes, list) else [source_bytes]
        for sb in source_list:
            if not sb:
                continue
            try:
                if _ZIP_FILE_CACHE is None or _ZIP_FILE_BYTES_ID != id(sb):
                    _ZIP_FILE_CACHE = zipfile.ZipFile(BytesIO(sb))
                    _ZIP_FILE_BYTES_ID = id(sb)
                if member in _ZIP_FILE_CACHE.namelist():
                    img_bytes = _ZIP_FILE_CACHE.read(member)
                    encoded = base64.b64encode(img_bytes).decode('utf-8')
                    mime = "image/jpeg"
                    if key.endswith(".png"): mime = "image/png"
                    elif key.endswith(".webp"): mime = "image/webp"
                    elif key.endswith(".gif"): mime = "image/gif"
                    data_uri = f"data:{mime};base64,{encoded}"
                    _bounded_store_set(store, key, data_uri)
                    return data_uri
            except KeyError:
                continue
            except Exception:
                continue
        return None
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
