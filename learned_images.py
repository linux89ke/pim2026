"""Learned image fingerprint and URL matching module for PIM validation.

Provides:
- Ingestion and caching of blocked image URLs and phashes from Image_learn.xlsx
- High-performance exact URL matching (0ms in-memory lookup)
- Perceptual hash matching for downloaded/pre-fetched images
- Helper functions to check a DataFrame and to annotate reports with learned image matches
"""

import os
import json
import logging
import re
import urllib.request
from io import BytesIO
from pathlib import Path
from PIL import Image
import pandas as pd
import streamlit as st
from constants import PHASH_MATCH_MAX_DISTANCE

logger = logging.getLogger(__name__)

_MODULE_DIR = Path(__file__).resolve().parent
_BLOCKED_IMG_EXCEL = str(_MODULE_DIR / "Image_learn.xlsx")
_BLOCKED_IMG_CACHE = str(_MODULE_DIR / "Image_learn_cache.json")

# pHash is a 64-bit signature. A small Hamming distance catches modest image
# changes while keeping automatic rejections conservative. Tune against a
# labeled match/non-match set before increasing this value.

_BLOCKED_FLAG_MAP = {
    "poor images":                  "Poor images",
    "poor image":                   "Poor images",
    "image blurry":                 "Image Blurry",
    "image stretched":              "Image Stretched",
    "image mismatch":               "Image Mismatch",
    "image infringing":             "Image Infringing",
    "image too many things":        "Image Too Many things displayed",
    "image too many things displayed": "Image Too Many things displayed",
    "restricted brands":            "Restricted brands",
    "restricted brand":             "Restricted brands",
    "suspected counterfeit":         "Suspected Fake product",
    "counterfeit product":           "Suspected Fake product",
}


def _blocked_excel_mtime() -> float:
    """Return the mtime of Image_learn.xlsx, or 0.0 if it doesn't exist."""
    try:
        return os.path.getmtime(_BLOCKED_IMG_EXCEL)
    except OSError:
        return 0.0


def _compute_phash(img_bytes: bytes) -> str:
    """Compute perceptual hash for an image."""
    try:
        import imagehash
        img = Image.open(BytesIO(img_bytes))
        if img.mode == "P":
            img = img.convert("RGBA")
        img = img.convert("RGB")
        return str(imagehash.phash(img))
    except Exception:
        return ""


@st.cache_resource(show_spinner=False)
def load_blocked_image_fingerprints(_mtime: float = 0.0) -> dict:
    """Return {phash_str or url_str: {"flag": ..., "brand": ..., "url": ...}}.

    Reads Image_learn.xlsx (columns: IMAGE_URL, FLAG, BRAND_NAME optional).
    Downloads any URL not yet in the JSON cache, computes its phash, and
    saves the updated cache. Already-cached URLs are not re-downloaded.

    _mtime ensures Streamlit cache invalidation when Image_learn.xlsx changes.
    """
    if not os.path.exists(_BLOCKED_IMG_EXCEL):
        return {}

    try:
        xl = pd.read_excel(_BLOCKED_IMG_EXCEL, dtype=str).fillna("")
    except Exception as _e:
        logger.warning("Image_learn.xlsx could not be read: %s", _e)
        return {}

    # Normalise column names — tolerate minor spelling differences.
    xl.columns = [c.strip().upper().replace(" ", "_") for c in xl.columns]
    url_col   = next((c for c in xl.columns if "URL" in c or "IMAGE" in c), None)
    flag_col  = next((c for c in xl.columns if "FLAG" in c), None)
    brand_col = next((c for c in xl.columns if "BRAND" in c), None)
    if url_col is None or flag_col is None:
        logger.warning("Image_learn.xlsx must have IMAGE_URL and FLAG columns.")
        return {}

    _cache: dict = {}
    _cache_stale = True
    if os.path.exists(_BLOCKED_IMG_CACHE):
        try:
            with open(_BLOCKED_IMG_CACHE, "r", encoding="utf-8") as _f:
                _loaded = json.load(_f)
            _cached_mtime = _loaded.pop("_excel_mtime", 0.0)
            if _mtime and _cached_mtime >= _mtime:
                _cache = _loaded
                _cache_stale = False
            else:
                logger.info(
                    "Image_learn.xlsx changed (%.0f → %.0f); rebuilding phash cache.",
                    _cached_mtime, _mtime,
                )
        except Exception:
            _cache = {}

    blocked: dict = {}
    _cache_dirty = _cache_stale

    for _, row in xl.iterrows():
        url      = str(row[url_col]).strip()
        raw_flag = str(row[flag_col]).strip()
        brand    = str(row[brand_col]).strip() if brand_col else ""
        if not url or url.lower() == "nan":
            continue
        flag = _BLOCKED_FLAG_MAP.get(raw_flag.lower(), raw_flag) or "Poor images"

        if url in _cache:
            ph = _cache[url].get("phash", "")
        else:
            _SKIP_DOMAINS = ("localhost", "127.0.0.1", "0.0.0.0")
            _is_skip = any(d in url.lower() for d in _SKIP_DOMAINS)
            ph = ""
            if _is_skip:
                _cache[url] = {"phash": "", "flag": flag, "brand": brand, "_skipped": True}
                _cache_dirty = True
            else:
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=15) as resp:
                        raw = resp.read()
                    ph = _compute_phash(raw)
                except Exception as _dl_e:
                    logger.warning("Blocked-image fetch failed for %s: %s", url, _dl_e)
                _cache[url] = {"phash": ph, "flag": flag, "brand": brand}
                _cache_dirty = True

        if ph:
            blocked[ph] = {"flag": flag, "brand": brand, "url": url}
        if url:
            blocked[url] = {"flag": flag, "brand": brand, "url": url, "phash": ph}

    if _cache_dirty:
        try:
            _to_save = dict(_cache)
            _to_save["_excel_mtime"] = _mtime
            with open(_BLOCKED_IMG_CACHE, "w", encoding="utf-8") as _f:
                json.dump(_to_save, _f, indent=2)
        except Exception as _w_e:
            logger.warning("Could not save Image_learn_cache.json: %s", _w_e)

    return blocked


def get_blocked_image_matches(
    data: pd.DataFrame,
    blocked_map: dict,
    hash_by_url: dict = None,
) -> dict:
    """Find all products in `data` matching a blocked image URL or phash.

    Returns:
        dict: {PRODUCT_SET_SID (str): {"flag": str, "brand": str, "url": str, "detail": str}}
    """
    if not blocked_map or data is None or data.empty:
        return {}

    img_col = next(
        (c for c in ["MAIN_IMAGE", "image1", "Image", "IMAGE_URL", "IMAGE", "Main Image", "Main_Image"] if c in data.columns),
        None,
    )
    sid_col = next(
        (c for c in ["PRODUCT_SET_SID", "ProductSetSid", "SID", "cod_productset_sid"] if c in data.columns),
        None,
    )
    if not img_col or not sid_col:
        return {}

    if hash_by_url is None:
        try:
            hash_by_url = st.session_state.get("_image_phash_by_url", {})
        except Exception:
            hash_by_url = {}

    # Build the hash-only catalog once per call. `blocked_map` also contains
    # exact URL keys, which must not be treated as hash signatures.
    hash_catalog = []
    for key, entry in blocked_map.items():
        if not isinstance(entry, dict) or len(key) != 16:
            continue
        try:
            hash_catalog.append((int(key, 16), entry))
        except (TypeError, ValueError):
            continue

    # Repeated image URLs/hashes are common in a product batch. Memoize their
    # nearest catalog result so each distinct upload hash is compared once.
    nearest_cache = {}
    matches = {}
    for _, row in data.iterrows():
        sid = str(row[sid_col]).strip()
        url = str(row[img_col]).strip()
        if not url or url.lower() == "nan" or not sid:
            continue

        # 1. Exact URL match
        entry = blocked_map.get(url)
        matched_by = "url" if entry else ""
        distance = 0 if entry else None

        # 2. Exact pHash match
        if not entry and hash_by_url:
            ph = hash_by_url.get(url, "")
            if ph:
                entry = blocked_map.get(ph)
                if entry:
                    matched_by = "phash"
                    distance = 0

                # 3. Near pHash match. Cache both hits and misses per hash.
                if not entry:
                    try:
                        query_hash = int(str(ph), 16)
                    except (TypeError, ValueError):
                        query_hash = None
                    if query_hash is not None:
                        if ph not in nearest_cache:
                            best_distance = PHASH_MATCH_MAX_DISTANCE + 1
                            best_entry = None
                            for catalog_hash, catalog_entry in hash_catalog:
                                candidate_distance = (query_hash ^ catalog_hash).bit_count()
                                if candidate_distance < best_distance:
                                    best_distance = candidate_distance
                                    best_entry = catalog_entry
                                    if best_distance == 0:
                                        break
                            nearest_cache[ph] = (
                                (best_entry, best_distance)
                                if best_entry is not None and best_distance <= PHASH_MATCH_MAX_DISTANCE
                                else None
                            )
                        nearest = nearest_cache[ph]
                        if nearest:
                            entry, distance = nearest
                            matched_by = "near-phash"

        if entry:
            flag = entry.get("flag", "Poor images")
            brand = entry.get("brand", "")
            if flag == "Restricted brands" and brand:
                detail = f"Image matched known restricted brand image ({brand})."
            else:
                detail = f"Image matched known blocked image."
            if matched_by == "near-phash":
                detail += f" Near image match (pHash distance {distance}/64)."
            if matched_by == "url":
                confidence = 0.99
            elif matched_by == "phash":
                confidence = 0.96
            else:
                # Keep near-match confidence separate from exact-match
                # confidence. Distances 5–6 are review evidence, not a 90%+
                # automatic rejection signal.
                _near_distance = int(distance or PHASH_MATCH_MAX_DISTANCE)
                confidence = max(0.55, 0.95 - (_near_distance * 0.04))
            matches[sid] = {
                "flag": flag,
                "brand": brand,
                "url": url,
                # The matched rule URL is different from the product URL for
                # pHash and near-pHash matches. Keep it so review surfaces can
                # remove the exact learned record instead of guessing from the
                # product image URL.
                "matched_rule_url": str(entry.get("url", "") or "").strip(),
                "matched_rule_phash": str(entry.get("phash", "") or "").strip(),
                "phash": str(hash_by_url.get(url, "") or "").strip(),
                "detail": detail,
                "match_method": matched_by,
                "phash_distance": distance,
                "confidence": round(confidence, 2),
                # Exact URL/pHash matches are automatic rejections. Near
                # matches are only automatic through distance 4; distances
                # 5–6 remain visible review commentary and never change the
                # product status by themselves.
                "decision": (
                    "reject"
                    if matched_by in {"url", "phash"}
                    or (matched_by == "near-phash" and int(distance or 99) <= 4)
                    else "review"
                ),
            }

    return matches


def _learned_brands_agree(declared: str, learned: str) -> bool:
    """Return True when the listing brand is compatible with the image brand."""
    declared = str(declared or "").strip().casefold()
    learned = str(learned or "").strip().casefold()
    if not learned or learned in {"nan", "none", "unknown", "no brand", "n/a"}:
        return True
    if not declared or declared in {"nan", "none", "unknown", "no brand", "n/a"}:
        return False
    if declared == learned:
        return True
    # Treat "NIVEA" and "NIVEA BABY" as compatible while rejecting unrelated
    # declarations such as Generic, Lattafa, or Sanford.
    return bool(
        re.search(r"\b" + re.escape(learned) + r"\b", declared)
        or re.search(r"\b" + re.escape(declared) + r"\b", learned)
    )


def check_blocked_image_fingerprints(
    data: pd.DataFrame,
    blocked_map: dict,
    _image_cache: dict = None,
    **kwargs,
) -> pd.DataFrame:
    """Validator function returning DataFrame of flagged products."""
    if not blocked_map or data is None or data.empty:
        return pd.DataFrame(columns=data.columns if data is not None else [])

    img_col = next(
        (c for c in ["MAIN_IMAGE", "image1", "Image", "IMAGE_URL", "IMAGE", "Main Image", "Main_Image"] if c in data.columns),
        None,
    )
    sid_col = next(
        (c for c in ["PRODUCT_SET_SID", "ProductSetSid", "SID", "cod_productset_sid"] if c in data.columns),
        None,
    )
    if not img_col or not sid_col:
        return pd.DataFrame(columns=data.columns)

    hash_by_url = {}
    try:
        hash_by_url = st.session_state.get("_image_phash_by_url", {})
    except Exception:
        pass

    matches = get_blocked_image_matches(data, blocked_map, hash_by_url)
    if not matches:
        return pd.DataFrame(columns=data.columns)

    country_code = str(kwargs.get("country_code", "")).strip().upper()
    code_to_path = kwargs.get("code_to_path", {}) or {}
    category_text = data.get("CATEGORY", pd.Series("", index=data.index)).fillna("").astype(str).str.strip()
    category_code = data.get("CATEGORY_CODE", pd.Series("", index=data.index)).fillna("").astype(str).str.strip()
    mapped_category = category_code.map(code_to_path).fillna("").astype(str).str.strip()
    category_path = category_text.where(category_text.str.len().gt(0), mapped_category).str.casefold()
    books_sids = set(data.loc[
        category_path.str.startswith("books, movies and music")
        | category_path.str.contains(r"(?:^|[/,>])\s*books?\b", regex=True, na=False),
        sid_col,
    ].astype(str).str.strip())
    declared_brand_by_sid = {}
    if "BRAND" in data.columns:
        declared_brand_by_sid = dict(zip(
            data[sid_col].astype(str).str.strip(),
            data["BRAND"].fillna("").astype(str).str.strip(),
        ))

    flagged_rows = []
    for _, row in data.iterrows():
        sid = str(row[sid_col]).strip()
        if sid in matches and matches[sid].get("decision", "reject") == "reject":
            m = matches[sid]
            frow = row.copy()
            learned_flag = str(m.get("flag", "Poor images")).strip()
            if learned_flag == "Restricted brands" and sid in books_sids:
                continue
            # Uganda has no restricted-brand rejection catalogue. Its learned
            # restricted-image records are used only as evidence that the
            # declared brand should agree with the brand attached to the image.
            # A correct declaration passes; a mismatch becomes the normal
            # Brand Image Mismatch validation so it is reviewable and clearly
            # explains the wrong brand.
            if country_code == "UG" and learned_flag == "Restricted brands":
                learned_brand = str(m.get("brand", "")).strip()
                declared_brand = declared_brand_by_sid.get(sid, str(row.get("BRAND", "")).strip())
                if _learned_brands_agree(declared_brand, learned_brand):
                    continue
                frow["Comment_Detail"] = (
                    f"Image shows '{learned_brand}' but listing brand is "
                    f"'{declared_brand}' (learned restricted-image brand check)."
                )
                learned_flag = "Brand Image Mismatch"
            else:
                frow["Comment_Detail"] = f"{m['detail']} (image fingerprint match)"
            frow["_blocked_flag"] = learned_flag
            frow["_blocked_source"] = m.get("source", "excel")
            frow["_blocked_rule_url"] = m.get("matched_rule_url", "")
            frow["_blocked_rule_phash"] = m.get("matched_rule_phash", "")
            flagged_rows.append(frow)

    if not flagged_rows:
        return pd.DataFrame(columns=data.columns)

    flagged = pd.DataFrame(flagged_rows)
    return flagged.drop_duplicates(subset=[sid_col])
