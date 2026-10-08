"""Extended audit validation rules for Targeted Audit and Word Report.

Includes:
1. audit_suspected_fake_products: checks brand price ceilings from suspected_fake.xlsx
2. audit_generic_brand_evasion: checks generic/fashion brand with genuine brand in title
3. audit_specs_inconsistency: checks RAM/Storage/OS mismatch and missing smartphone specs
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd
from constants import PRICE_CEILING_MODEL_ALIASES

# Load catalog evasion keywords from Master Brand Catalog at import time.
# Maps keyword_lower -> canonical_brand_title (e.g. "airmax" -> "Nike", "gshock" -> "Casio")
try:
    from brand_catalog_loader import CATALOG_EVASION_KEYWORDS
    _CATALOG_EVASION_BRANDS: dict[str, str] = {}
    for _brand, _kws in CATALOG_EVASION_KEYWORDS.items():
        _brand_title = _brand.title()
        for _kw in _kws:
            if _kw and len(_kw) >= 3:
                _CATALOG_EVASION_BRANDS[_kw] = _brand_title
except Exception:
    _CATALOG_EVASION_BRANDS = {}


# ── 1. Suspected Fake / Counterfeit Products ─────────────────────────────────

def audit_suspected_fake_products(
    data: pd.DataFrame,
    suspected_fake_df: pd.DataFrame,
    sneaker_category_codes: Optional[List[str]] = None,
) -> Dict[str, Dict[str, str]]:
    """Evaluates data against price ceilings from suspected_fake.xlsx.

    Returns dict mapping: ProductSetSid -> {'reason_type': ..., 'detail': ...}
    """
    if data.empty or suspected_fake_df is None or suspected_fake_df.empty:
        return {}

    req_cols = ["PRODUCT_SET_SID", "CATEGORY_CODE", "BRAND"]
    if not all(c in data.columns for c in req_cols):
        return {}

    try:
        ref_data = suspected_fake_df.copy()
        brand_cat_price = {}
        for brand in [
            c for c in ref_data.columns
            if c not in ["Unnamed: 0", "Brand", "Price"] and pd.notna(c)
        ]:
            try:
                pt = pd.to_numeric(ref_data[brand].iloc[0], errors="coerce")
                if pd.isna(pt) or pt <= 0:
                    continue
            except Exception:
                continue
            for cat in ref_data[brand].iloc[1:].dropna():
                cat_base = str(cat).strip().split(".")[0]
                if cat_base and cat_base.lower() != "nan":
                    brand_cat_price[(brand.strip().lower(), cat_base)] = pt

        if not brand_cat_price:
            return {}

        d = data.copy()
        
        # If price columns are present, calculate price_to_use
        if "GLOBAL_SALE_PRICE" in d.columns or "GLOBAL_PRICE" in d.columns:
            d["price_to_use"] = pd.to_numeric(
                d["GLOBAL_SALE_PRICE"].where(
                    d["GLOBAL_SALE_PRICE"].notna()
                    & (pd.to_numeric(d["GLOBAL_SALE_PRICE"], errors="coerce") > 0),
                    d.get("GLOBAL_PRICE", 0),
                ) if "GLOBAL_SALE_PRICE" in d.columns else d.get("GLOBAL_PRICE", 0),
                errors="coerce",
            ).fillna(0)
        elif "PRICE" in d.columns:
            d["price_to_use"] = pd.to_numeric(d["PRICE"], errors="coerce").fillna(0)
        else:
            return {}

        _sheet_brands = sorted({b for (b, _c) in brand_cat_price}, key=len, reverse=True)
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
        _name = d.get("NAME", pd.Series("", index=d.index)).astype(str).str.lower()
        _brand_lower = d.get("BRAND", pd.Series("", index=d.index)).fillna("").astype(str).str.strip().str.lower()

        _NO_DESCRIPTION_SCAN_BRANDS = {"beats"}
        _sneaker_cats = {str(c).strip() for c in (sneaker_category_codes or [])}
        _in_scope_cats = _sneaker_cats | {c for (_b, c) in brand_cat_price}
        _cat_clean = d["CATEGORY_CODE"].astype(str).str.strip()
        _in_scope = _cat_clean.isin(_in_scope_cats) if _in_scope_cats else pd.Series(False, index=d.index)

        _tag_re = re.compile(r"<[^>]+>")
        if _in_scope.any():
            _blob = (
                d.get("DESCRIPTION", pd.Series("", index=d.index)).astype(str)
                + " "
                + d.get("SHORT_DESCRIPTION", pd.Series("", index=d.index)).astype(str)
            ).where(_in_scope, "").str.replace(_tag_re, " ", regex=True).str.lower()
        else:
            _blob = pd.Series("", index=d.index)

        _no_blob = pd.Series(False, index=d.index)
        _claims = {}
        for b, rx in _brand_res.items():
            _blob_hit = _no_blob if b in _NO_DESCRIPTION_SCAN_BRANDS else _blob.str.contains(rx, na=False)
            _claims[b] = ((_brand_lower == b) | _name.str.contains(rx, na=False) | _blob_hit)

        for a, parent in _alias_terms.items():
            rx = _alias_res[a]
            _blob_hit = _no_blob if parent in _NO_DESCRIPTION_SCAN_BRANDS else _blob.str.contains(rx, na=False)
            _claims[parent] = _claims.get(parent, pd.Series(False, index=d.index)) | (
                (_brand_lower == a) | _name.str.contains(rx, na=False) | _blob_hit
            )

        cats = _cat_clean.values
        _flag = pd.Series(False, index=d.index)
        _detail = pd.Series("", index=d.index)

        for b, claimed in _claims.items():
            if not claimed.any():
                continue
            _ceil = pd.Series([brand_cat_price.get((b, c), -1) for c in cats], index=d.index)
            _under = claimed & (d["price_to_use"] <= _ceil) & (_ceil > 0)
            if not _under.any():
                continue

            _where = pd.Series("BRAND", index=d.index)
            _where = _where.mask(_brand_lower != b, "NAME")
            _where = _where.mask((_brand_lower != b) & ~_name.str.contains(_brand_res[b], na=False), "DESCRIPTION")

            _new = _under & ~_flag
            _detail.loc[_new] = (
                f"{b.title()} claimed in " + _where.loc[_new]
                + " but priced at " + d["price_to_use"].loc[_new].astype(str)
                + " (at or under the " + _ceil.loc[_new].astype(int).astype(str) + " minimum counterfeit ceiling)"
            )
            _flag = _flag | _under

        violations = {}
        for idx in d[_flag].index:
            sid = str(d.at[idx, "PRODUCT_SET_SID"]).strip()
            violations[sid] = {
                "reason_type": "Suspected Fake / Counterfeit",
                "detail": _detail.loc[idx],
                "is_overturned": False,
            }
        return violations
    except Exception:
        return {}


# ── 2. Generic Brand Evasion / Brand Hijacking ───────────────────────────────

def audit_generic_brand_evasion(
    data: pd.DataFrame,
    brands_list: List[str],
    code_to_path: Optional[Dict] = None,
) -> Dict[str, Dict[str, str]]:
    """Flags listings using pseudo-brands (Generic, Fashion, Unbranded) with a genuine brand in title."""
    if data.empty or not brands_list:
        return {}
    if not {"PRODUCT_SET_SID", "NAME", "BRAND"}.issubset(data.columns):
        return {}

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

    brand_lower = data["BRAND"].fillna("").astype(str).str.strip().str.lower()
    mask = brand_lower.isin(_PSEUDO_BRANDS)

    # Build a resolved category path series — prefer the CATEGORY column when it
    # is already a readable path (contains "/"), otherwise fall back to
    # CATEGORY_CODE looked up through code_to_path (passed as an optional kwarg).
    _code_to_path: dict = code_to_path or {}
    if "CATEGORY" in data.columns:
        _raw_cat = data["CATEGORY"].fillna("").astype(str).str.strip()
        _is_path = _raw_cat.str.contains("/", regex=False)
        cat_resolved = _raw_cat.where(_is_path, "")
    else:
        cat_resolved = pd.Series("", index=data.index)

    # Fill blanks using CATEGORY_CODE when a mapping is provided
    if "CATEGORY_CODE" in data.columns:
        _blank = cat_resolved.eq("")
        if _blank.any():
            cat_resolved = cat_resolved.copy()
            cat_resolved[_blank] = (
                data.loc[_blank, "CATEGORY_CODE"]
                .astype(str).str.strip()
                .map(_code_to_path)
                .fillna("")
            )

    cat_lower = cat_resolved.str.lower()

    # Accessories/cases are allowed to declare generic and name the device they fit
    is_accessory_cat = cat_lower.str.contains(
        r"\b(?:case|cases|cover|covers|sleeve|sleeves|pouch|pouches|screen.?protector)\b",
        regex=True, na=False
    )
    # Phone & tablet accessories are explicitly exempt —
    # generic brand is expected for compatible accessories listed under
    # "Phones & Tablets / Accessories / ..." or paths containing
    # "phone accessories" / "tablet accessories" / "cell phone accessories".
    is_phone_tablet_accessory = cat_lower.str.contains(
        r"phones?\s*&\s*tablets?\s*/\s*accessories?"
        r"|\bphone\s+accessories?\b"
        r"|\btablet\s+accessories?\b"
        r"|\bcell\s*phone\s+accessories?\b",
        regex=True, na=False
    )
    # Device categories (phones, laptops, tablets, computers) are NOT accessories —
    # Brand=Generic on an actual device is brand evasion, not a compatible-accessory listing.
    # These must NOT be overturned.
    is_device_cat = cat_lower.str.contains(
        r"\b(?:smartphones?|mobile\s*phones?|android\s*phones?|iphones?|ios\s*phones?|"
        r"cell\s*phones?|feature\s*phones?|laptops?|notebooks?|netbooks?|macbooks?|"
        r"ultrabooks?|chromebooks?|tablets?|ipads?|computers?|desktops?|pc)\b",
        regex=True, na=False
    )
    # Exclude device categories from flagging, but phone/tablet accessories are always exempt.
    mask = mask & ~is_device_cat & ~is_phone_tablet_accessory

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
        # Merge all catalog-sourced evasion aliases from Master_Brand_Product_Catalog_V3.xlsx.
        # Hardcoded entries above take precedence (they are not overwritten).
        **{k: v for k, v in _CATALOG_EVASION_BRANDS.items()
           if k not in {"air", "max", "force", "star", "pro", "one", "original",
                        "authentic", "genuine", "brand", "new", "classic", "sport", "sports",
                        "run", "running", "boost", "ultra", "low", "high", "mid"}},
    }

    violations = {}

    # Check for direct evasion where the seller set BRAND to an evasion alias
    for idx, row in data.iterrows():
        b_raw = str(row.get("BRAND", "")).strip().lower()
        if b_raw in _EVASION_BRANDS:
            sid = str(row.get("PRODUCT_SET_SID", "")).strip()
            declared_b = str(row.get("BRAND", "")).strip()
            parent = _EVASION_BRANDS[b_raw]
            violations[sid] = {
                "reason_type": "Generic Brand Evasion / Brand Hijacking",
                "detail": f"Brand evasion: Brand declared as '{declared_b}' which is a protected model line belonging to '{parent}'.",
                "is_overturned": False,
            }

    gen = data[mask].copy()
    if gen.empty and not violations:
        return violations

    brand_trie = {}
    for b in brands_list:
        if not b:
            continue
        bc = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", str(b).lower())).strip()
        if not bc or bc in _PSEUDO_BRANDS or (len(bc) < 3 and bc not in _ALLOWED_SHORT_BRANDS):
            continue
        first_word = bc.split()[0]
        if first_word not in brand_trie:
            brand_trie[first_word] = []
        brand_trie[first_word].append((bc, b.title() if len(b) > 2 else b.upper()))

    # Also index distinctive model lines and aliases from catalog (Air Max, Air Jordan, All Stars, G-Shock, fx-82ms, etc.)
    try:
        from brand_catalog_loader import _GENERIC_TOKENS
    except ImportError:
        _GENERIC_TOKENS = frozenset()

    for alias, parent in PRICE_CEILING_MODEL_ALIASES.items():
        if not alias or len(alias) < 3 or alias in _GENERIC_TOKENS:
            continue
        if " " not in alias and len(alias) < 4:
            continue
        alias_clean = re.sub(r"\s+", " ", re.sub(r"['\.\-]", " ", alias.lower())).strip()
        fw = alias_clean.split()[0]
        if fw not in brand_trie:
            brand_trie[fw] = []
        brand_trie[fw].append((alias_clean, f"{parent.title()} ({alias.title()})"))
        comp = alias_clean.replace(" ", "")
        if comp != alias_clean and len(comp) >= 4:
            if comp not in brand_trie:
                brand_trie[comp] = []
            brand_trie[comp].append((comp, f"{parent.title()} ({alias.title()})"))

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

    if not gen.empty:
        for idx, row in gen.iterrows():
            sid = str(row.get("PRODUCT_SET_SID", "")).strip()
            if sid in violations:
                continue
            name_val = str(row.get("NAME", "")).strip()
            detected = detect(name_val)
            if detected:
                declared_b = str(row.get("BRAND", "")).strip().title()
                violations[sid] = {
                    "reason_type": "Generic Brand Evasion / Brand Hijacking",
                    "detail": f"Generic brand listing claims protected brand in title (Brand set to '{declared_b}' but title claims '{detected}').",
                    "is_overturned": False,
                }
    return violations



# ── 3. Smartphone Name & Specs Inconsistency ─────────────────────────────────

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
_SPEC_RAM_RE2 = re.compile(r"\bram\s*[:\-]\s*(\d+)\s*gb\b", re.IGNORECASE)
_SPEC_STORAGE_GB_RE1 = re.compile(r"(\d+)\s*gb\s*(?:rom|storage|internal(?:\s+storage)?|ssd|hdd|nvme|emmc)\b", re.IGNORECASE)
_SPEC_STORAGE_TB_RE1 = re.compile(r"(\d+)\s*tb\s*(?:rom|storage|internal(?:\s+storage)?|memory|ssd|hdd|nvme|emmc)\b", re.IGNORECASE)
_SPEC_STORAGE_RE2 = re.compile(r"\b(?:rom|storage|internal(?:\s+storage)?|ssd|hdd|nvme|emmc)\s*[:\-]\s*(\d+)\s*gb\b", re.IGNORECASE)
_SPEC_MEMORY_RE = re.compile(
    r"(?:(\d+)\s*gb\s*memory\b|\bmemory\s*[:\-]\s*(\d+)\s*gb\b)", re.IGNORECASE
)
_SPEC_COMBO_RE = re.compile(r"\b(\d+)\s*(?:gb)?\s*[/+]\s*(\d+)\s*gb\b", re.IGNORECASE)
# Virtual/extended RAM notation: "8GB RAM(4+4)GB" — physical + virtual/extended.
# Group 1 = physical, Group 2 = extended. Both are added to name_ram so the
# description's physical value (e.g. 4GB) is not flagged against the stated total.
_SPEC_VIRTUAL_RAM_RE = re.compile(
    r"(?:\d+\s*gb\s*ram|ram\s*[:\-]?\s*\d+\s*gb)\s*[\(\[\{]?\s*(\d+)\s*\+\s*(\d+)\s*[\)\]\}]?\s*gb",
    re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(r"<[^>]+>")

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
    ram = {int(m.group(1)) for m in _SPEC_RAM_RE1.finditer(text)}
    ram |= {int(m.group(1)) for m in _SPEC_RAM_RE2.finditer(text)}
    storage = {int(m.group(1)) for m in _SPEC_STORAGE_GB_RE1.finditer(text)}
    storage |= {int(m.group(1)) * 1024 for m in _SPEC_STORAGE_TB_RE1.finditer(text)}
    storage |= {int(m.group(1)) for m in _SPEC_STORAGE_RE2.finditer(text)}
    for m in _SPEC_MEMORY_RE.finditer(text):
        _v = int(m.group(1) or m.group(2))
        (ram if _v <= max_memory_as_ram else storage).add(_v)
    if allow_combo:
        for m in _SPEC_COMBO_RE.finditer(text):
            _a, _b = int(m.group(1)), int(m.group(2))
            if _a > _b:
                _a, _b = _b, _a
            ram.add(_a)
            storage.add(_b)
        # Virtual/extended RAM: "8GB RAM(4+4)GB" — add physical & extended parts
        # so description's physical RAM value matches.
        for m in _SPEC_VIRTUAL_RAM_RE.finditer(text):
            phys, ext = int(m.group(1)), int(m.group(2))
            if phys <= max_ram:
                ram.add(phys)
            if ext <= max_ram:
                ram.add(ext)

    impossible_ram = {v for v in ram if v > max_ram}
    if impossible_ram:
        ram -= impossible_ram
        storage |= {v for v in impossible_ram if v >= 16}

    return ram, storage


def audit_specs_inconsistency(
    data: pd.DataFrame,
    spec_category_codes: Optional[List[str]] = None,
    smartphone_category_codes: Optional[List[str]] = None,
) -> Dict[str, Dict[str, str]]:
    """Detects RAM/Storage/OS contradictions between title and description, and missing specs."""
    # Omitted from targeted audit report per user request
    return {}

    violations = {}
    spec_codes = {str(c).strip() for c in (spec_category_codes or [])}
    phone_codes = {str(c).strip() for c in (smartphone_category_codes or [])}

    # 1. Missing Storage/Memory spec on smartphones
    if phone_codes:
        phone_mask = data["CATEGORY_CODE"].astype(str).str.strip().isin(phone_codes)
        pat = re.compile(r"\b\d+\s*(?:gb|tb)\b", re.IGNORECASE)
        for idx in data[phone_mask].index:
            sid = str(data.at[idx, "PRODUCT_SET_SID"]).strip()
            name_val = str(data.at[idx, "NAME"] or "").strip()
            if not pat.search(name_val):
                violations[sid] = {
                    "reason_type": "AI Missed Storage Spec - Missing RAM/Storage in Title",
                    "detail": "Smartphone title missing storage/memory capacity (e.g. 64GB, 128GB).",
                }

    # 2. Specs Inconsistency: Title vs Description
    text_cols = [c for c in ("DESCRIPTION", "SHORT_DESCRIPTION") if c in data.columns]
    if not text_cols:
        return violations

    in_scope = pd.Series(False, index=data.index)
    if spec_codes:
        in_scope |= data["CATEGORY_CODE"].astype(str).str.strip().isin(spec_codes)
    if "CATEGORY" in data.columns:
        in_scope |= data["CATEGORY"].astype(str).str.contains(_SPEC_CATEGORY_KEYWORDS_RE, na=False)

    target = data[in_scope].copy()
    if target.empty:
        return violations

    name_lower = target["NAME"].astype(str).str.lower()
    target = target[name_lower.str.contains(_SPEC_ANY_RE, na=False)]
    if target.empty:
        return violations

    col_text = {
        c: target[c].astype(str).str.replace(_HTML_TAG_RE, " ", regex=True).str.lower()
        for c in text_cols
    }

    def _fmt(vals: set) -> str:
        return "/".join(f"{v}GB" for v in sorted(vals))

    for idx in target.index:
        sid = str(target.at[idx, "PRODUCT_SET_SID"]).strip()
        cat_str = str(target.at[idx, "CATEGORY"]) if "CATEGORY" in target.columns else ""
        name_str = str(target.at[idx, "NAME"]) if "NAME" in target.columns else ""
        combined_dev_text = f"{cat_str} {name_str}".lower()

        is_phone_tablet = bool(_PHONE_TABLET_PAT.search(combined_dev_text)) and not bool(_LAPTOP_COMP_PAT.search(combined_dev_text))
        row_max_ram = 24 if is_phone_tablet else 64
        row_max_memory_as_ram = 24 if is_phone_tablet else 32

        name_ram, name_storage = _extract_ram_storage(
            name_str.lower(),
            allow_combo=True,
            max_ram=row_max_ram,
            max_memory_as_ram=row_max_memory_as_ram,
        )
        name_os = _extract_os(name_str.lower())
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

            _MIN_STORAGE_GB = 16
            _f_storage_plausible = {v for v in f_storage if v >= _MIN_STORAGE_GB}
            _demoted = f_storage - _f_storage_plausible
            if _demoted:
                if not f_ram:
                    f_ram = f_ram | {v for v in _demoted if v <= row_max_ram}
                f_storage = _f_storage_plausible

            if name_ram and f_ram and not (name_ram & f_ram):
                mismatches.append(f"RAM: title says {_fmt(name_ram)}, {c} says {_fmt(f_ram)}")
            if name_storage and f_storage and not (name_storage & f_storage):
                mismatches.append(f"Storage: title says {_fmt(name_storage)}, {c} says {_fmt(f_storage)}")
            if name_os and f_os and not (name_os & f_os):
                mismatches.append(f"OS: title says {'/'.join(sorted(name_os))}, {c} says {'/'.join(sorted(f_os))}")

        if mismatches:
            violations[sid] = {
                "reason_type": "AI Missed Specs Inconsistency - Title Contradicts Description",
                "detail": "Specs inconsistency — " + " | ".join(mismatches),
            }

    return violations
