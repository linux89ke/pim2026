"""brand_catalog_loader.py
Loads Master_Brand_Product_Catalog_V3.xlsx and extracts:
1. CATALOG_MODEL_ALIASES - dict[token_lower -> canonical_brand_lower]
   Enriches PRICE_CEILING_MODEL_ALIASES so the price-ceiling check
   catches evasion like airmax95, airjordan 4, gshock dw5600, fx991ex even
   when BRAND is Generic/Fashion/Unbranded.

2. CATALOG_EVASION_KEYWORDS - dict[brand_lower -> frozenset(keyword_lower)]
   Per-brand keyword sets for title keyword containment checks in fake product audit.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from typing import Dict, FrozenSet, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_CATALOG_PATH = os.path.join(
    os.path.expanduser("~"), "Downloads", "Master_Brand_Product_Catalog_V3.xlsx"
)

# Resolve sub-brands to canonical brands present in suspected_fake price sheets
_BRAND_NORMALISE: dict[str, str] = {
    "air jordan": "nike",
    "jordan": "nike",
}

_MANUAL_EVASION: dict[str, list[str]] = {
    "nike": [
        "airmax", "air max", "airmaz", "air-max",
        "airmax90", "airmax 90", "air max 90",
        "airmax95", "airmax 95", "air max 95",
        "airmax97", "airmax 97", "air max 97",
        "airmax270", "air max 270",
        "airmax720", "air max 720",
        "airmax2090", "air max 2090",
        "airforce", "air force", "air-force",
        "airforce1", "af1", "air force 1", "airforce 1",
        "airjordan", "air jordan", "air-jordan",
        "aj1", "aj4", "aj5", "aj11", "aj12",
        "nik", "nikee", "n!ke", "mike",
        "nike dunk", "dunk low", "dunk high", "sb dunk",
        "air", "jumpman",
    ],
    "adidas": [
        "addidas", "adida", "@didas", "adids",
        "yeezy", "yeezi", "yeezzy",
        "ultraboost", "ultra boost",
        "superstar", "stan smith",
    ],
    "converse": [
        "conver", "convers", "converce", "converses",
        "chuck taylor", "chuck 70", "all star", "all stars",
        "allstar", "allstars",
    ],
    "casio": [
        "g-shock", "gshock", "g shock", "g_shock",
        "gw5600", "dw5600", "dw-5600", "dw6900", "dw-6900",
        "ga2100", "ga-2100", "casioak", "casiaok",
        "ga110", "ga-110",
        "baby-g", "babyg", "baby g",
        "edifice", "pro trek", "protrek",
        "fx991", "fx-991", "fx991ex", "fx-991ex", "classwiz",
        "fx82ms", "fx-82ms",
        "fx9750", "fx-9750",
    ],
    "puma": [
        "rs-x", "rsx", "cali star",
    ],
    "new balance": [
        "newbalance", "nb990", "nb 990", "nb550", "nb 550",
    ],
}

_GENERIC_TOKENS = frozenset({
    "air", "max", "force", "one", "low", "high", "mid", "pro", "og",
    "classic", "slide", "campus", "forum", "samba", "dunk", "free",
    "run", "running", "sport", "sports", "star", "ultra", "boost",
    "fit", "lite", "light", "fast", "speed", "new", "retro",
    "men", "women", "unisex", "size", "black", "white", "red", "blue",
    "original", "authentic", "genuine", "brand",
    "watch", "clock", "digital", "analog", "solar",
    "calculator", "scientific",
})


def _clean_token(t: str) -> str:
    return re.sub(r"\s+", " ", t.strip().lower())


def _tokenise_model_name(name: str) -> list[str]:
    name = _clean_token(name)
    words = name.split()
    phrases = []
    for length in range(2, len(words) + 1):
        for start in range(len(words) - length + 1):
            phrases.append(" ".join(words[start : start + length]))
    for w in words:
        if w not in _GENERIC_TOKENS and not w.isdigit() and len(w) > 3:
            phrases.append(w)
    return phrases


@lru_cache(maxsize=1)
def load_catalog(path: Optional[str] = None) -> Tuple[Dict[str, str], Dict[str, FrozenSet[str]]]:
    """Load the Master Brand Catalog and return (model_aliases, evasion_keywords)."""
    catalog_path = path or _DEFAULT_CATALOG_PATH
    if not os.path.isfile(catalog_path):
        # Also check current working directory or relative path
        alt_path = os.path.join(os.getcwd(), "Master_Brand_Product_Catalog_V3.xlsx")
        if os.path.isfile(alt_path):
            catalog_path = alt_path
        else:
            logger.warning("Master Brand Catalog not found at %s", catalog_path)
            return {}, {}

    try:
        xl = pd.ExcelFile(catalog_path)
        master = xl.parse("Master Catalog", header=2)
        master.columns = ["ID", "Brand", "Product Line", "Category", "Sub-Category", "Model Name"]
        master = master.dropna(subset=["ID"])
        master = master[master["Brand"].astype(str).str.strip() != "Brand"]
    except Exception as exc:
        logger.warning("Failed to read Master Brand Catalog: %s", exc)
        return {}, {}

    model_aliases: dict[str, str] = {}
    evasion_keywords: dict[str, list[str]] = {}

    for _, row in master.iterrows():
        raw_brand = str(row.get("Brand", "")).strip()
        if not raw_brand or raw_brand.lower() == "nan":
            continue
        brand_lower = raw_brand.lower()
        canonical = _BRAND_NORMALISE.get(brand_lower, brand_lower)

        if canonical not in evasion_keywords:
            evasion_keywords[canonical] = []

        model_raw = str(row.get("Model Name", "")).strip()
        if model_raw and model_raw.lower() not in ("nan", ""):
            model_clean = _clean_token(model_raw)
            model_aliases[model_clean] = canonical
            evasion_keywords[canonical].append(model_clean)
            for phrase in _tokenise_model_name(model_raw):
                if phrase not in _GENERIC_TOKENS and phrase not in model_aliases:
                    model_aliases[phrase] = canonical
                    evasion_keywords[canonical].append(phrase)

        pl_raw = str(row.get("Product Line", "")).strip()
        if pl_raw and pl_raw.lower() not in ("nan", ""):
            pl_clean = _clean_token(pl_raw)
            if pl_clean not in model_aliases:
                model_aliases[pl_clean] = canonical
            evasion_keywords[canonical].append(pl_clean)

        sub_raw = str(row.get("Sub-Category", "")).strip()
        if sub_raw and sub_raw.lower() not in ("nan", ""):
            sub_clean = _clean_token(sub_raw)
            if len(sub_clean.split()) > 1 and sub_clean not in _GENERIC_TOKENS:
                if sub_clean not in model_aliases:
                    model_aliases[sub_clean] = canonical
                evasion_keywords[canonical].append(sub_clean)

    for brand, variants in _MANUAL_EVASION.items():
        canonical = _BRAND_NORMALISE.get(brand, brand)
        if canonical not in evasion_keywords:
            evasion_keywords[canonical] = []
        for v in variants:
            v_clean = _clean_token(v)
            if v_clean not in model_aliases:
                model_aliases[v_clean] = canonical
            evasion_keywords[canonical].append(v_clean)

    frozen_evasion = {b: frozenset(kws) for b, kws in evasion_keywords.items()}
    logger.info("Brand catalog loaded: %d brands, %d alias tokens.", len(frozen_evasion), len(model_aliases))
    return model_aliases, frozen_evasion


try:
    CATALOG_MODEL_ALIASES, CATALOG_EVASION_KEYWORDS = load_catalog()
except Exception:
    CATALOG_MODEL_ALIASES = {}
    CATALOG_EVASION_KEYWORDS = {}
