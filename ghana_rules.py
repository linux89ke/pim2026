"""
ghana_rules.py - Ghana-specific QC validation rules
Follows the same conventions as nigeria_rules.py / morocco_rules.py.
"""

import os
import re
import logging
import pandas as pd
import streamlit as st
from typing import Dict

logger = logging.getLogger(__name__)


# ── Internal helpers (mirrors nigeria_rules.py) ───────────────────────────────

def _clean_category_code(code) -> str:
    try:
        if pd.isna(code): return ""
        s = str(code).strip()
        if '.' in s: s = s.split('.')[0]
        return s
    except: return str(code).strip()


def _safe_excel_read(filename: str, sheet_name, usecols=None) -> pd.DataFrame:
    if not os.path.exists(filename): return pd.DataFrame()
    try:
        df = pd.read_excel(filename, sheet_name=sheet_name, usecols=usecols, engine='openpyxl', dtype=str)
        return df.dropna(how='all')
    except Exception as e:
        logger.error(f"safe_excel_read: tab='{sheet_name}' file={filename}: {e}")
        return pd.DataFrame()


# ── Fallback keywords (used when Excel file is absent or sheet is empty) ─────
_DEFAULT_SMART_GLASSES_KEYWORDS = [
    "smart glasses",
    "smart glass",
    "camera glasses",
    "camera glass",
    "spy glasses",
    "spy glass",
    "video glasses",
    "recording glasses",
    "ar glasses",
    "smartglasses",
    "eyewear camera",
    "glasses camera",
    "glasses with camera",
    "glasses camera built",
]


@st.cache_data(ttl=3600)
def load_ghana_qc_rules() -> Dict:
    """
    Loads Ghana-specific QC rules from Ghana_QC_Rules.xlsx.
    Falls back to hardcoded defaults if the file is missing.

    Expected sheet layout
    ─────────────────────
    Sheet: "Smart Glasses"
      Column A  keyword   e.g. "smart glasses"
      Column B  notes     (optional — ignored by the check)
    """
    FILE_NAME = "Ghana_QC_Rules.xlsx"

    result: Dict = {
        "smart_glasses": {
            "keywords": set(_DEFAULT_SMART_GLASSES_KEYWORDS),
        },
    }

    if not os.path.exists(FILE_NAME):
        logger.warning("Ghana_QC_Rules.xlsx not found — falling back to default smart-glasses keywords.")
        return result

    # ── Smart Glasses sheet ───────────────────────────────────────────────────
    try:
        df = _safe_excel_read(FILE_NAME, sheet_name="Smart Glasses")
        if not df.empty:
            df.columns = [str(c).strip() for c in df.columns]
            kw_col = df.columns[0]
            loaded_keywords = (
                set(df[kw_col].dropna().astype(str).str.strip().str.lower())
                - {"", "nan", "keyword", "keywords"}
            )
            if loaded_keywords:
                result["smart_glasses"]["keywords"] = loaded_keywords
                logger.info(f"Ghana smart-glasses: loaded {len(loaded_keywords)} keywords from Excel.")
    except Exception as e:
        logger.warning(f"load_ghana_qc_rules smart_glasses: {e}")

    return result


def check_ghana_smart_glasses(data: pd.DataFrame, gh_rules: Dict) -> pd.DataFrame:
    """
    Flags any product whose NAME contains keywords indicating smart glasses
    with a built-in camera. ALL such products are prohibited in Ghana —
    there is no approved-seller whitelist.

    Parameters
    ----------
    data     : product DataFrame (must contain NAME column)
    gh_rules : output of load_ghana_qc_rules()

    Returns
    -------
    Subset of data that should be rejected, with Comment_Detail populated.
    """
    if "NAME" not in data.columns:
        return pd.DataFrame(columns=data.columns)

    keywords = gh_rules.get("smart_glasses", {}).get("keywords", set(_DEFAULT_SMART_GLASSES_KEYWORDS))
    if not keywords:
        return pd.DataFrame(columns=data.columns)

    pattern = re.compile(
        r"(?<!\w)(" +
        "|".join(re.escape(kw) for kw in sorted(keywords, key=len, reverse=True)) +
        r")(?!\w)",
        re.IGNORECASE,
    )

    mask = data["NAME"].astype(str).str.contains(pattern, na=False)
    if not mask.any():
        return pd.DataFrame(columns=data.columns)

    flagged = data[mask].copy()

    def _build_comment(name: str) -> str:
        m = pattern.search(str(name))
        keyword_found = m.group(0) if m else "smart glasses with camera"
        return (
            f"Prohibited in Ghana: smart glasses with camera detected "
            f"(keyword: '{keyword_found}')"
        )

    flagged["Comment_Detail"] = flagged["NAME"].apply(_build_comment)

    return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])
