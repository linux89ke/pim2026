import sys
import os
import re
import hashlib
import traceback
import logging
from io import BytesIO

# ------------------------------------------------------------------
# PATH FIX — must be first, before any local imports.
# On Streamlit Cloud the CWD is not always the repo root, so all
# relative file loads (xlsx, txt) and local module imports fail.
# ------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

import pandas as pd
import streamlit as st

# ------------------------------------------------------------------
# LOCAL IMPORTS  (all modules live in ROOT)
# ------------------------------------------------------------------
try:
    from translations import LANGUAGES, get_translation
    _TRANSLATIONS_OK = True
except ImportError:
    _TRANSLATIONS_OK = False
    def get_translation(lang, key): return key

try:
    from postqc import (
        detect_file_type,
        normalize_post_qc,
        run_checks as run_post_qc_checks,
        render_post_qc_section,
        load_category_map,
    )
    _POSTQC_OK = True
except Exception as _postqc_err:
    _POSTQC_OK = False
    _postqc_err_msg = str(_postqc_err)

try:
    import _preqc_registry  # noqa: F401 — just ensures it's importable
except ImportError:
    pass

try:
    from jumia_scraper import enrich_post_qc_df
    _SCRAPER_OK = True
except ImportError:
    _SCRAPER_OK = False

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------------
try:
    st.set_page_config(
        page_title="Post-QC Validation",
        page_icon="🔍",
        layout=st.session_state.get("layout_mode", "wide"),
    )
except Exception:
    pass

# ------------------------------------------------------------------
# THEME
# ------------------------------------------------------------------
ORANGE      = "#F68B1E"
ORANGE2     = "#FF9933"
RED         = "#E73C17"
DARK        = "#313133"
MED         = "#5A5A5C"
LIGHT       = "#F5F5F5"
BORDER      = "#E0E0E0"
GREEN       = "#4CAF50"

st.markdown(f"""
<style>
.stButton > button {{ border-radius: 4px; font-weight: 600; }}
.stButton > button[kind="primary"] {{
    background-color: {ORANGE} !important;
    border: none !important; color: white !important;
}}
.stButton > button[kind="primary"]:hover {{
    background-color: {ORANGE2} !important;
}}
div[data-testid="stMetric"] {{
    background: {LIGHT}; border-radius: 0 0 8px 8px;
    padding: 12px 16px 16px; text-align: center;
}}
div[data-testid="stMetricValue"] {{
    color: {DARK}; font-weight: 700; font-size: 26px !important;
}}
div[data-testid="stMetricLabel"] {{
    color: {MED}; font-size: 11px; text-transform: uppercase;
}}
div[data-testid="stExpander"] {{
    border: 1px solid {BORDER}; border-radius: 8px;
}}
div[data-testid="stExpander"] summary {{
    background-color: {LIGHT}; padding: 12px; border-radius: 8px 8px 0 0;
}}
h1, h2, h3 {{ color: {DARK} !important; }}
@media (prefers-color-scheme: dark) {{
    div[data-testid="stMetricValue"] {{ color: #F5F5F5 !important; }}
    div[data-testid="stMetric"]      {{ background: #2a2a2e !important; }}
    h1, h2, h3                       {{ color: #F5F5F5 !important; }}
}}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------
# SESSION STATE DEFAULTS
# ------------------------------------------------------------------
_SS_DEFAULTS = {
    "ui_lang":          "en",
    "selected_country": "Kenya",
    "layout_mode":      "wide",
    "pq_country":       "Kenya",
    "pq_summary":       pd.DataFrame(),
    "pq_results":       {},
    "pq_data":          pd.DataFrame(),
    "pq_last_sig":      None,
    "pq_exports_cache": {},
    "pq_cached_files":  [],
    "scraper_enabled":  False,
}
for _k, _v in _SS_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------
COUNTRY_CODES = {
    "Kenya": "KE", "Uganda": "UG", "Nigeria": "NG",
    "Ghana": "GH", "Morocco": "MA",
}
COUNTRIES = list(COUNTRY_CODES.keys())


def _t(key):
    return get_translation(st.session_state.ui_lang, key)


def _code(name: str) -> str:
    return COUNTRY_CODES.get(name, "KE")


def _load_support_files() -> dict:
    """
    Re-use support files cached by the main page when available.
    Falls back to loading them fresh from disk if not.
    """
    if "support_files" in st.session_state:
        return st.session_state.support_files

    # Try to call load_all_support_files from the already-loaded main module
    mod = sys.modules.get("streamlit_app") or sys.modules.get("__main__")
    if mod and hasattr(mod, "load_all_support_files"):
        try:
            sf = mod.load_all_support_files()
            st.session_state.support_files = sf
            return sf
        except Exception:
            pass

    # Minimal fallback — only category_map (enough for post-QC normalisation)
    cat_map = load_category_map() if _POSTQC_OK else {}
    return {"category_map": cat_map}


def _reset_results():
    st.session_state.pq_summary       = pd.DataFrame()
    st.session_state.pq_results       = {}
    st.session_state.pq_data          = pd.DataFrame()
    st.session_state.pq_last_sig      = None
    st.session_state.pq_exports_cache = {}


# ------------------------------------------------------------------
# SIDEBAR
# ------------------------------------------------------------------
with st.sidebar:
    st.header("🔍 Post-QC Settings")

    pq_country = st.selectbox(
        "Country",
        COUNTRIES,
        index=COUNTRIES.index(st.session_state.pq_country),
        key="pq_country_select",
    )
    if pq_country != st.session_state.pq_country:
        st.session_state.pq_country = pq_country
        _reset_results()

    country_code = _code(pq_country)

    st.markdown("---")

    if _SCRAPER_OK:
        st.subheader("🌐 Enrichment")
        st.session_state.scraper_enabled = st.toggle(
            "Auto-fill missing fields from Jumia",
            value=st.session_state.scraper_enabled,
            help=(
                "Scrapes Color, Warranty and Variation count from Jumia "
                "product pages for any columns absent in your upload. "
                "Runs once per file upload."
            ),
        )
        if st.session_state.scraper_enabled:
            st.caption("⏱ ~1–3 s per product. Runs once on upload.")
    else:
        st.caption(
            "Add `beautifulsoup4` and `requests` to requirements.txt "
            "to enable auto-enrichment."
        )

    st.markdown("---")

    if st.button("🗑 Clear Results", use_container_width=True, type="secondary"):
        _reset_results()
        st.session_state.pq_cached_files = []
        st.rerun()

# ------------------------------------------------------------------
# HEADER BANNER
# ------------------------------------------------------------------
st.markdown(f"""
<div style="background:linear-gradient(135deg,{ORANGE},{ORANGE2});
padding:20px 24px;border-radius:10px;margin-bottom:20px;
box-shadow:0 4px 12px rgba(246,139,30,0.25);">
<h2 style="color:white;margin:0;font-size:26px;font-weight:700;">
🔍 Post-QC Validation</h2>
<p style="color:rgba(255,255,255,0.9);margin:6px 0 0;font-size:13px;">
Upload a Jumia post-QC export to run quality checks</p>
</div>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------
# HARD STOP if postqc.py failed to import
# ------------------------------------------------------------------
if not _POSTQC_OK:
    st.error(
        f"**postqc.py could not be imported.**\n\n"
        f"Error: `{_postqc_err_msg}`\n\n"
        "Make sure `postqc.py` is in the same folder as `streamlit_app.py`."
    )
    st.stop()

# ------------------------------------------------------------------
# FILE UPLOAD
# ------------------------------------------------------------------
st.header("📁 Upload Post-QC File", anchor=False)
st.caption(
    "Expected columns: **SKU, Name, Brand, Category, Price, Seller** "
    "— plus any extras your export includes (Rating, Discount, Stock, etc.)"
)

uploaded = st.file_uploader(
    "Drop your post-QC export here",
    type=["csv", "xlsx"],
    accept_multiple_files=True,
    key="pq_uploader",
)

if uploaded:
    st.session_state.pq_cached_files = [
        {"name": f.name, "bytes": f.read()} for f in uploaded
    ]
elif uploaded is not None and len(uploaded) == 0:
    st.session_state.pq_cached_files = []
    _reset_results()

files = st.session_state.pq_cached_files

# ------------------------------------------------------------------
# PROCESSING
# ------------------------------------------------------------------
if files:
    sig = hashlib.md5(
        (
            str(sorted(f["name"] + str(len(f["bytes"])) for f in files))
            + country_code
        ).encode()
    ).hexdigest()

    if st.session_state.pq_last_sig != sig:
        _reset_results()

        try:
            all_dfs = []
            for uf in files:
                buf = BytesIO(uf["bytes"])
                if uf["name"].endswith(".xlsx"):
                    raw = pd.read_excel(buf, engine="openpyxl", dtype=str)
                else:
                    try:
                        raw = pd.read_csv(buf, dtype=str)
                        if len(raw.columns) <= 1:
                            buf.seek(0)
                            raw = pd.read_csv(
                                buf, sep=";", encoding="ISO-8859-1", dtype=str
                            )
                    except Exception:
                        buf.seek(0)
                        raw = pd.read_csv(
                            buf, sep=";", encoding="ISO-8859-1", dtype=str
                        )

                if detect_file_type(raw) != "post_qc":
                    st.error(
                        f"**{uf['name']}** doesn't look like a post-QC export. "
                        "Expected columns: SKU, Name, Brand, Category, Price, Seller."
                    )
                    st.stop()

                all_dfs.append(raw)

            # ── Support files ──────────────────────────────────────
            support_files = _load_support_files()
            cat_map = support_files.get("category_map", {})
            support_files_pq = dict(support_files)
            support_files_pq["country_code"] = country_code
            support_files_pq["country_name"] = pq_country

            # ── Normalise ──────────────────────────────────────────
            norm_dfs = []
            for df in all_dfs:
                ndf = normalize_post_qc(df, category_map=cat_map)
                # If category_map was empty, try to resolve codes inline
                if cat_map and "CATEGORY" in ndf.columns:
                    resolved = ndf["CATEGORY_CODE"].str.match(r"^\d+$").sum()
                    if resolved == 0:
                        def _resolve(raw, cmap=cat_map):
                            if not raw or raw == "nan":
                                return ""
                            segs = [
                                s.strip()
                                for s in re.split(r"[>/]", str(raw))
                                if s.strip()
                            ]
                            for seg in reversed(segs):
                                code = cmap.get(seg.lower())
                                if code:
                                    return code
                            last = segs[-1] if segs else raw
                            return re.sub(r"[^a-z0-9]", "_", last.lower())
                        ndf["CATEGORY_CODE"] = (
                            ndf["CATEGORY"].astype(str).apply(_resolve)
                        )
                norm_dfs.append(ndf)

            merged = pd.concat(norm_dfs, ignore_index=True)
            merged_dedup = merged.drop_duplicates(
                subset=["PRODUCT_SET_SID"], keep="first"
            )

            # ── Scraper enrichment ─────────────────────────────────
            if _SCRAPER_OK and st.session_state.scraper_enabled:
                _missing = [
                    c for c in [
                        "COLOR", "PRODUCT_WARRANTY", "WARRANTY_DURATION",
                        "COUNT_VARIATIONS", "MAIN_IMAGE",
                    ]
                    if c not in merged_dedup.columns
                    or merged_dedup[c]
                        .astype(str).str.strip()
                        .replace("nan", "").eq("").all()
                ]
                if _missing:
                    _bar = st.progress(
                        0, text=f"Enriching {len(merged_dedup)} products…"
                    )
                    _txt = st.empty()

                    def _cb(done, total, sku,
                            bar=_bar, txt=_txt):
                        bar.progress(
                            done / max(total, 1),
                            text=f"Scraped {done}/{total} — {sku}",
                        )
                        txt.caption(f"Last scraped: {sku}")

                    merged_dedup = enrich_post_qc_df(
                        merged_dedup,
                        country_code=country_code,
                        progress_callback=_cb,
                    )
                    _bar.empty()
                    _txt.empty()

                    filled = sum(
                        1 for c in _missing
                        if c in merged_dedup.columns
                        and not merged_dedup[c]
                            .astype(str).str.strip()
                            .replace("nan", "").eq("").all()
                    )
                    if filled:
                        st.toast(
                            f"✅ Enriched {filled} column(s) from Jumia",
                            icon="🌐",
                        )
                else:
                    st.toast(
                        "All columns already present — no scraping needed.",
                        icon="ℹ️",
                    )

            # ── Run checks ─────────────────────────────────────────
            with st.spinner("Running Post-QC checks…"):
                summary_df, results = run_post_qc_checks(
                    merged_dedup, support_files_pq
                )

            st.session_state.pq_summary  = summary_df
            st.session_state.pq_results  = results
            st.session_state.pq_data     = merged_dedup
            st.session_state.pq_last_sig = sig

        except Exception as exc:
            st.error(f"Processing error: {exc}")
            st.code(traceback.format_exc())

# ------------------------------------------------------------------
# RESULTS
# ------------------------------------------------------------------
if not st.session_state.pq_summary.empty:
    # render_post_qc_section reads from post_qc_* session state keys.
    # Temporarily point them at our pq_* keys, render, then restore.
    _save = {
        k: st.session_state.get(k)
        for k in ("post_qc_summary", "post_qc_results",
                  "post_qc_data", "exports_cache")
    }

    st.session_state.post_qc_summary = st.session_state.pq_summary
    st.session_state.post_qc_results = st.session_state.pq_results
    st.session_state.post_qc_data    = st.session_state.pq_data
    st.session_state.exports_cache   = st.session_state.pq_exports_cache

    support_files = _load_support_files()
    render_post_qc_section(support_files)

    # Persist export cache entries back to pq_ namespace
    st.session_state.pq_exports_cache = st.session_state.exports_cache

    # Restore original keys
    for k, v in _save.items():
        if v is None:
            st.session_state.pop(k, None)
        else:
            st.session_state[k] = v

elif files:
    st.info("⏳ File uploaded — results will appear here once processing completes.")
else:
    st.info("👆 Upload a post-QC export above to get started.")
