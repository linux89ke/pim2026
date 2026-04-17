import sys
import os

# ── Make parent directory importable on Streamlit Cloud ──────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import gzip
import streamlit as st
import pandas as pd

from helpers        import safe_str, safe_float, safe_int, has_ai_summary
from ui_components  import render_card_grid, display_pdp_card
from export_utils   import export_json_to_df

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = "Jumia Product Viewer",
    page_icon  = "🛍️",
    layout     = "wide",
)

st.title("🛍️ Jumia Product Viewer")
st.caption("Upload an export file to browse products with full AI analysis — no scraping needed.")

# ─── File upload ──────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    "Upload export file (.json.gz)",
    type = ["gz","json"],
    help = "Export from the main app using the 'Export for Viewer' button",
)

if not uploaded:
    st.info("Upload an export file above to get started.")
    st.stop()

# ─── Load data ────────────────────────────────────────────────────────────────
try:
    raw_bytes = uploaded.read()
    df        = export_json_to_df(raw_bytes)
    st.success(f"Loaded {len(df)} products")
except Exception as e:
    st.error(f"Could not read export file: {e}")
    st.stop()

if df.empty:
    st.warning("No products found in the export file.")
    st.stop()

# ─── Sidebar filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")

    search = st.text_input("Search products", placeholder="e.g. Samsung TV")

    cats = sorted(df["Category"].dropna().unique().tolist()) if "Category" in df.columns else []
    sel_cats = st.multiselect("Category", cats)

    brands = sorted(df["Brand"].dropna().unique().tolist()) if "Brand" in df.columns else []
    sel_brands = st.multiselect("Brand", brands)

    if "AI Summary" in df.columns:
        sel_grades = st.multiselect("AI Grade", ["A","B","C","D","F"])
    else:
        sel_grades = []

    if "Price (KSh)" in df.columns:
        prices = pd.to_numeric(df["Price (KSh)"], errors="coerce").dropna()
        if len(prices) > 1 and prices.min() < prices.max():
            price_range = st.slider(
                "Price (KSh)",
                min_value = int(prices.min()),
                max_value = int(prices.max()),
                value     = (int(prices.min()), int(prices.max())),
                step      = 100,
            )
        else:
            price_range = None
    else:
        price_range = None

    sort_by = st.selectbox(
        "Sort by",
        ["Default", "Price: Low to High", "Price: High to Low",
         "Rating", "AI Score", "Name A-Z"],
    )

    st.markdown("---")
    st.caption(f"{len(df)} products in file")

# ─── Apply filters ────────────────────────────────────────────────────────────
filtered = df.copy()

if search:
    mask = filtered.apply(
        lambda r: (
            search.lower() in str(r.get("Product Name","")).lower()
            or search.lower() in str(r.get("Title","")).lower()
            or search.lower() in str(r.get("Brand","")).lower()
        ),
        axis=1,
    )
    filtered = filtered[mask]

if sel_cats and "Category" in filtered.columns:
    filtered = filtered[filtered["Category"].isin(sel_cats)]

if sel_brands and "Brand" in filtered.columns:
    filtered = filtered[filtered["Brand"].isin(sel_brands)]

if sel_grades and "AI Summary" in filtered.columns:
    def _grade(ai_raw):
        try:    return json.loads(ai_raw).get("grade","")
        except: return ""
    filtered = filtered[filtered["AI Summary"].apply(_grade).isin(sel_grades)]

if price_range and "Price (KSh)" in filtered.columns:
    filtered = filtered.copy()
    filtered["_p"] = pd.to_numeric(filtered["Price (KSh)"], errors="coerce")
    filtered = filtered[
        (filtered["_p"] >= price_range[0]) &
        (filtered["_p"] <= price_range[1])
    ].drop(columns=["_p"])

# ─── Sort ─────────────────────────────────────────────────────────────────────
def _score(ai_raw):
    try:    return json.loads(ai_raw).get("overall_score", 0)
    except: return 0

if sort_by == "Price: Low to High" and "Price (KSh)" in filtered.columns:
    filtered = filtered.copy()
    filtered["_s"] = pd.to_numeric(filtered["Price (KSh)"], errors="coerce")
    filtered = filtered.sort_values("_s").drop(columns=["_s"])
elif sort_by == "Price: High to Low" and "Price (KSh)" in filtered.columns:
    filtered = filtered.copy()
    filtered["_s"] = pd.to_numeric(filtered["Price (KSh)"], errors="coerce")
    filtered = filtered.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_by == "Rating" and "Rating" in filtered.columns:
    filtered = filtered.copy()
    filtered["_s"] = pd.to_numeric(filtered["Rating"], errors="coerce")
    filtered = filtered.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_by == "AI Score" and "AI Summary" in filtered.columns:
    filtered = filtered.copy()
    filtered["_s"] = filtered["AI Summary"].apply(_score)
    filtered = filtered.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_by == "Name A-Z":
    name_col = "Product Name" if "Product Name" in filtered.columns else "Title"
    if name_col in filtered.columns:
        filtered = filtered.sort_values(name_col)

# ─── Stats bar ────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Products", len(filtered))
with c2:
    ai_done = (
        filtered["AI Summary"].apply(has_ai_summary).sum()
        if "AI Summary" in filtered.columns else 0
    )
    st.metric("AI Analysed", ai_done)
with c3:
    prices_num = pd.to_numeric(
        filtered.get("Price (KSh)", pd.Series()), errors="coerce"
    ).dropna()
    st.metric("Avg Price", f"KSh {int(prices_num.mean()):,}" if len(prices_num) else "N/A")
with c4:
    if "AI Summary" in filtered.columns:
        scores = filtered["AI Summary"].apply(_score)
        scores = scores[scores > 0]
        st.metric("Avg AI Score", f"{scores.mean():.0f}/100" if len(scores) else "N/A")
    else:
        st.metric("Avg AI Score", "N/A")

st.markdown("---")

# ─── View toggle ──────────────────────────────────────────────────────────────
v1, v2 = st.columns([2, 1])
with v1:
    view_mode = st.radio(
        "View", ["Card Grid", "Detailed List"],
        horizontal=True,
        label_visibility="collapsed",
    )
with v2:
    if view_mode == "Card Grid":
        cols_per_row = st.select_slider(
            "Columns", options=[2,3,4,5], value=4,
            label_visibility="collapsed",
        )
    else:
        cols_per_row = 4

if filtered.empty:
    st.warning("No products match your filters.")
    st.stop()

# ─── Render ───────────────────────────────────────────────────────────────────
filtered_reset = filtered.reset_index(drop=True)

if view_mode == "Card Grid":
    render_card_grid(filtered_reset, cols_per_row=cols_per_row)
else:
    for idx, (_, row) in enumerate(filtered_reset.iterrows()):
        display_pdp_card(row, ai_api_key="", card_idx=idx)
        st.markdown("---")
