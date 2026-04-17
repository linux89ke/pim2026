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
    type    = ["gz","json"],
    help    = "Export from the main Jumia Scraper app using the 'Export for Viewer' button",
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

    # Search
    search = st.text_input("Search products", placeholder="e.g. Samsung TV")

    # Category filter
    cats = sorted(df["Category"].dropna().unique().tolist()) if "Category" in df.columns else []
    sel_cats = st.multiselect("Category", cats)

    # Brand filter
    brands = sorted(df["Brand"].dropna().unique().tolist()) if "Brand" in df.columns else []
    sel_brands = st.multiselect("Brand", brands)

    # AI grade filter
    if "AI Summary" in df.columns:
        grades = ["A","B","C","D","F"]
        sel_grades = st.multiselect("AI Grade", grades, default=[])
    else:
        sel_grades = []

    # Price range
    if "Price (KSh)" in df.columns:
        prices = pd.to_numeric(df["Price (KSh)"], errors="coerce").dropna()
        if len(prices) > 0:
            min_p = int(prices.min())
            max_p = int(prices.max())
            if min_p < max_p:
                price_range = st.slider(
                    "Price (KSh)",
                    min_value = min_p,
                    max_value = max_p,
                    value     = (min_p, max_p),
                    step      = 100,
                )
            else:
                price_range = (min_p, max_p)
        else:
            price_range = None
    else:
        price_range = None

    # Sort
    sort_by = st.selectbox(
        "Sort by",
        ["Default","Price: Low to High","Price: High to Low",
         "Rating","AI Score","Name A-Z"],
    )

    st.markdown("---")
    st.caption(f"{len(df)} products in file")

# ─── Apply filters ────────────────────────────────────────────────────────────
filtered = df.copy()

if search:
    mask = filtered.apply(
        lambda r: search.lower() in str(r.get("Product Name","")).lower()
                  or search.lower() in str(r.get("Title","")).lower()
                  or search.lower() in str(r.get("Brand","")).lower(),
        axis=1,
    )
    filtered = filtered[mask]

if sel_cats:
    filtered = filtered[filtered["Category"].isin(sel_cats)]

if sel_brands:
    filtered = filtered[filtered["Brand"].isin(sel_brands)]

if sel_grades and "AI Summary" in filtered.columns:
    def get_grade(ai_raw):
        try:
            return json.loads(ai_raw).get("grade","")
        except Exception:
            return ""
    filtered = filtered[filtered["AI Summary"].apply(get_grade).isin(sel_grades)]

if price_range and "Price (KSh)" in filtered.columns:
    filtered["_price_num"] = pd.to_numeric(filtered["Price (KSh)"], errors="coerce")
    filtered = filtered[
        (filtered["_price_num"] >= price_range[0]) &
        (filtered["_price_num"] <= price_range[1])
    ]
    filtered = filtered.drop(columns=["_price_num"], errors="ignore")

# Sort
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
    def get_score(ai_raw):
        try:
            return json.loads(ai_raw).get("overall_score", 0)
        except Exception:
            return 0
    filtered = filtered.copy()
    filtered["_s"] = filtered["AI Summary"].apply(get_score)
    filtered = filtered.sort_values("_s", ascending=False).drop(columns=["_s"])
elif sort_by == "Name A-Z":
    name_col = "Product Name" if "Product Name" in filtered.columns else "Title"
    if name_col in filtered.columns:
        filtered = filtered.sort_values(name_col)

# ─── Stats bar ────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Products", len(filtered))
with col2:
    ai_done = filtered["AI Summary"].apply(has_ai_summary).sum() if "AI Summary" in filtered.columns else 0
    st.metric("AI Analysed", ai_done)
with col3:
    prices_num = pd.to_numeric(filtered.get("Price (KSh)", pd.Series()), errors="coerce").dropna()
    avg_price  = f"KSh {int(prices_num.mean()):,}" if len(prices_num) else "N/A"
    st.metric("Avg Price", avg_price)
with col4:
    def get_score(ai_raw):
        try:
            return json.loads(ai_raw).get("overall_score", None)
        except Exception:
            return None
    if "AI Summary" in filtered.columns:
        scores = filtered["AI Summary"].apply(get_score).dropna()
        avg_score = f"{scores.mean():.0f}/100" if len(scores) else "N/A"
    else:
        avg_score = "N/A"
    st.metric("Avg AI Score", avg_score)

st.markdown("---")

# ─── View toggle ──────────────────────────────────────────────────────────────
view_mode = st.radio(
    "View",
    ["Card Grid", "Detailed List"],
    horizontal=True,
    label_visibility="collapsed",
)

if filtered.empty:
    st.warning("No products match your filters.")
    st.stop()

# ─── Card grid view ───────────────────────────────────────────────────────────
if view_mode == "Card Grid":
    cols_per_row = st.select_slider(
        "Columns", options=[2,3,4,5], value=4,
        label_visibility="collapsed",
    )
    render_card_grid(filtered.reset_index(drop=True), cols_per_row=cols_per_row)

# ─── Detailed list view ───────────────────────────────────────────────────────
else:
    filtered_reset = filtered.reset_index(drop=True)
    for idx, (_, row) in enumerate(filtered_reset.iterrows()):
        display_pdp_card(row, ai_api_key="", card_idx=idx)
        st.markdown("---")
