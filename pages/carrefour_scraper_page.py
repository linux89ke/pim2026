"""
Carrefour Kenya scraper - Streamlit UI (page module)

This lives in your app's pages/ folder. It imports carrefour_scraper.py,
which should sit in your project ROOT (same level as your main app
script, e.g. Test.py) - NOT inside pages/, since Streamlit tries to
treat every .py file directly under pages/ as its own page.

Requires: streamlit, requests, pandas, openpyxl
    pip install streamlit requests pandas openpyxl
"""

import sys
import os

# Streamlit does not automatically add the project root to sys.path when
# running a script from pages/, so a plain `import carrefour_scraper`
# fails with ModuleNotFoundError even if the file is right there in the
# root. Add the parent directory (one level up from pages/) explicitly.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_THIS_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st
import pandas as pd
import io

import carrefour_scraper_playwright as cs

st.set_page_config(page_title="Carrefour KE Scraper", page_icon="🛒", layout="wide")

st.title("🛒 Carrefour Kenya Scraper")
st.caption(
    "Reverse-engineered from carrefour.ke's internal API (Majid Al Futtaim "
    "storefront). No login required - runs as an anonymous session."
)

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "session" not in st.session_state:
    st.session_state.session = None
if "category_tree" not in st.session_state:
    st.session_state.category_tree = None
if "flat_categories" not in st.session_state:
    st.session_state.flat_categories = None
if "products_df" not in st.session_state:
    st.session_state.products_df = None


def get_or_create_session():
    if st.session_state.session is None:
        st.session_state.session = cs.get_session()
    return st.session_state.session


def show_scraper_error(e: cs.ScraperError):
    st.error(str(e))
    if e.status_code is not None:
        st.caption(f"HTTP status: {e.status_code}")
    if e.body:
        with st.expander("Raw response body (for debugging)"):
            st.code(e.body, language="html")
    st.info(
        "If this keeps happening, the site may be blocking requests from "
        "this network/IP or requires more browser-like headers. Try again "
        "from a different network, or re-capture a fresh HAR and check "
        "whether the header set has changed."
    )


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Sidebar - category tree
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Category Tree")

    if st.button("Fetch category tree", use_container_width=True):
        session = get_or_create_session()
        with st.spinner("Fetching category tree..."):
            try:
                tree = cs.get_category_tree(session)
                st.session_state.category_tree = tree
                st.session_state.flat_categories = cs.flatten_categories(tree)
                st.success(f"Loaded {len(st.session_state.flat_categories)} categories.")
            except cs.ScraperError as e:
                show_scraper_error(e)
            except Exception as e:
                st.error(f"Unexpected error: {e}")

    if st.session_state.flat_categories:
        flat = st.session_state.flat_categories
        df_cats = pd.DataFrame(flat)
        st.dataframe(
            df_cats[["level", "path", "count", "id"]],
            use_container_width=True,
            height=300,
        )
        st.download_button(
            "Download categories (CSV)",
            df_cats.to_csv(index=False).encode("utf-8"),
            file_name="carrefour_ke_categories.csv",
            mime="text/csv",
            use_container_width=True,
        )

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_category, tab_search, tab_related = st.tabs(
    ["📂 Scrape by category", "🔍 Keyword search", "🔗 Related products"]
)

# --- Tab 1: category listing scrape ---
with tab_category:
    st.subheader("Scrape a full category listing")

    col1, col2 = st.columns([2, 1])
    with col1:
        if st.session_state.flat_categories:
            options = {
                f"{row['path']} (count={row['count']}, id={row['id']})": row["id"]
                for row in st.session_state.flat_categories
                if row["id"]
            }
            chosen_label = st.selectbox("Pick a category (or type an ID below)", list(options.keys()))
            category_id_input = options.get(chosen_label, "")
        else:
            st.info("Fetch the category tree in the sidebar to pick from a list, or type an ID manually below.")
            category_id_input = ""

        category_id = st.text_input("Category ID", value=category_id_input, placeholder="e.g. NFKEN3020000")

    with col2:
        limit_pages = st.checkbox("Limit pages (for testing)", value=True)
        max_pages = st.number_input("Max pages", min_value=1, value=2, disabled=not limit_pages)

    if st.button("Scrape category", type="primary", disabled=not category_id):
        session = get_or_create_session()
        progress_bar = st.progress(0.0)
        status_text = st.empty()

        collected = {"products": []}

        def progress_cb(page, total_pages, running_total):
            total_pages = total_pages or 1
            progress_bar.progress(min(page / total_pages, 1.0))
            status_text.text(f"Page {page}/{total_pages} — {running_total} products collected so far")

        try:
            with st.spinner("Scraping..."):
                products = cs.get_category_products(
                    session,
                    category_id,
                    max_pages=(int(max_pages) if limit_pages else None),
                    progress_callback=progress_cb,
                )
            progress_bar.progress(1.0)
            if products:
                df = pd.DataFrame(products)
                st.session_state.products_df = df
                st.success(f"Collected {len(df)} products.")
            else:
                st.warning("No products returned for this category ID.")
        except cs.ScraperError as e:
            show_scraper_error(e)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

    if st.session_state.products_df is not None:
        df = st.session_state.products_df
        st.dataframe(df, use_container_width=True, height=400)

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "Download as CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name="carrefour_ke_products.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            st.download_button(
                "Download as Excel",
                to_excel_bytes(df),
                file_name="carrefour_ke_products.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

# --- Tab 2: keyword search ---
with tab_search:
    st.subheader("Search by keyword")
    st.caption(
        "Uses the site's search route directly. Note: this returns whatever "
        "the first page of results contains (pagination for this specific "
        "route hasn't been mapped) - for exhaustive results on a topic, "
        "prefer the category scrape tab."
    )

    keyword = st.text_input("Keyword", placeholder="e.g. blueband")
    if st.button("Search", type="primary", disabled=not keyword):
        session = get_or_create_session()
        try:
            with st.spinner(f"Searching for '{keyword}'..."):
                results = cs.search_products(session, keyword)
            if results:
                df_search = pd.DataFrame(results)
                st.success(f"Found {len(df_search)} products.")
                st.dataframe(df_search, use_container_width=True, height=400)
                st.download_button(
                    "Download search results (CSV)",
                    df_search.to_csv(index=False).encode("utf-8"),
                    file_name=f"carrefour_ke_search_{keyword}.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No products found for that keyword.")
        except cs.ScraperError as e:
            show_scraper_error(e)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

# --- Tab 3: related products ---
with tab_related:
    st.subheader("Get related / recommended products")
    st.caption("Given a known product ID, pulls frequently-bought-together and sponsored recommendations.")

    col1, col2 = st.columns(2)
    with col1:
        product_id = st.text_input("Product ID", placeholder="e.g. 28484")
    with col2:
        category_hint = st.text_input("Category hint (optional)", placeholder="e.g. Cleaning & Household")

    if st.button("Get related products", type="primary", disabled=not product_id):
        session = get_or_create_session()
        try:
            with st.spinner("Fetching..."):
                related = cs.get_related_products(session, product_id, category_id=category_hint)
            placements = related.get("placements", [])
            if not placements:
                st.warning("No placements returned.")
            for p in placements:
                st.markdown(f"**{p.get('strategyMessage', p.get('placement', 'Recommendations'))}**")
                recs = p.get("recommendedProducts", [])
                if recs:
                    df_rel = pd.DataFrame(recs)
                    st.dataframe(df_rel, use_container_width=True)
                else:
                    st.caption("No products in this placement.")
        except cs.ScraperError as e:
            show_scraper_error(e)
        except Exception as e:
            st.error(f"Unexpected error: {e}")

st.divider()
st.caption(
    "⚠️ Be a good citizen: this hits carrefour.ke's production API directly. "
    "Keep delays between requests, avoid tight parallel loops, and don't "
    "hammer this at scale."
)
