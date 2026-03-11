import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import st_yled
from io import BytesIO
from datetime import datetime
import re
import logging
from typing import Dict, List, Tuple, Optional, Set
import traceback
import json
import zipfile
import os
import concurrent.futures
from dataclasses import dataclass
import base64
import hashlib
import requests
from PIL import Image

# -------------------------------------------------
# CONFIGURATION & THEME
# -------------------------------------------------
JUMIA_COLORS = {
    'primary_orange': '#F68B1E',
    'secondary_orange': '#FF9933',
    'jumia_red': '#E73C17',
    'dark_gray': '#313133',
    'medium_gray': '#5A5A5C',
    'light_gray': '#F5F5F5',
    'border_gray': '#E0E0E0',
    'success_green': '#4CAF50',
    'white': '#FFFFFF'
}

PRODUCTSETS_COLS = ["ProductSetSid", "ParentSKU", "Status", "Reason", "Comment", "FLAG", "SellerName"]
FULL_DATA_COLS = [
    "PRODUCT_SET_SID", "ACTIVE_STATUS_COUNTRY", "NAME", "BRAND", "CATEGORY", "CATEGORY_CODE",
    "COLOR", "COLOR_FAMILY", "MAIN_IMAGE", "VARIATION", "PARENTSKU", "SELLER_NAME", "SELLER_SKU",
    "GLOBAL_PRICE", "GLOBAL_SALE_PRICE", "TAX_CLASS", "FLAG", "LISTING_STATUS",
    "PRODUCT_WARRANTY", "WARRANTY_DURATION", "COUNT_VARIATIONS"
]
GRID_COLS = ['PRODUCT_SET_SID', 'NAME', 'BRAND', 'CATEGORY', 'SELLER_NAME', 'MAIN_IMAGE', 'GLOBAL_SALE_PRICE', 'COLOR']

# -------------------------------------------------
# STATE INITIALIZATION
# -------------------------------------------------
for key, val in {
    'layout_mode': "wide", 'final_report': pd.DataFrame(), 'all_data_map': pd.DataFrame(),
    'file_mode': None, 'grid_page': 0, 'grid_items_per_page': 50, 'main_toasts': [],
    'exports_cache': {}, 'display_df_cache': {}, 'do_scroll_top': False
}.items():
    if key not in st.session_state: st.session_state[key] = val

try: st.set_page_config(page_title="Product Tool", layout=st.session_state.layout_mode)
except: pass
st_yled.init()

# -------------------------------------------------
# UTILITIES (Mapping, Hashing, Formatting)
# -------------------------------------------------
def clean_category_code(code) -> str:
    if pd.isna(code): return ""
    s = str(code).strip()
    return s.split('.')[0] if '.' in s else s

def df_hash(df: pd.DataFrame) -> str:
    return hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()

def normalize_text(text: str) -> str:
    if pd.isna(text): return ""
    text = str(text).lower().strip()
    return re.sub(r'[^\w\s]', '', text).replace(" ", "")

def create_match_key(row: pd.Series) -> str:
    return f"{normalize_text(row.get('BRAND',''))}|{normalize_text(row.get('NAME',''))}|{normalize_text(row.get('COLOR',''))}"

# -------------------------------------------------
# DATA LOADING (LOCAL FILES)
# -------------------------------------------------
@st.cache_data(ttl=3600)
def load_support_files_lazy():
    # Helper to load txt/excel rules (logic simplified for brevity, matches your source)
    def load_txt(f): return [l.strip() for l in open(f, 'r') if l.strip()] if os.path.exists(f) else []
    
    return {
        'flags_mapping': load_flags_mapping(), # Uses your existing Excel logic
        'unnecessary_words': load_txt('unnecessary.txt'),
        'colors': load_txt('colors.txt'),
        'color_categories': load_txt('color_cats.txt'),
        'category_fas': load_txt('Fashion_cat.txt'),
        'warranty_category_codes': load_txt('warranty.txt'),
        'restricted_brands_all': load_restricted_brands_from_local(),
        'prohibited_words_all': load_prohibited_from_local(),
        'sneaker_category_codes': load_txt('Sneakers_Cat.txt'),
        'sneaker_sensitive_brands': [b.lower() for b in load_txt('Sneakers_Sensitive.txt')],
        # ... include all other keys (jerseys_data, books_data, etc.)
    }

# -------------------------------------------------
# VALIDATION ENGINE
# -------------------------------------------------
# [Includes all your check_ functions: check_miscellaneous_category, check_restricted_brands, etc.]

@st.cache_data(show_spinner=False, ttl=3600)
def cached_validate_products(data_hash, _data, _support_files, country_code, data_has_warranty_cols):
    # This wrapper prevents re-validating the same file on every UI interaction
    from logic_engine import validate_products # Assume logic is accessible
    return validate_products(_data, _support_files, CountryValidator(st.session_state.selected_country), data_has_warranty_cols)

# -------------------------------------------------
# ACTION HANDLERS
# -------------------------------------------------
def apply_rejection(sids, reason_code, comment, flag_name):
    mask = st.session_state.final_report['ProductSetSid'].isin(sids)
    st.session_state.final_report.loc[mask, ['Status', 'Reason', 'Comment', 'FLAG']] = ['Rejected', reason_code, comment, flag_name]
    st.session_state.exports_cache.clear()
    st.session_state.display_df_cache.clear()

def restore_single_item(sid):
    mask = st.session_state.final_report['ProductSetSid'] == sid
    st.session_state.final_report.loc[mask, ['Status', 'Reason', 'Comment', 'FLAG']] = ['Approved', '', '', 'Approved by User']
    st.session_state.pop(f"quick_rej_{sid}", None)
    st.session_state.exports_cache.clear()
    st.session_state.display_df_cache.clear()

# -------------------------------------------------
# UI SECTION: FLAG EXPANDERS
# -------------------------------------------------
@st.fragment
def render_flag_sections():
    if st.session_state.final_report.empty: return
    
    st.subheader(":material/flag: Flags Breakdown", anchor=False)
    fr = st.session_state.final_report
    data = st.session_state.all_data_map
    
    rej_only = fr[fr['Status'] == 'Rejected']
    if rej_only.empty:
        st.success("No rejections found!")
        return

    for flag_name in rej_only['FLAG'].unique():
        count = len(rej_only[rej_only['FLAG'] == flag_name])
        with st.expander(f"{flag_name} ({count})"):
            # 1. Prepare display data
            cache_key = f"disp_{flag_name}"
            if cache_key not in st.session_state.display_df_cache:
                flagged_sids = rej_only[rej_only['FLAG'] == flag_name][['ProductSetSid']]
                merged = pd.merge(flagged_sids, data, left_on='ProductSetSid', right_on='PRODUCT_SET_SID')
                st.session_state.display_df_cache[cache_key] = merged
            
            df_view = st.session_state.display_df_cache[cache_key]
            
            # 2. Dataframe with Selection
            event = st.dataframe(
                df_view[['PRODUCT_SET_SID', 'NAME', 'BRAND', 'SELLER_NAME']],
                hide_index=True, use_container_width=True,
                selection_mode="multi-row", key=f"tbl_{flag_name}"
            )
            
            # 3. Bulk Actions
            selected_rows = event.selection.rows
            c1, c2 = st.columns(2)
            if c1.button(f"✓ Approve Selected ({len(selected_rows)})", key=f"app_{flag_name}", disabled=not selected_rows):
                sids = df_view.iloc[selected_rows]['PRODUCT_SET_SID'].tolist()
                for s in sids: restore_single_item(s)
                st.rerun()
            
            if c2.button(f"↩ Restore All {count}", key=f"all_{flag_name}"):
                for s in df_view['PRODUCT_SET_SID'].tolist(): restore_single_item(s)
                st.rerun()

# -------------------------------------------------
# UI SECTION: IMAGE GRID (HTML/JS BRIDGE)
# -------------------------------------------------
@st.fragment
def render_manual_review():
    if st.session_state.final_report.empty: return
    
    st.markdown("---")
    st.header(":material/pageview: Manual Image Review", anchor=False)

    # Bridge for JS communication
    action_bridge = st.text_input("bridge", key="card_action_bridge", label_visibility="collapsed", placeholder="__CARD_ACT__")
    
    if action_bridge:
        st.session_state["card_action_bridge"] = ""
        if _process_card_bridge_action(action_bridge, support_files):
            st.rerun()

    # Pagination & Filtering
    # [Insert your column filters and search logic here]
    
    # Render HTML Grid
    # [Insert build_fast_grid_html call here]

# -------------------------------------------------
# MAIN APP FLOW
# -------------------------------------------------
def main():
    # 1. Sidebar & Setup
    # ... (Country selection, file uploader)
    
    # 2. Processing Logic
    if uploaded_files and st.session_state.get('last_processed_files') != process_signature:
        # Standardize, Filter, and Validate
        # final_report, results = cached_validate_products(...)
        pass

    # 3. Render UI Parts
    if not st.session_state.final_report.empty:
        # A. Summary Metrics
        # B. Flag Expanders (The missing part you wanted back)
        render_flag_sections()
        
        # C. Manual Image Grid
        render_manual_review()
        
        # D. Exports
        render_exports_section()

if __name__ == "__main__":
    main()
