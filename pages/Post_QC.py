import sys
import os
import re
import hashlib
import traceback
import logging
import json
import concurrent.futures
from io import BytesIO

# ------------------------------------------------------------------
# PATH FIX
# ------------------------------------------------------------------
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Professional Icons (SVG strings for use in Markdown)
ICON_SEARCH = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>'
ICON_FILE = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path><polyline points="13 2 13 9 20 9"></polyline></svg>'
ICON_SHIELD = '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path></svg>'
ICON_ALERT = '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#E73C17" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>'

# ... [Keep imports and logic as in your original file] ...

# ------------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------------
st.set_page_config(
    page_title="Post-QC Analysis",
    page_icon="🔍",
    layout="wide",
)

# ------------------------------------------------------------------
# THEME & STYLE OVERRIDES
# ------------------------------------------------------------------
ORANGE  = "#F68B1E"
ORANGE2 = "#FF9933"
RED     = "#E73C17"
DARK    = "#313133"
MED     = "#5A5A5C"
LIGHT   = "#F8F9FA"
BORDER  = "#E0E4E8"

st.markdown(f"""
<style>
    /* Global professional fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}

    /* Custom Header Banner */
    .header-banner {{
        background: linear-gradient(90deg, {DARK} 0%, {MED} 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        border-left: 6px solid {ORANGE};
    }}

    /* Professional Badges */
    .status-badge {{
        padding: 4px 12px;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    .badge-success {{ background: #E8F5E9; color: #2E7D32; border: 1px solid #A5D6A7; }}
    .badge-warning {{ background: #FFF3E0; color: #E65100; border: 1px solid #FFCC80; }}

    /* Card styling for Metrics */
    div[data-testid="stMetric"] {{
        background: white;
        border: 1px solid {BORDER};
        border-radius: 10px;
        padding: 15px !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02);
    }}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------
# HEADER
# ------------------------------------------------------------------
st.markdown(f"""
<div class="header-banner">
    <div style="display: flex; align-items: center; gap: 12px;">
        {ICON_SEARCH}
        <h1 style="color: white; margin: 0; font-size: 24px; font-weight: 700;">Post-QC Validation Engine</h1>
    </div>
    <p style="margin: 8px 0 0; opacity: 0.8; font-size: 14px;">
        Standardized data integrity analysis and automated field enrichment.
    </p>
</div>
""", unsafe_allow_html=True)

# ... [Keep logic for input fetching and pipeline] ...

# ------------------------------------------------------------------
# REPLACEMENT FOR DATA QUALITY FLAGS (Professional Icons)
# ------------------------------------------------------------------
def _render_data_quality_flags(df: pd.DataFrame) -> None:
    flag_cfg = [
        ("_OLD_PRICE_CORRUPTED", "Price Concatenation Detected", "Global price exceeds sale price by >500x."),
        ("_DISCOUNT_MISMATCH", "Price-Discount Variance", "Calculated discount deviates from stated discount by >5%."),
        ("_BRAND_IN_TITLE", "Redundant Branding", "Brand name appears multiple times in the product title."),
        ("_RATING_NO_REVIEWS", "Inconsistent Rating Data", "Positive rating exists without associated review counts.")
    ]

    for flag_col, title, desc in flag_cfg:
        if flag_col in df.columns:
            flagged = df[df[flag_col] == True]
            if not flagged.empty:
                with st.expander(f"{title} ({len(flagged)})"):
                    st.markdown(f"""
                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 10px;">
                        {ICON_ALERT} <span style="font-size: 13px; color: {MED};">{desc}</span>
                    </div>
                    """, unsafe_allow_html=True)
                    st.dataframe(flagged, use_container_width=True)

# ------------------------------------------------------------------
# VALIDATION RESULTS (Professional Layout)
# ------------------------------------------------------------------
if not _val_report.empty:
    st.markdown(f"### {ICON_SHIELD} Audit Summary", unsafe_allow_html=True)
    
    # ... [Logic for mc1, mc2, mc3 metrics] ...

    st.markdown("---")
    st.markdown("#### Detailed Action Report")
    
    # Update Flag Breakdown labels to be more professional
    # e.g., Replace "Flags Breakdown" with "Validation Exceptions"
    
# ... [Rest of the file] ...
