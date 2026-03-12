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
import time
import hashlib
import requests
from PIL import Image
import random

try:
    from postqc import detect_file_type, normalize_post_qc, run_checks as run_post_qc_checks, render_post_qc_section
except ImportError:
    pass

# ────────────────────────────────────────────────
# JUMIA THEME & CONSTANTS (unchanged)
# ────────────────────────────────────────────────

JUMIA_COLORS = {
    'primary_orange': '#F68B1E',
    'secondary_orange': '#FF9933',
    'jumia_red': '#E73C17',
    'dark_gray': '#313133',
    'medium_gray': '#5A5A5C',
    'light_gray': '#F5F5F5',
    'border_gray': '#E0E0E0',
    'success_green': '#4CAF50',
    'warning_yellow': '#FFC107',
    'white': '#FFFFFF',
    'black': '#000000'
}

PRODUCTSETS_COLS = ["ProductSetSid", "ParentSKU", "Status", "Reason", "Comment", "FLAG", "SellerName"]
REJECTION_REASONS_COLS = ['CODE - REJECTION_REASON', 'COMMENT']
FULL_DATA_COLS = [
    "PRODUCT_SET_SID", "ACTIVE_STATUS_COUNTRY", "NAME", "BRAND", "CATEGORY", "CATEGORY_CODE",
    "COLOR", "COLOR_FAMILY", "MAIN_IMAGE", "VARIATION", "PARENTSKU", "SELLER_NAME", "SELLER_SKU",
    "GLOBAL_PRICE", "GLOBAL_SALE_PRICE", "TAX_CLASS", "FLAG", "LISTING_STATUS",
    "PRODUCT_WARRANTY", "WARRANTY_DURATION", "WARRANTY_ADDRESS", "WARRANTY_TYPE", "COUNT_VARIATIONS",
    "LIST_VARIATIONS"
]
GRID_COLS = ['PRODUCT_SET_SID', 'NAME', 'BRAND', 'CATEGORY', 'SELLER_NAME', 'MAIN_IMAGE', 'GLOBAL_SALE_PRICE', 'GLOBAL_PRICE', 'COLOR']

FX_RATE = 128.0
COUNTRY_CURRENCY = {
    "Kenya": {"code": "KES", "symbol": "KSh", "pair": "USD/KES"},
    "Uganda": {"code": "UGX", "symbol": "USh", "pair": "USD/UGX"},
    "Nigeria": {"code": "NGN", "symbol": "₦", "pair": "USD/NGN"},
    "Ghana": {"code": "GHS", "symbol": "GH₵", "pair": "USD/GHS"},
    "Morocco": {"code": "MAD", "symbol": "MAD", "pair": "USD/MAD"},
}

# ... (keep all your original @st.cache_data functions: fetch_exchange_rate, format_local_price, etc.)

SPLIT_LIMIT = 9998
NEW_FILE_MAPPING = { ... }  # keep your original mapping

logger = logging.getLogger(__name__)

# Session state initialization (expanded with pending)
if 'layout_mode' not in st.session_state: st.session_state.layout_mode = "wide"
if 'final_report' not in st.session_state: st.session_state.final_report = pd.DataFrame()
if 'all_data_map' not in st.session_state: st.session_state.all_data_map = pd.DataFrame()
if 'post_qc_summary' not in st.session_state: st.session_state.post_qc_summary = pd.DataFrame()
if 'post_qc_results' not in st.session_state: st.session_state.post_qc_results = {}
if 'post_qc_data' not in st.session_state: st.session_state.post_qc_data = pd.DataFrame()
if 'file_mode' not in st.session_state: st.session_state.file_mode = None
if 'intersection_sids' not in st.session_state: st.session_state.intersection_sids = set()
if 'intersection_count' not in st.session_state: st.session_state.intersection_count = 0
if 'grid_page' not in st.session_state: st.session_state.grid_page = 0
if 'grid_items_per_page' not in st.session_state: st.session_state.grid_items_per_page = 50
if 'main_toasts' not in st.session_state: st.session_state.main_toasts = []
if 'exports_cache' not in st.session_state: st.session_state.exports_cache = {}
if 'do_scroll_top' not in st.session_state: st.session_state.do_scroll_top = False
if 'display_df_cache' not in st.session_state: st.session_state.display_df_cache = {}
if 'committed' not in st.session_state: st.session_state.committed = {}     # sid → reason (final)
if 'pending' not in st.session_state: st.session_state.pending = {}         # sid → reason (waiting confirm)
if 'grid_bridge' not in st.session_state: st.session_state.grid_bridge = ""
if 'grid_msg_counter' not in st.session_state: st.session_state.grid_msg_counter = 0

try: st.set_page_config(page_title="Product Tool", layout=st.session_state.layout_mode)
except: pass

st_yled.init()

# Your original global CSS (unchanged)
st.markdown(f"""<style>...</style>""", unsafe_allow_html=True)  # ← paste your full CSS here

# ... keep get_default_country(), country selection, toasts, clean_category_code(), normalize_text(), etc.

# ────────────────────────────────────────────────
# LOAD SUPPORT FILES (unchanged)
# ────────────────────────────────────────────────

try:
    support_files = load_support_files_lazy()
except Exception as e:
    st.error(f"Failed to load configs: {e}")
    st.stop()

# ────────────────────────────────────────────────
# FILE UPLOAD & PROCESSING (mostly unchanged)
# ────────────────────────────────────────────────

st.header(":material/upload_file: Upload Files", anchor=False)
country_choice = st.segmented_control("Country", ["Kenya", "Uganda", "Nigeria", "Ghana", "Morocco"],
                                      default=st.session_state.get('selected_country', 'Kenya'))
if country_choice:
    st.session_state.selected_country = country_choice

country_validator = CountryValidator(st.session_state.selected_country)

uploaded_files = st.file_uploader("Upload CSV or XLSX files", type=['csv', 'xlsx'], accept_multiple_files=True)

# Your original file processing logic here (keep as-is)
# When files change → reset states including committed & pending
if uploaded_files:
    # ... your signature + reset logic ...
    st.session_state.committed.clear()
    st.session_state.pending.clear()

    # ... rest of upload / validation / caching logic ...

# ────────────────────────────────────────────────
# IMPROVED GRID (fast click + batch safe)
# ────────────────────────────────────────────────

@st.fragment
def render_image_grid():
    if st.session_state.final_report.empty or st.session_state.file_mode == "post_qc":
        return

    st.markdown("---")
    st.header(":material/pageview: Manual Image & Category Review", anchor=False)

    fr = st.session_state.final_report
    data = st.session_state.all_data_map

    committed_rej_sids = set(st.session_state.committed.keys())
    mask = (fr["Status"] == "Approved") | (fr["ProductSetSid"].isin(committed_rej_sids))
    valid_grid_df = fr[mask]

    # Search & filter
    c1, c2, c3 = st.columns([1.5, 1.5, 2])
    with c1: search_n = st.text_input("Search by Name", key="search_name")
    with c2: search_sc = st.text_input("Seller or Category", key="search_sc")
    with c3: st.session_state.grid_items_per_page = st.select_slider("Items/page", [20,50,100,200],
                                                                     value=st.session_state.grid_items_per_page)

    review_data = pd.merge(
        valid_grid_df[["ProductSetSid"]],
        data[GRID_COLS],
        left_on="ProductSetSid", right_on="PRODUCT_SET_SID", how="left"
    )

    if search_n:
        review_data = review_data[review_data["NAME"].str.contains(search_n, case=False, na=False)]
    if search_sc:
        mc = review_data["CATEGORY"].str.contains(search_sc, case=False, na=False) if "CATEGORY" in review_data else False
        ms = review_data["SELLER_NAME"].str.contains(search_sc, case=False, na=False)
        review_data = review_data[mc | ms]

    ipp = st.session_state.grid_items_per_page
    total_pages = max(1, (len(review_data) + ipp - 1) // ipp)
    if st.session_state.grid_page >= total_pages: st.session_state.grid_page = 0

    # Pagination controls
    pg1, pg2, pg3 = st.columns([1,2,1])
    with pg1:
        if st.button("◀ Prev", disabled=st.session_state.grid_page == 0):
            st.session_state.grid_page -= 1
            st.rerun(scope="fragment")
    with pg2:
        st.markdown(f"**Page {st.session_state.grid_page+1} / {total_pages}**  ·  {len(review_data)} items")
    with pg3:
        if st.button("Next ▶", disabled=st.session_state.grid_page >= total_pages-1):
            st.session_state.grid_page += 1
            st.rerun(scope="fragment")

    page_start = st.session_state.grid_page * ipp
    page_data = review_data.iloc[page_start : page_start + ipp]

    # Image quality (unchanged)
    page_warnings = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(analyze_image_quality_cached, str(r.get("MAIN_IMAGE","")).strip()): str(r["PRODUCT_SET_SID"])
                   for _, r in page_data.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            warns = future.result()
            if warns: page_warnings[futures[future]] = warns

    cols_per_row = 3 if st.session_state.layout_mode == "centered" else 4

    grid_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  *{{margin:0;padding:0;box-sizing:border-box;font-family:system-ui,sans-serif;}}
  body{{background:#f9fafb;padding:16px;}}
  .ctrl-bar{{position:sticky;top:0;z-index:10;background:white;padding:12px 20px;border-radius:10px;
            box-shadow:0 2px 10px rgba(0,0,0,0.08);margin-bottom:20px;display:flex;justify-content:space-between;
            align-items:center;gap:16px;flex-wrap:wrap;}}
  .stats{{font-weight:600;color:#c2410c;}}
  .grid{{display:grid;grid-template-columns:repeat({cols_per_row},1fr);gap:16px;}}
  .card{{border:2px solid #e5e7eb;border-radius:12px;overflow:hidden;background:white;transition:all .18s;position:relative;}}
  .card.selected{{border-color:#16a34a;box-shadow:0 0 0 3px rgba(22,163,74,.15);}}
  .card.committed{{opacity:.54;filter:grayscale(.5);border-color:#9ca3af;}}
  .card.pending{{border-color:#ea580c;box-shadow:0 0 0 3px rgba(234,88,12,.2);}}
  .img-wrap{{position:relative;cursor:pointer;height:260px;background:#f3f4f6;}}
  img{{width:100%;height:100%;object-fit:contain;}}
  .tick{{position:absolute;bottom:10px;right:10px;width:32px;height:32px;background:rgba(0,0,0,.45);border-radius:50%;
         color:white;font-weight:bold;display:flex;align-items:center;justify-content:center;pointer-events:none;font-size:18px;}}
  .card.selected .tick{{background:#16a34a;}}
  .meta{{padding:12px;font-size:13.5px;line-height:1.4;}}
  .name{{font-weight:600;max-height:44px;overflow:hidden;}}
  .brand{{color:#c2410c;font-weight:700;margin:4px 0 3px;}}
  .cat,.seller{{color:#4b5563;font-size:12.5px;}}
  .status{{position:absolute;top:10px;right:10px;padding:5px 12px;border-radius:999px;font-size:11px;font-weight:700;color:white;}}
  .status-pending{{background:#ea580c;}}
  .status-rejected{{background:#dc2626;}}
  .actions{{padding:0 12px 14px;display:flex;gap:10px;}}
  button,select{{padding:7px 14px;border-radius:8px;border:none;cursor:pointer;font-weight:600;font-size:13px;}}
  .quick-btn{{background:#c2410c;color:white;flex:1;}}
  .more{{background:#e5e7eb;color:#1f2937;}}
  #batch-btn{{background:#c2410c;color:white;}}
  #confirm-batch{{background:#dc2626;color:white;font-weight:bold;padding:9px 18px;}}
  #confirm-batch:disabled{{opacity:.5;cursor:not-allowed;background:#9ca3af;}}
</style>
</head>
<body>

<div class="ctrl-bar">
  <div><span class="stats" id="sel">0 selected</span> <span style="margin-left:20px;color:#6b7280;">Page {st.session_state.grid_page + 1}</span></div>
  <div style="display:flex;gap:14px;align-items:center;flex-wrap:wrap;">
    <select id="reason">
      <option value="POOR_IMAGE">Poor Image</option>
      <option value="WRONG_CAT">Wrong Category</option>
      <option value="FAKE">Suspected Fake</option>
      <option value="PROHIBITED">Prohibited</option>
      <option value="BRAND">Restricted Brand</option>
    </select>
    <button id="batch-btn">Batch → Pending</button>
    <span id="pending-count" style="font-weight:600;color:#ea580c;">Pending: 0</span>
    <button id="confirm-batch" disabled>CONFIRM REJECT</button>
    <button onclick="deselectAll()">Deselect All</button>
  </div>
</div>

<div class="grid" id="grid"></div>

<script>
const CARDS = {json.dumps(cards)};
const COMMITTED = {json.dumps(st.session_state.committed)};
const PENDING_FROM_PY = {json.dumps(st.session_state.pending)};

let selected = {{}};
let pending  = {{...PENDING_FROM_PY}};

function $(id){{return document.getElementById(id);}}

function updateUI(){{
  $('sel').textContent = Object.keys(selected).length + " selected";
  $('pending-count').textContent = "Pending: " + Object.keys(pending).length;
  $('confirm-batch').disabled = Object.keys(pending).length === 0;
}}

function render(){{
  const html = CARDS.map(c => {{
    const sid = c.sid;
    let cls = 'card';
    let statusHtml = '';
    if (sid in COMMITTED) {{
      cls += ' committed';
      statusHtml = `<div class="status status-rejected">REJECTED</div>`;
    }} else if (sid in pending) {{
      cls += ' pending';
      statusHtml = `<div class="status status-pending">PENDING</div>`;
    }} else if (sid in selected) {{
      cls += ' selected';
    }}
    return `
      <div class="${{cls}}" id="c${{sid}}">
        ${{statusHtml}}
        <div class="img-wrap" onclick="toggle('${{sid}}')">
          <img src="${{c.img}}" loading="lazy" onerror="this.src='https://via.placeholder.com/260?text=×'">
          <div class="tick">✓</div>
        </div>
        <div class="meta">
          <div class="name">${{c.name}}</div>
          <div class="brand">${{c.brand}}</div>
          <div class="cat">${{c.cat}}</div>
          <div class="seller">${{c.seller}}</div>
        </div>
        <div class="actions">
          <button class="quick-btn" onclick="event.stopPropagation();quickReject('${{sid}}','POOR_IMAGE')">Poor Img</button>
          <select class="more" onchange="if(this.value){{event.stopPropagation();quickReject('${{sid}}',this.value);this.value='';}}">
            <option value="">More…</option>
            <option value="WRONG_CAT">Wrong Cat</option>
            <option value="FAKE">Fake</option>
            <option value="PROHIBITED">Prohibited</option>
            <option value="BRAND">Brand</option>
          </select>
        </div>
      </div>`;
  }}).join('');
  $('grid').innerHTML = html;
  updateUI();
}}

function toggle(sid){{
  if (sid in COMMITTED || sid in pending) return;
  if (sid in selected) delete selected[sid];
  else selected[sid] = true;
  render();
  updateUI();
}}

function quickReject(sid, reason){{
  if (sid in COMMITTED) return;
  delete selected[sid];
  pending[sid] = reason;
  render();
  updateUI();
}}

function batchToPending(){{
  const reason = $('reason').value;
  if (!reason) return alert("Select a reason first");
  Object.keys(selected).forEach(sid => {{
    if (!(sid in COMMITTED)) pending[sid] = reason;
  }});
  selected = {{}};
  render();
  updateUI();
}}

function confirmBatch(){{
  if (Object.keys(pending).length === 0) return;
  send('batch_confirm', pending);
  Object.assign(COMMITTED, pending);
  pending = {{}};
  render();
  updateUI();
}}

function deselectAll(){{
  selected = {{}};
  render();
}}

function send(type, payload){{
  window.parent.postMessage({{type:"grid_msg", action:type, payload}}, "*");
}}

$('batch-btn').onclick = batchToPending;
$('confirm-batch').onclick = confirmBatch;

render();
</script>
</body>
</html>
    """

    components.html(grid_html, height=1450, scrolling=True)

    # Process bridge messages (only batch_confirm for now)
    bridge_val = st.text_input("bridge", value=st.session_state.grid_bridge, key=f"bridge_{st.session_state.grid_msg_counter}",
                               label_visibility="collapsed")

    if bridge_val and bridge_val != st.session_state.grid_bridge:
        try:
            msg = json.loads(bridge_val)
            if msg.get("action") == "batch_confirm":
                payload = msg.get("payload", {})
                count = len(payload)
                for sid, reason in payload.items():
                    st.session_state.committed[sid] = reason
                st.session_state.pending.clear()
                st.toast(f"✅ Committed {count} rejections", icon="🟢")
                st.rerun(scope="app")
        except:
            pass
        finally:
            st.session_state.grid_bridge = bridge_val

# Keep your render_flag_expander, bulk_approve_dialog, exports, etc. unchanged

render_image_grid()
render_exports_section()  # your original export fragment
