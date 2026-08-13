"""
ui_components.py - All Streamlit UI rendering components, dialogs, and the image grid
"""

import base64
import concurrent.futures
import gc
import hashlib
import html as html_lib
import json
import logging
import re
import zipfile
from collections import OrderedDict
from io import BytesIO

import orjson
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image

from constants import (
    GRID_COLS, JUMIA_COLORS,
    SNEAKER_BRAND_ALIASES as _SNEAKER_ALIASES,
    # Shared with check_image_stretched: the grid badges the band the server
    # does NOT reject, so the two have to be defined against each other.
    ASPECT_ADVISORY_TALL as _ASPECT_ADVISORY_TALL,
    ASPECT_ADVISORY_WIDE as _ASPECT_ADVISORY_WIDE,
    ASPECT_REJECT_TALL as _ASPECT_REJECT_TALL,
    ASPECT_REJECT_WIDE as _ASPECT_REJECT_WIDE,
)
from design_tokens import (
    COLORS as DT,
    SEVERITY,
    SEVERITY_ORDER,
    flag_label,
    flag_severity,
    severity_sort_key,
)
from data_utils import (
    _get_image_from_zip,
    clean_category_code,
    df_hash,
    format_local_price,
    load_df_parquet,
    save_df_parquet,
    save_manual_decisions,
)
from export_utils import generate_smart_export, prepare_full_data_merged
from targeted_audit import targeted_audit_modal

logger = logging.getLogger(__name__)

# Securely encoded Base64 placeholder (No Image fallback)
_SVG_RAW = "<svg xmlns='http://www.w3.org/2000/svg' width='150' height='150'><rect width='150' height='150' fill='#f0f0f0'/><text x='75' y='75' text-anchor='middle' dominant-baseline='central' font-size='12' font-family='sans-serif' fill='#999'>No Image</text></svg>"
_NO_IMAGE_SVG = f"data:image/svg+xml;base64,{base64.b64encode(_SVG_RAW.encode('utf-8')).decode('utf-8')}"
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif")


PREFETCH_DISPLAY_COLUMNS = {
    "Wrong Category": [
        "Category_Check_Status",
        "Category_Check_Rejection_Reason",
        "Initial_Category_Path",
        "Suggested_Categories",
        "Top1_Category",
        "AI_Product_Caption",
        "Category_Match_Score",
        "Top1_Score",
    ],
    "Product Warranty": [
        "Warranty_Check_Status",
        "Warranty_Rejection_Reason",
        "product_warranty",
        "warranty_duration",
        "warranty_type",
        "warranty_address",
    ],
    "Missing COLOR": [
        "Color_Check_Status",
        "Color_Rejection_Reason",
        "Color_AI_Normalized",
        "color",
        "color_family",
    ],
    "Wrong Variation": [
        "Variation_Check_Status",
        "Variation_Rejection_Reason",
        "count_variations",
        "list_variations",
        "COUNT_VARIATIONS",
        "LIST_VARIATIONS",
    ],
    "BRAND name repeated in NAME": [
        "Brand_Image_Check_Status",
        "Brand_Image_Check_Reason",
        "Brand_Detected_On_Product",
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product_Name_Brand_Name_Status",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Poor images": [
        "Image_Quality_Check_Status",
        "Image_Quality_Check_Reason",
        "Image_Extraction_Status",
        "Image_Filename",
    ],
    "Missing Weight/Volume": [
        "Title_Language_Check_Status",
        "Title_Language_Check_Reason",
    ],
    "Duplicate product": ["Duplicate_Flag"],
    "FDA": ["FDA_Check_Status", "FDA_Rejection_Reason", "FDA"],
    "Category Check": [
        "Category_Check_Status",
        "Category_Check_Rejection_Reason",
        "Initial_Category_Path",
        "Suggested_Categories",
        "Top1_Category",
        "AI_Product_Caption",
        "Category_Match_Score",
        "Top1_Score",
    ],
    # ── Category Check sub-buckets (all inherit same display columns) ─────────
    "Category Check – Prohibited Category": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "Top1_Category",
        "AI_Product_Caption", "Category_Match_Score", "Top1_Score",
    ],
    "Category Check – Inactive Category": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "Top1_Category",
        "AI_Product_Caption", "Category_Match_Score", "Top1_Score",
    ],
    "Category Check – Replica Jersey / IP Violation": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Adult product listed under Baby category": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Baby/toddler listed under non-baby category": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Pet product listed under non-pet category": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Sexual Wellness Miscategory": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Fragrance/Perfume Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Clothing Subcategory Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Books Wrong Subcategory": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Electronics / Accessories Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Hair / Grooming Appliance Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Kitchen / Home Appliance Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Health / Supplement Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Food / Beverage Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Skincare Subcategory Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Lighting Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Bedding / Linen Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Tools / Hardware Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Medical Device Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    "Category Check – Other Mismatch": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "Suggested_Categories", "AI_Product_Caption",
    ],
    # ── AI API Errors bucket ─────────────────────────────────────────────────
    "Category Check – AI API Errors": [
        "Category_Check_Status", "Category_Check_Rejection_Reason",
        "Initial_Category_Path", "AI_Product_Caption",
    ],
    "Warranty Check": [
        "Warranty_Check_Status",
        "Warranty_Rejection_Reason",
        "product_warranty",
        "warranty_duration",
        "warranty_type",
        "warranty_address",
    ],
    "Color Check": [
        "Color_Check_Status",
        "Color_Rejection_Reason",
        "Color_AI_Normalized",
        "color",
    ],
    "Variation Check": [
        "Variation_Check_Status",
        "Variation_Rejection_Reason",
        "count_variations",
        "list_variations",
        "COUNT_VARIATIONS",
        "LIST_VARIATIONS",
    ],
    "Brand Image Check": [
        "Brand_Image_Check_Status",
        "Brand_Image_Check_Reason",
        "Brand_Detected_On_Product",
    ],
    "Product Name Brand Name – Brand Repeated In Title": [
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Product Name Brand Name – Inspired/Alternative Perfume Brand": [
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Product Name Brand Name – Generic/Placeholder Brand": [
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Product Name Brand Name – High-End Brand Counterfeit Suspected": [
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Product Name Brand Name – Other": [
        "Product Name_Brand Name_Status",
        "Product name_Brand name_rejection reason",
        "Product Name_Brand Name_Rejection_Reason",
    ],
    "Title Language Check - Not In English": [
        "Title_Language_Check_Status",
        "Title_Language_Check_Reason",
    ],
    "Title Language Check - Other": [
        "Title_Language_Check_Status",
        "Title_Language_Check_Reason",
    ],
    "Image Quality Check": [
        "Image_Quality_Check_Status",
        "Image_Quality_Check_Reason",
        "Image_Extraction_Status",
        "Image_Filename",
    ],
}


# Search, seller and category are per-flag and built inside each expander
# (see render_flag_expander). They lived briefly in one shared bar above the
# flags list, and then in the sidebar; both put them away from the rows they
# filter. A sticky shared bar would have solved that, but position:sticky
# does not work inside Streamlit's per-element wrappers — a sticky child's
# containing block is exactly its own height, so it has no room to travel.


def resolve_category_paths(df: pd.DataFrame, code_to_path: dict) -> pd.Series:
    """Full category path per row, without ever losing what we already had.

    Both call sites used to overwrite CATEGORY with a bare dict lookup on
    CATEGORY_CODE. When a code was missing from category_map.xlsx — which
    happens for ZIP uploads carrying newer or renamed categories — the flag
    table's lookup defaulted to "" and *erased* a perfectly good category,
    while the grid's defaulted to the raw code. That is why some ZIP products
    showed no full category at all.

    Order of preference: mapped path -> the CATEGORY already on the row ->
    the raw code. Blank only if there was genuinely nothing.
    """
    idx = df.index
    existing = (
        df["CATEGORY"].astype(str) if "CATEGORY" in df.columns
        else pd.Series("", index=idx)
    )
    if "CATEGORY_CODE" not in df.columns:
        return existing

    codes = df["CATEGORY_CODE"]
    mapped = codes.apply(
        lambda c: (code_to_path or {}).get(str(c).strip(), "") if pd.notna(c) else ""
    ).astype(str)

    out = mapped.where(mapped.str.strip().ne(""), existing)
    out = out.astype(str)
    # Still nothing? The code itself beats an empty cell — it is at least
    # something a reviewer can look up.
    raw = codes.astype(str)
    out = out.where(
        ~out.str.strip().str.lower().isin(["", "nan", "none"]),
        raw,
    )
    return out.where(~out.str.strip().str.lower().isin(["nan", "none"]), "")


def render_severity_group_header(level: str, flag_count: int, sku_count: int):
    """
    The band that opens each severity group in the flags list.

    The list used to be flat and alphabetical, so `[247] Wrong Category` and
    `[12] Restricted brands` carried identical weight — one is a cosmetic fix,
    the other is a legal blocker. Grouping puts the triage decision in the
    page structure rather than in the reviewer's head.
    """
    cfg = SEVERITY[level]
    st.html(
        f"""
    <div style="
      display:flex; align-items:baseline; gap:10px;
      margin:22px 0 8px; padding:0 0 6px;
      border-bottom:1px solid {DT['hairline']};">
      <span style="
        display:inline-block; width:3px; height:14px; border-radius:2px;
        background:{cfg['spine']}; align-self:center;"></span>
      <span style="
        font-size:12px; font-weight:700; letter-spacing:.08em;
        text-transform:uppercase; color:{cfg['color']};">{cfg['label']}</span>
      <span style="
        font-family:var(--font-mono); font-variant-numeric:tabular-nums slashed-zero;
        font-size:12px; font-weight:600; color:{DT['ink']};">{sku_count:,}</span>
      <span style="font-size:12px; color:{DT['ink_faint']};">
        SKUs across {flag_count} {"check" if flag_count == 1 else "checks"}
      </span>
      <span style="
        flex:1; text-align:right; font-size:11px;
        color:{DT['ink_faint']};">{cfg['blurb']}</span>
    </div>
    """
    )


def flag_pill_header(flag_name: str, count: int, is_zip: bool = False) -> str:
    """
    The header inside an opened flag expander.

    Colour was previously a per-flag lookup across five unrelated hues, which
    told a reviewer *which check fired* — something the label already says.
    It now comes from the severity ramp, so colour carries the one thing the
    label doesn't: how much trouble this is.
    """
    level = flag_severity(flag_name)
    cfg = SEVERITY[level]
    label = flag_label(flag_name)

    # The ZIP badge stays as it was: this is how a reviewer tells a result the
    # QC system already produced from one this tool computed itself, and the
    # blue reads as "different source" rather than as a severity. Only the
    # gradient's light end was darkened (#3b82f6 -> #2563eb) because white on
    # it was 3.7:1 at 10px; the darker blue keeps the same look at 5.2:1.
    zip_badge = (
        ' <span style="background:linear-gradient(135deg, #2563eb, #1d4ed8);'
        'color:white;border-radius:6px;padding:2px 8px;font-size:11px;'
        'font-weight:900;box-shadow:0 2px 4px rgba(0,0,0,0.1);'
        'margin-left:8px;">ZIP</span>'
        if is_zip
        else ""
    )

    return (
        f'<div style="display:flex;align-items:center;gap:10px;padding:4px 0 12px;'
        f'border-left:3px solid {cfg["spine"]};padding-left:12px;margin-bottom:4px;">'
        f'<span style="background:{cfg["wash"]};color:{cfg["color"]};'
        f'border:1px solid {cfg["spine"]}33;border-radius:6px;padding:3px 10px;'
        f'font-family:var(--font-mono);font-variant-numeric:tabular-nums slashed-zero;'
        f'font-size:13px;font-weight:600;">{count:,}</span>'
        f'<span style="font-size:15px;font-weight:600;color:{DT["ink"]};">{label}</span>'
        f'{zip_badge}</div>'
    )


def render_context_rail(
    country: str,
    flag_src: str = "",
    logo_html: str = "",
    file_count: int = 0,
    sku_count: int = 0,
    rejected_count: int = 0,
):
    """
    The persistent answer to "what am I looking at".

    Country drives which rule set runs — Kenya, Morocco and Nigeria reject
    different things — but it used to be visible only in the flag picker at
    the very top of the page. Two thousand pixels down, in a flag expander,
    nothing on screen said which market produced these rejections. That is a
    correctness risk, not a polish one, so the rail sticks.
    """
    flag_img = (
        f'<img src="{flag_src}" class="rail-flag" alt="">' if flag_src else ""
    )

    batch = ""
    if file_count or sku_count:
        parts = []
        if file_count:
            parts.append(
                f'<span class="rail-num">{file_count}</span> '
                f'{"file" if file_count == 1 else "files"}'
            )
        if sku_count:
            parts.append(f'<span class="rail-num">{sku_count:,}</span> SKUs')
        if rejected_count:
            parts.append(
                f'<span class="rail-num rail-rej">{rejected_count:,}</span> rejected'
            )
        batch = (
            '<span class="rail-sep"></span>'
            + '<span class="rail-dot"></span>'.join(
                f'<span class="rail-stat">{p}</span>' for p in parts
            )
        )

    st.html(
        f"""
    <style>
      .ctx-rail {{
        position: sticky; top: 0; z-index: 999;
        display: flex; align-items: center; gap: 10px;
        /* Tightened from 9px: the rail plus the sticky filter toolbar plus
           Streamlit's own 60px header were taking 156px — 20% of a 768px
           laptop screen — before any content. */
        padding: 6px 14px; margin: 0 0 10px 0;
        background: {DT['panel']};
        border: 1px solid {DT['hairline']};
        border-left: 3px solid {DT['accent']};
        border-radius: 8px;
        font-size: 13px; color: {DT['ink']};
      }}
      .rail-logo {{ height: 20px; width: auto; opacity: .9; }}
      .rail-logo-fallback {{ font-size: 20px; color: {DT['accent_text']}; }}
      .rail-flag {{
        width: 22px; height: 16px; border-radius: 2px;
        object-fit: cover; box-shadow: 0 0 0 1px rgba(0,0,0,.12);
      }}
      .rail-country {{
        font-weight: 600; letter-spacing: .04em;
        text-transform: uppercase; font-size: 12px;
      }}
      .rail-rules {{ color: {DT['ink_muted']}; font-size: 12px; }}
      .rail-sep {{ flex: 1; }}
      .rail-stat {{ color: {DT['ink_muted']}; font-size: 12px; white-space: nowrap; }}
      .rail-num {{
        font-family: var(--font-mono);
        font-variant-numeric: tabular-nums slashed-zero;
        font-weight: 600; color: {DT['ink']};
      }}
      .rail-rej {{ color: {DT['negative']}; }}
      .rail-dot {{
        display: inline-block; width: 3px; height: 3px; border-radius: 50%;
        background: {DT['ink_faint']}; margin: 0 10px; vertical-align: middle;
      }}
      @media (max-width: 760px) {{
        .ctx-rail {{ flex-wrap: wrap; }}
        .rail-sep {{ flex-basis: 100%; height: 0; }}
      }}
    </style>
    <div class="ctx-rail">
      {logo_html}
      {flag_img}
      <span class="rail-country">{country}</span>
      <span class="rail-rules">{country} rules</span>
      {batch}
    </div>
    """
    )


def render_kpi_bar(final_report: pd.DataFrame):
    total = len(final_report)
    approved = int((final_report["Status"] == "Approved").sum())
    rejected = int((final_report["Status"] == "Rejected").sum())
    zip_rej = (
        int(((final_report["Is_Zip"] == True) & (final_report["Status"] == "Rejected")).sum())
        if "Is_Zip" in final_report.columns
        else 0
    )
    pct = round(approved / total * 100, 1) if total else 0
    trend_color = (
        DT["positive"] if pct >= 70
        else SEVERITY["judgment"]["color"] if pct >= 40
        else DT["negative"]
    )
    trend_note = (
        "In the normal range" if pct >= 70
        else "Worth a closer look" if pct >= 40
        else "Most of this batch is failing"
    )

    st.html(
        f"""
    <style>
      .kpi-strip {{
        display:grid; grid-template-columns:repeat(4,minmax(0,1fr));
        gap:10px; margin-bottom:14px;
      }}
      .kpi-card {{
        background:{DT['panel']}; border:1px solid {DT['hairline']};
        border-radius:10px; padding:12px 16px;
      }}
      .kpi-label {{
        font-size:11px; font-weight:600; color:{DT['ink_muted']};
        letter-spacing:.07em; text-transform:uppercase;
      }}
      .kpi-value {{
        font-family:var(--font-mono);
        font-variant-numeric:tabular-nums slashed-zero;
        font-size:26px; font-weight:600; margin:2px 0 1px; line-height:1.15;
      }}
      .kpi-sub {{ font-size:11px; color:{DT['ink_faint']}; }}
      .kpi-bar {{
        height:3px; border-radius:99px; margin-top:9px;
        background:{DT['panel_sunken']}; overflow:hidden;
      }}
      .kpi-fill {{
        height:3px; border-radius:99px; background:{trend_color};
        width:{pct}%; transition:width .6s cubic-bezier(.4,0,.2,1);
      }}
      @media (max-width: 860px) {{
        .kpi-strip {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      }}
    </style>
    <div class="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-label">Total SKUs</div>
        <div class="kpi-value" style="color:{DT['ink']}">{total:,}</div>
        <div class="kpi-sub">Unique product sets</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Approved</div>
        <div class="kpi-value" style="color:{DT['positive']}">{approved:,}</div>
        <div class="kpi-sub">{pct}% of the batch</div>
        <div class="kpi-bar"><div class="kpi-fill"></div></div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Rejected</div>
        <div class="kpi-value" style="color:{DT['negative']}">{rejected:,}</div>
        <div class="kpi-sub">{zip_rej:,} from ZIP/prefetch</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Approval rate</div>
        <div class="kpi-value" style="color:{trend_color}">{pct}%</div>
        <div class="kpi-sub">{trend_note}</div>
        <div class="kpi-bar"><div class="kpi-fill"></div></div>
      </div>
    </div>
    """
    )


def render_summary_header(final_report: pd.DataFrame):
    render_kpi_bar(final_report)


def render_rejection_donut(final_report: pd.DataFrame):
    import plotly.graph_objects as go
    import plotly.express as px

    rej = final_report[final_report["Status"] == "Rejected"]
    if rej.empty:
        return
    counts = rej["FLAG"].str.replace(r" \(Prefetched\)", "", regex=True).value_counts()
    fig = go.Figure(
        go.Pie(
            labels=counts.index,
            values=counts.values,
            hole=0.55,
            textinfo="label+percent",
            textfont_size=11,
            marker=dict(
                colors=px.colors.qualitative.Pastel,
                line=dict(color="#fff", width=2),
            ),
            hovertemplate="<b>%{label}</b><br>%{value} SKUs (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        annotations=[
            dict(
                text=f"<b>{len(rej)}</b><br>rejected",
                x=0.5,
                y=0.5,
                font_size=14,
                showarrow=False,
            )
        ],
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        height=240,
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, width='stretch', config={"displayModeBar": False})


def _base_prefetched_title(title: str) -> str:
    # Strip (Prefetched) and then also strip known sub-bucket suffixes so that
    # sub-buckets like "Category Check – Prohibited Category" still resolve to
    # their exact key in PREFETCH_DISPLAY_COLUMNS (which is registered fully).
    t = str(title).replace("(Prefetched)", "").strip()
    # Remove trailing " ➡ ZIP" marker if present
    t = t.replace("\u26a1 ZIP", "").strip()
    return t


def _clean_reason_value(value) -> str:
    val = str(value).strip()
    return (
        ""
        if val.lower() in ("", "nan", "none", "null", "rejected", "approved", "skipped")
        else val
    )


def _prefetched_reason_for_row(title: str, row, fallback="No reason provided") -> str:
    base_title = _base_prefetched_title(title)
    for col in PREFETCH_DISPLAY_COLUMNS.get(base_title, []):
        if col in row.index:
            val = _clean_reason_value(row.get(col))
            if val:
                return val
    for col in row.index:
        col_l = str(col).lower()
        if "reason" in col_l and col_l != "reason":
            val = _clean_reason_value(row.get(col))
            if val:
                return val
    return fallback


def _t(key):
    from translations import get_translation

    return get_translation(st.session_state.get("ui_lang", "en"), key)


def _clear_flag_df_selection(title: str):
    ver_key = f"df_ver_{title}"
    st.session_state[ver_key] = st.session_state.get(ver_key, 0) + 1


def _normalize_sid_set(sids) -> set:
    return {str(s).strip() for s in sids if str(s).strip()}


# Counter for deferred GC — only force-collect every N cache clears (or explicitly)
_cache_clear_count: int = 0
_GC_COLLECT_INTERVAL: int = 50  # run gc.collect() at most once per 50 status changes


def _clear_result_caches(*, batch_gc: bool = False) -> None:
    """Clear display/export caches after a status change.

    ``gc.collect()`` is intentionally NOT called on every invocation — doing so
    forces Python to walk the entire object graph on every single row-level edit,
    which is expensive and blocks the event loop.  Instead we either:
    * Call it once per ``_GC_COLLECT_INTERVAL`` routine clears, OR
    * Call it immediately when ``batch_gc=True`` (e.g. after a large batch op).
    """
    global _cache_clear_count
    st.session_state.exports_cache.clear()
    st.session_state.display_df_cache.clear()
    st.session_state.pop("_grid_review_data_cache", None)
    st.session_state.pop("_grid_warm_urls", None)
    _cache_clear_count += 1
    if batch_gc or (_cache_clear_count % _GC_COLLECT_INTERVAL == 0):
        gc.collect()


def _get_norm_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a cached normalized (str-stripped) Series for *col* on *df*.

    The result is stored in a hidden column ``_norm_<col>`` so the expensive
    ``.astype(str).str.strip()`` scan is done at most once per unique DataFrame
    object, saving O(N × B) work in tight batch loops.
    """
    cache_col = f"_norm_{col}"
    if cache_col not in df.columns:
        df[cache_col] = df[col].astype(str).str.strip()
    return df[cache_col]


def _drop_sids_from_post_qc_results(sid_set: set) -> None:
    results = st.session_state.get("post_qc_results", {})
    if not isinstance(results, dict) or not sid_set:
        return
    for flag, df in list(results.items()):
        if (
            not isinstance(df, pd.DataFrame)
            or df.empty
            or "PRODUCT_SET_SID" not in df.columns
        ):
            continue
        mask = _get_norm_col(df, "PRODUCT_SET_SID").isin(sid_set)
        if mask.any():
            results[flag] = df.loc[~mask].copy()


def _add_sids_to_post_qc_results(sid_set: set, flag: str, comment: str = "") -> None:
    if not sid_set or not flag:
        return
    base_flag = str(flag).replace("(Prefetched)", "").strip()
    data = st.session_state.get("all_data_map", pd.DataFrame())
    if (
        not isinstance(data, pd.DataFrame)
        or data.empty
        or "PRODUCT_SET_SID" not in data.columns
    ):
        return
    base_rows = data[
        _get_norm_col(data, "PRODUCT_SET_SID").isin(sid_set)
    ].copy()
    if base_rows.empty:
        return
    base_rows["Comment_Detail"] = comment
    results = st.session_state.setdefault("post_qc_results", {})
    existing = results.get(base_flag)
    if isinstance(existing, pd.DataFrame) and not existing.empty:
        combined = pd.concat([existing, base_rows], ignore_index=True)
        combined = combined.drop_duplicates(subset=["PRODUCT_SET_SID"], keep="last")
        results[base_flag] = combined
    else:
        results[base_flag] = base_rows


def _get_image_maps(all_data):
    if "_image_maps" not in st.session_state or st.session_state.get("_image_maps_df_id") != id(all_data):
        if all_data is None or "PRODUCT_SET_SID" not in all_data.columns or "IMAGE1" not in all_data.columns:
            st.session_state["_image_maps"] = ({}, {})
        else:
            sid_to_img = dict(zip(all_data["PRODUCT_SET_SID"].astype(str).str.strip(), all_data["IMAGE1"]))
            img_to_sids = {}
            for sid, img in sid_to_img.items():
                if pd.isna(img) or not str(img).strip():
                    continue
                if img not in img_to_sids:
                    img_to_sids[img] = set()
                img_to_sids[img].add(sid)
            st.session_state["_image_maps"] = (sid_to_img, img_to_sids)
            st.session_state["_image_maps_df_id"] = id(all_data)
    return st.session_state["_image_maps"]

def _get_phash_maps(all_data):
    """(sid_to_phash, phash_to_sids) — the same photo, whoever listed it.

    img_to_sids above keys on the raw IMAGE1 value, so two sellers who upload
    the *same photo* under different URLs never match. A perceptual hash does
    match them: measured on a real batch, 11 groups covering 64 images (17%)
    shared an identical phash across different sellers and brands.

    Costs nothing to build. Every hash was already computed during
    _fetch_all_image_dimensions, in the same pass that fetched dimensions —
    this is a dict lookup per row, no decoding and no network. Cached against
    id(all_data) exactly like _get_image_maps, so it is built once per dataset
    and every later lookup is a dictionary hit.

    Exact equality only. Hamming-distance matching would be all-pairs, which
    is a billion comparisons on a 50k batch; exact match already catches the
    overwhelming majority.
    """
    if (
        "_phash_maps" in st.session_state
        and st.session_state.get("_phash_maps_df_id") == id(all_data)
    ):
        return st.session_state["_phash_maps"]

    hash_by_url = st.session_state.get("_image_phash_by_url") or {}
    if (
        all_data is None
        or not hash_by_url
        or "PRODUCT_SET_SID" not in getattr(all_data, "columns", [])
        or "MAIN_IMAGE" not in getattr(all_data, "columns", [])
    ):
        maps = ({}, {})
    else:
        sids = all_data["PRODUCT_SET_SID"].astype(str).str.strip()
        imgs = all_data["MAIN_IMAGE"].astype(str)
        sid_to_phash = {}
        phash_to_sids = {}
        for sid, img in zip(sids, imgs):
            ph = hash_by_url.get(img)
            if not ph:
                continue
            sid_to_phash[sid] = ph
            phash_to_sids.setdefault(ph, set()).add(sid)
        maps = (sid_to_phash, phash_to_sids)

    st.session_state["_phash_maps"] = maps
    st.session_state["_phash_maps_df_id"] = id(all_data)
    return maps


def find_same_photo_siblings(sids, exclude_rejected: bool = True) -> dict:
    """Other products carrying the identical photo of the ones just actioned.

    Returns {sid: [sibling_sid, ...]} for siblings outside `sids`. Already
    rejected siblings are dropped by default: re-rejecting them is a no-op
    that would still overwrite their FLAG and destroy the original reason.
    """
    all_data = st.session_state.get("all_data_map")
    sid_to_phash, phash_to_sids = _get_phash_maps(all_data)
    if not phash_to_sids:
        return {}

    src = {str(s).strip() for s in sids}

    # Candidates first — pure dict work, no pandas.
    candidates = {}
    for sid in src:
        ph = sid_to_phash.get(sid)
        if not ph:
            continue
        others = (phash_to_sids.get(ph) or set()) - src
        if others:
            candidates[sid] = others
    if not candidates:
        return {}

    # Only now touch final_report, and only for the handful of candidates.
    # Scanning it up front cost ~12ms per rejection on a 50k batch because
    # .astype(str).str.strip() ran over every row; _get_norm_col keeps that
    # normalisation cached on the frame instead of redoing it each call.
    rejected = set()
    if exclude_rejected:
        fr = st.session_state.get("final_report", pd.DataFrame())
        if isinstance(fr, pd.DataFrame) and not fr.empty and "Status" in fr.columns:
            _all_cand = {s for v in candidates.values() for s in v}
            _norm = _get_norm_col(fr, "ProductSetSid")
            _hit = _norm.isin(_all_cand) & (fr["Status"] == "Rejected")
            if _hit.any():
                rejected = set(_norm[_hit])

    out = {}
    for sid, others in candidates.items():
        remaining = others - rejected
        if remaining:
            out[sid] = sorted(remaining)
    return out


def render_sibling_prompt():
    """Offer to carry a judgement rejection across to identical listings.

    Deliberately a prompt and not an automatic cascade. An identical photo
    proves the same *picture*, not the same listing — variants legitimately
    share one, which is why the duplicate check keys on
    seller|phash|colour|size|model rather than the hash alone. For a category
    or colour call the other seller may have got it right, so a human decides.
    """
    _cascaded = st.session_state.pop("_sibling_cascaded", None)
    if _cascaded:
        st.info(
            f"Also rejected **{_cascaded['count']}** identical listing"
            f"{'s' if _cascaded['count'] != 1 else ''} from other sellers — "
            f"*{flag_label(_cascaded['flag'])}* applies to the product itself, "
            "not to who is selling it.",
            icon=":material/content_copy:",
        )

    prompt = st.session_state.get("_sibling_prompt")
    if not prompt or not prompt.get("sids"):
        return

    sids = prompt["sids"]
    label = flag_label(prompt["flag"])

    with st.container(border=True):
        st.markdown(
            f"**{len(sids)} other listing{'s' if len(sids) != 1 else ''} "
            f"use the same photo.**"
        )
        st.caption(
            f"You rejected {prompt['from_count']} product"
            f"{'s' if prompt['from_count'] != 1 else ''} for *{label}*. "
            "These are a different seller's listing of what looks like the same "
            "product. This one is a judgement call, so nothing has been applied "
            "— the other seller may have got it right."
        )

        _data = st.session_state.get("all_data_map", pd.DataFrame())
        if isinstance(_data, pd.DataFrame) and not _data.empty:
            _cols = [c for c in ("PRODUCT_SET_SID", "NAME", "BRAND", "SELLER_NAME", "CATEGORY")
                     if c in _data.columns]
            if _cols:
                _view = _data[_data["PRODUCT_SET_SID"].astype(str).str.strip().isin(sids)][_cols]
                if not _view.empty:
                    st.dataframe(_view.head(50), hide_index=True, width="stretch")

        c1, c2 = st.columns(2)
        if c1.button(
            f"Reject these too ({len(sids)})",
            key="btn_sibling_apply", type="primary", width="stretch",
            help=f"Applies the same reason — {label} — to all of them.",
        ):
            n = apply_status_change(
                sids,
                status="Rejected",
                reason=prompt.get("reason", ""),
                comment=prompt.get("comment", ""),
                flag=prompt["flag"],
                is_manual=True,
                # Already the cascade — do not look for siblings of siblings.
                propagate_siblings=False,
            )
            checkpoint_final_report()
            st.session_state.pop("_sibling_prompt", None)
            st.session_state.setdefault("main_toasts", []).append(
                (f"Rejected {n:,} matching listing(s) as {label}", ":material/content_copy:")
            )
            st.rerun()
        if c2.button("Leave them", key="btn_sibling_dismiss", width="stretch"):
            st.session_state.pop("_sibling_prompt", None)
            st.rerun()


def checkpoint_final_report(fr: pd.DataFrame = None) -> bool:
    """Persist the current report — including manual decisions — to disk.

    Manual approvals/rejections otherwise live only in st.session_state, and
    Streamlit discards a disconnected session after 2 minutes
    (MemorySessionStorage ttl_seconds = 2 * 60), so a dropped websocket loses
    every decision made since validation finished. The startup fast path reads
    this same `{sig_hash}_report.parquet`, so a reconnect resumes with the
    decisions intact instead of reverting to the post-validation state.

    Writes are atomic (temp file + os.replace) and failures are swallowed and
    logged by save_df_parquet — a checkpoint must never break the review UI.
    """
    if fr is None:
        fr = st.session_state.get("final_report")
    if not isinstance(fr, pd.DataFrame) or fr.empty:
        return False

    # Journal first: it is keyed on the uploaded file content alone, so it is
    # the copy that still resolves when sig_hash shifts (learning DB grows,
    # cache version bumps) and the report checkpoint below is orphaned.
    save_manual_decisions(
        st.session_state.get("last_processed_files"),
        fr,
        # Recorded alongside the journal so a later upload that merely adds a
        # file to this set can still find it. Without these the journal is
        # written exactly as before, just not discoverable.
        file_tokens=st.session_state.get("_process_file_tokens"),
        country=st.session_state.get("_process_country", ""),
    )

    sig = st.session_state.get("current_sig_hash")
    if not sig:
        return False
    save_df_parquet(fr, f"{sig}_report.parquet")
    return True


def apply_status_change(
    sids,
    *,
    status: str,
    reason: str = "",
    comment: str = "",
    flag: str = "",
    is_manual: bool = True,
    is_zip: bool = False,
    sync_quick_rejects: bool = True,
    propagate_siblings: bool = True,
) -> int:
    sid_set = _normalize_sid_set(sids)

    # ── Same photo, another seller ────────────────────────────────────────
    #
    # Rejecting a product on page 1 should not leave an identical listing from
    # a different seller untouched on page 6. What happens next depends on the
    # reason, because the two kinds do not travel the same way:
    #
    #   blocker   — restricted brand, prohibited, counterfeit. A property of
    #               the product itself, so it holds for whoever lists it.
    #               Cascades silently and reports what it did.
    #   judgement — wrong category, colour, title. Seller-specific: the other
    #               seller may well have filed the same product correctly.
    #               Never auto-applied; raised as a prompt to confirm.
    #
    # propagate_siblings=False is how the cascade calls back in without
    # re-triggering itself.
    _sibling_map = {}
    if propagate_siblings and status == "Rejected" and flag:
        try:
            _sibling_map = find_same_photo_siblings(sid_set)
        except Exception as _e:
            logger.warning("Sibling lookup failed: %s", _e)
            _sibling_map = {}

    _sibling_sids = sorted({s for v in _sibling_map.values() for s in v})
    _severity = flag_severity(flag) if _sibling_sids else None

    if _sibling_sids and _severity == "blocker":
        # Compliance reason — apply it to the identical listings too.
        sid_set.update(_sibling_sids)
        st.session_state["_sibling_cascaded"] = {
            "flag": flag,
            "count": len(_sibling_sids),
        }
    elif _sibling_sids:
        # Judgement reason — surface it, do not act on it.
        st.session_state["_sibling_prompt"] = {
            "flag": flag,
            "reason": reason,
            "comment": comment,
            "sids": _sibling_sids,
            "from_count": len(_sibling_map),
        }

    is_image_rej = status == "Rejected" and any(x in str(flag).lower() for x in ["image", "stretched", "blurry", "poor", "mismatch"])
    if is_image_rej:
        all_data = st.session_state.get("all_data_map")
        if all_data is not None and "PRODUCT_SET_SID" in all_data.columns and "IMAGE1" in all_data.columns:
            sid_to_img, img_to_sids = _get_image_maps(all_data)
            _target_images = {sid_to_img[sid] for sid in sid_set if sid in sid_to_img and pd.notna(sid_to_img[sid])}
            for img in _target_images:
                sid_set.update(img_to_sids.get(img, set()))

    fr = st.session_state.get("final_report", pd.DataFrame())
    if (
        not sid_set
        or not isinstance(fr, pd.DataFrame)
        or fr.empty
        or "ProductSetSid" not in fr.columns
    ):
        return 0

    mask = _get_norm_col(fr, "ProductSetSid").isin(sid_set)
    if not mask.any():
        return 0

    from datetime import datetime

    st.session_state["undo_snapshot"] = {
        "final_report": fr.copy(),
        "timestamp": datetime.now(),
    }

    fr.loc[mask, ["Status", "Reason", "Comment", "FLAG", "Is_Manual", "Is_Zip"]] = [
        status,
        reason,
        comment,
        flag,
        is_manual,
        is_zip,
    ]

    _drop_sids_from_post_qc_results(sid_set)
    if status == "Rejected" and flag:
        _add_sids_to_post_qc_results(sid_set, flag, comment)

    if sync_quick_rejects:
        if "quick_rejects" not in st.session_state:
            st.session_state["quick_rejects"] = {}
        for sid in sid_set:
            if status == "Rejected":
                st.session_state["quick_rejects"][sid] = flag or comment or reason
            else:
                st.session_state["quick_rejects"].pop(sid, None)

    st.session_state.data_version = st.session_state.get("data_version", 0) + 1
    _clear_result_caches(batch_gc=len(sid_set) >= 20)

    if len(sid_set) > 1:
        st.session_state["show_undo_toast"] = {
            "count": len(sid_set),
            "status": status,
            "time": datetime.now(),
        }

    # Every manual approve/reject funnels through here, so this is the one place
    # a checkpoint is needed to make decisions survive a disconnect.
    checkpoint_final_report(fr)

    return int(mask.sum())


# ── Override history ───────────────────────────────────────────────────────
# Approving a product out of one flag when other checks still reject it is a
# judgement call, and it is the one action here with no trace: the report ends
# up saying "Approved by User" with nothing to say what was waived. This
# records it so it can be reviewed and undone.
#
# It is also what stops the loop. Once a flag is recorded as overridden for a
# product, re-checking that product ignores it, so approving cannot bounce the
# same product between the same two flags forever.
def _override_log() -> dict:
    return st.session_state.setdefault("_flag_overrides", {})


def record_overrides(sid_to_flags: dict, approved_from: str = "") -> None:
    """Remember which flags a reviewer waived, per product."""
    if not sid_to_flags:
        return
    from datetime import datetime  # local, as elsewhere in this module

    log = _override_log()
    stamp = datetime.now()
    for sid, flags in sid_to_flags.items():
        entry = log.setdefault(str(sid).strip(), {"flags": [], "at": None, "from": ""})
        # approved_from is recorded alongside the others. The caller already
        # excludes it from the re-check, so this changes nothing today — but it
        # makes "an approved product cannot be re-flagged by anything it was
        # approved past" a property of the log rather than of every call site.
        for f in list(flags) + ([approved_from] if approved_from else []):
            if f and f not in entry["flags"]:
                entry["flags"].append(f)
        entry["at"] = stamp
        entry["from"] = approved_from or entry.get("from", "")


def overridden_flags(sid: str) -> list:
    return list(_override_log().get(str(sid).strip(), {}).get("flags", []))


def clear_overrides(sids=None) -> int:
    """Forget waivers, so the next approval re-checks those flags again."""
    log = _override_log()
    if sids is None:
        n = len(log)
        log.clear()
        return n
    n = 0
    for sid in _normalize_sid_set(sids):
        if log.pop(str(sid).strip(), None) is not None:
            n += 1
    return n


def render_override_history() -> None:
    """Show what was waived, and let it be undone.

    Every expander approval re-validates the selection with the current flag
    skipped, so "approve anyway" is a deliberate decision to clear other,
    still-failing checks. Without this the report records only "Approved by
    User" and the waived issues leave no trace at all.
    """
    log = _override_log()
    if not log:
        return
    _rows = []
    for sid, entry in log.items():
        _rows.append({
            "ProductSetSid": sid,
            "Issues cleared": ", ".join(entry.get("flags", [])),
            "Approved from": entry.get("from", ""),
            "When": entry["at"].strftime("%H:%M") if entry.get("at") else "",
        })
    _n_flags = sum(len(e.get("flags", [])) for e in log.values())
    with st.expander(
        f"Overridden issues — {len(log)} product(s), {_n_flags} check(s) cleared",
        expanded=False, icon=":material/gavel:",
    ):
        st.caption(
            "Approved despite other checks still failing. Reverting only forgets "
            "the waiver — it does not change the product's status, so the next "
            "approval will ask about these issues again."
        )
        st.dataframe(pd.DataFrame(_rows), hide_index=True, width="stretch")
        if st.button("Revert all waivers", key="clear_all_overrides",
                     help="Forget every waiver above."):
            _c = clear_overrides()
            st.toast(f"Reverted waivers on {_c} product(s)", icon=":material/undo:")
            st.rerun()


@st.dialog("Confirm Bulk Approval", icon=":material/check_circle:")
def bulk_approve_dialog(
    sids_to_process,
    title,
    subset_data,
    data_has_warranty_cols_check,
    support_files,
    country_validator,
    validation_runner,
):
    try:
        from category_matcher_engine import get_engine

        _CAT_MATCHER_AVAILABLE = True
    except ImportError:
        _CAT_MATCHER_AVAILABLE = False

    # ── Third stage: carry out the choice ─────────────────────────────────
    # Runs before anything is drawn, using the re-check computed on the first
    # pass. Deliberately does not re-validate: the answer is already known and
    # a second run would double the wait for no new information.
    _run = st.session_state.pop(f"_bulk_run_{title}", None)
    if _run:
        _mode = st.session_state.pop(f"_bulk_ack_{title}", "all")
        _still, _sids_all = _run["still"], _run["sids"]
        _clean = [s for s in _sids_all if s not in _still]
        _fm = (support_files or {}).get("flags_mapping", {})

        if _mode == "all":
            # Waiving the other issues is the whole point of this branch, so
            # it is logged against each product rather than left implicit in
            # an "Approved by User" row that mentions none of it.
            record_overrides(_still, approved_from=title)
            _n = apply_status_change(
                _sids_all, status="Approved", reason="", comment="",
                flag="Approved by User", is_manual=True, is_zip=False,
            )
            st.toast(f"Approved {_n} product(s), clearing {len(_still)} with other issues",
                     icon=":material/check_circle:")
        else:
            _n = apply_status_change(
                _clean, status="Approved", reason="", comment="",
                flag="Approved by User", is_manual=True, is_zip=False,
            ) if _clean else 0
            # The rest move to the issue that still applies, so the expander
            # they land in is the one that explains why they are still here.
            _moved = 0
            for _sid, _flags in _still.items():
                _nf = _flags[0]
                _info = _fm.get(_nf, {"reason": "1000007 - Other Reason", "en": _nf})
                apply_status_change(
                    [_sid], status="Rejected",
                    reason=_info.get("reason", "1000007 - Other Reason"),
                    comment=_info.get("en", _nf), flag=_nf,
                    is_manual=True, is_zip=False, propagate_siblings=False,
                )
                _moved += 1
            st.toast(f"Approved {_n}; {_moved} kept rejected under their remaining issue",
                     icon=":material/move_down:")
        st.session_state.data_version = st.session_state.get("data_version", 0) + 1
        st.rerun()

    st.warning(
        f"You are about to approve **{len(sids_to_process)}** items from `{title}`."
    )
    _preview_cols = [
        c
        for c in ["PRODUCT_SET_SID", "NAME", "BRAND", "SELLER_NAME"]
        if c in subset_data.columns
    ]
    _preview_df = subset_data[subset_data["PRODUCT_SET_SID"].isin(sids_to_process)][
        _preview_cols
    ].reset_index(drop=True)
    with st.expander(
        f"Preview {len(_preview_df)} item(s) to be approved",
        expanded=len(_preview_df) <= 10,
    ):
        st.dataframe(_preview_df, hide_index=True, width='stretch')

    # ── Second stage: other checks still reject some of these ──────────────
    # Rendered in place of the approve button rather than after a rerun, so the
    # validation result computed below is used as-is and never recomputed.
    _pending = st.session_state.get(f"_bulk_pending_{title}")
    if _pending:
        _still = _pending["still"]
        _sids_all = _pending["sids"]
        _clean_n = len(_sids_all) - len(_still)
        st.error(
            f"**{len(_still)} of {len(_sids_all)} products are still rejected by other checks.** "
            f"Approving them here would clear those too.",
            icon=":material/report:",
        )
        _by_flag: dict = {}
        for _s, _fl in _still.items():
            for _f in _fl:
                _by_flag[_f] = _by_flag.get(_f, 0) + 1
        st.dataframe(
            pd.DataFrame(
                sorted(_by_flag.items(), key=lambda kv: -kv[1]),
                columns=["Other issue", "Products"],
            ),
            hide_index=True, width="stretch",
        )
        _o1, _o2 = st.columns(2)
        if _o1.button(
            f"Approve all {len(_sids_all)} anyway",
            type="primary", width="stretch", key=f"bulk_all_{title}",
            help="Clears every issue listed above. Recorded so you can revert it.",
        ):
            st.session_state[f"_bulk_ack_{title}"] = "all"
            st.session_state[f"_bulk_run_{title}"] = _pending
            st.session_state.pop(f"_bulk_pending_{title}", None)
            st.rerun()
        if _o2.button(
            f"Approve only the {_clean_n} clean one(s)",
            width="stretch", key=f"bulk_clean_{title}",
            help="The rest stay rejected, under the issue that still applies.",
        ):
            st.session_state[f"_bulk_ack_{title}"] = "clean"
            st.session_state[f"_bulk_run_{title}"] = _pending
            st.session_state.pop(f"_bulk_pending_{title}", None)
            st.rerun()
        if st.button("Cancel", width="stretch", key=f"bulk_cancel_{title}"):
            st.session_state.pop(f"_bulk_pending_{title}", None)
            st.rerun()
        return

    if st.button(_t("approve_btn"), type="primary", width='stretch'):
        with st.spinner("Validating…"):
            _progress = st.progress(0, text="Running validation…")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _executor:
                data_hash = (
                    df_hash(subset_data) + country_validator.code + "_skip_" + title
                )
                _future = _executor.submit(
                    validation_runner,
                    data_hash,
                    subset_data,
                    support_files,
                    country_validator.code,
                    data_has_warranty_cols_check,
                    [title],
                )
                import time as _time

                _elapsed = 0
                while not _future.done():
                    _time.sleep(0.1)
                    _elapsed += 0.1
                    _progress.progress(
                        min(0.9, _elapsed / 10), text="Running validation…"
                    )
                _res = _future.result()
            _progress.progress(1.0, text="Done!")
            _progress.empty()
            sids_str = [str(sid).strip() for sid in sids_to_process]

            # This validation run already excluded `title`, so whatever it
            # still rejects is a genuinely different problem. The result used
            # to be discarded and every product approved regardless — the cost
            # was paid and the answer thrown away.
            _still: dict = {}
            try:
                _sub_results = _res[1] if isinstance(_res, tuple) and len(_res) > 1 else {}
                _wanted = {s for s in sids_str}
                for _flag, _df in (_sub_results or {}).items():
                    if _flag == title or _df is None or getattr(_df, "empty", True):
                        continue
                    if "PRODUCT_SET_SID" not in getattr(_df, "columns", []):
                        continue
                    _hit = _df["PRODUCT_SET_SID"].fillna("").astype(str).str.strip()
                    for _s in set(_hit) & _wanted:
                        if _flag in overridden_flags(_s):
                            continue
                        _still.setdefault(_s, []).append(_flag)
            except Exception:
                logger.exception("Re-check after bulk approval failed")
                _still = {}

            # One prompt for the whole batch, never one per product: at 200
            # items with 90 conflicts, per-product confirmation is unusable.
            if _still:
                st.session_state[f"_bulk_pending_{title}"] = {
                    "sids": sids_str, "still": _still, "title": title,
                }
                st.rerun()

            msg_moved = {}
            msg_approved = apply_status_change(
                sids_str,
                status="Approved",
                reason="",
                comment="",
                flag="Approved by User",
                is_manual=True,
                is_zip=False,
            )

            if msg_approved > 0:
                st.toast(
                    f"Approved {msg_approved} product(s) successfully!",
                    icon=":material/check_circle:",
                )
            if msg_moved:
                for f, c in msg_moved.items():
                    st.toast(
                        f"{c} product(s) moved to '{f}' (other issues found)",
                        icon=":material/note:",
                    )

            if title == "Wrong Category" and _CAT_MATCHER_AVAILABLE:
                try:
                    engine = get_engine()
                    if engine is not None:
                        learned = 0
                        for sid in sids_to_process:
                            row = subset_data[
                                subset_data["PRODUCT_SET_SID"].astype(str).str.strip()
                                == str(sid)
                            ]
                            if row.empty:
                                continue
                            name = str(row.iloc[0].get("NAME", "")).strip()
                            if not name:
                                continue
                            engine.set_compiled_rules(
                                st.session_state.get("compiled_json_rules", {})
                            )
                            predicted = engine.get_category_with_boost(name)
                            if predicted and predicted.lower() not in (
                                "nan",
                                "none",
                                "uncategorized",
                                "",
                            ):
                                engine.apply_learned_correction(
                                    name, predicted, auto_save=False
                                )
                                learned += 1
                        if learned:
                            engine.save_learning_db()
                            st.session_state.main_toasts.append(
                                f"Engine learned {learned} correction(s) from your approvals."
                            )
                except Exception as _le:
                    logger.warning("Wrong Category approval learning failed: %s", _le)

            if msg_approved > 0:
                st.session_state.main_toasts.append(
                    f"{msg_approved} items successfully Approved!"
                )
            for flag, count in msg_moved.items():
                st.session_state.main_toasts.append(
                    f"{count} items re-flagged as: {flag}"
                )

            st.session_state[f"exp_{title}"] = True
            _clear_flag_df_selection(title)
        st.rerun()


# Columns that may hold a product image, best first. Used when resolving
# previews for the flag tables.
_PREVIEW_IMG_COLS = [
    "image1", "MAIN_IMAGE_URL", "MAIN_IMAGE", "IMAGE_URL", "IMAGE1_ZIP",
    "MainImage", "Image", "IMAGE", "url", "Url", "IMAGE_URL_1", "Image1",
    "main_image",
]


def _resolve_preview_urls(df: pd.DataFrame) -> list:
    """Best image source per row, or None where there is nothing usable.

    Called only for rows actually being displayed, and only when previews are
    switched on. It used to run eagerly over every row of every flag bucket
    while building the display cache — base64-encoding every ZIP image up front
    — even though the toggle defaults to off and most buckets are never opened.
    """
    if df.empty:
        return []
    img_s = pd.Series("", index=df.index, dtype=object)
    for col in _PREVIEW_IMG_COLS:
        if col not in df.columns:
            continue
        empty = img_s.astype(str).str.strip() == ""
        if not empty.any():
            break
        candidate = df[col]
        usable = empty & candidate.notna() & (candidate.astype(str).str.strip() != "")
        img_s.loc[usable] = candidate[usable]

    names = df.get("NAME", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).values
    brands = df.get("BRAND", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).values

    out = []
    for name, brand, raw in zip(names, brands, img_s.fillna("").astype(str).values):
        zip_img = _get_image_from_zip(name, brand, raw)
        if zip_img:
            out.append(zip_img)
            continue
        u = str(raw).strip()
        if u.lower().startswith("http") or u.startswith("//") or u.startswith("data:image/"):
            if u.startswith("//"):
                u = "https:" + u
            elif u.lower().startswith("http://"):
                u = "https://" + u[7:]
            out.append(u)
        else:
            out.append(None)
    return out


@st.fragment
def render_flag_expander(
    title,
    df_flagged_sids,
    data,
    data_has_warranty_cols_check,
    support_files,
    country_validator,
    validation_runner,
):
    try:
        from category_matcher_engine import get_engine

        _CAT_MATCHER_AVAILABLE = True
    except ImportError:
        _CAT_MATCHER_AVAILABLE = False

    cache_key = f"display_df_{title}_{df_hash(data)}_prefetch_context_v3"
    base_display_cols = [
        "PRODUCT_SET_SID",
        "NAME",
        "Detected Issue",
        "BRAND",
        "CATEGORY",
        "COLOR",
        "GLOBAL_SALE_PRICE",
        "GLOBAL_PRICE",
        "PARENTSKU",
        "SELLER_NAME",
    ]
    current_display_cols = base_display_cols.copy()
    for col in PREFETCH_DISPLAY_COLUMNS.get(_base_prefetched_title(title), []):
        if col not in current_display_cols:
            current_display_cols.append(col)
    if title == "Wrong Variation":
        for col in ("COUNT_VARIATIONS", "LIST_VARIATIONS"):
            if col in data.columns:
                current_display_cols.append(col)

    if title == "Category Max Price Exceeded":
        current_display_cols.append("CAT_MAX_PRICE")

    if title == "Wrong Category":
        current_display_cols.append("AI Suggested Category")

    possible_img_cols = [
        "image1",
        "MAIN_IMAGE_URL",
        "MAIN_IMAGE",
        "IMAGE_URL",
        "IMAGE1_ZIP",
        "MainImage",
        "Image",
        "IMAGE",
        "url",
        "Url",
        "IMAGE_URL_1",
        "Image1",
    ]
    img_col = next((c for c in possible_img_cols if c in data.columns), None)
    if img_col and img_col not in current_display_cols:
        current_display_cols.append(img_col)

    if cache_key not in st.session_state.display_df_cache:
        # "Detected Issue" isn't a product column — it's final_report's
        # Comment (what a check found + which field, e.g. "Off-platform
        # contact detected — DESCRIPTION: 'whatsapp, 0712...'"), carried
        # through df_flagged_sids. Keep it in scope here even though the
        # data.columns filter below wouldn't otherwise include it — this
        # table previously showed the flag NAME but never the reason a
        # reviewer would need to act on it without opening the grid card.
        _extra_cols = [c for c in current_display_cols if c in data.columns or c == "Detected Issue"]
        if "CATEGORY_CODE" in data.columns and "CATEGORY_CODE" not in _extra_cols:
            _extra_cols.append("CATEGORY_CODE")

        if "PRODUCT_SET_SID" not in _extra_cols:
            _extra_cols.append("PRODUCT_SET_SID")

        if "Is_Zip" not in df_flagged_sids.columns:
            df_flagged_sids = df_flagged_sids.copy()
            df_flagged_sids["Is_Zip"] = False
        if "Is_Manual" not in df_flagged_sids.columns:
            df_flagged_sids = (
                df_flagged_sids.copy()
                if "Is_Zip" in df_flagged_sids.columns
                else df_flagged_sids
            )
            df_flagged_sids["Is_Manual"] = False
        if "Comment" not in df_flagged_sids.columns:
            df_flagged_sids = df_flagged_sids.copy()
            df_flagged_sids["Comment"] = ""
        df_display = pd.merge(
            df_flagged_sids[["ProductSetSid", "Is_Zip", "Comment"]].rename(
                columns={"Comment": "Detected Issue"}
            ),
            data,
            left_on="ProductSetSid",
            right_on="PRODUCT_SET_SID",
            how="left",
        )
        _di = df_display["Detected Issue"].astype(str).str.strip()
        df_display["Detected Issue"] = _di.where(
            ~_di.str.lower().isin(["nan", "none", "", "manual rejection", "rejected"]), ""
        )
        _extra_cols_cleaned = [c for c in _extra_cols if c in df_display.columns]
        if "IMAGE1_ZIP" in df_display.columns:
            _extra_cols_cleaned.append("IMAGE1_ZIP")

        df_display = df_display[list(dict.fromkeys(_extra_cols_cleaned + ["Is_Zip"]))]

        if (
            title == "Category Max Price Exceeded"
            and "CAT_MAX_PRICE" in df_flagged_sids.columns
        ):
            _cap_map = df_flagged_sids.set_index("ProductSetSid")[
                "CAT_MAX_PRICE"
            ].to_dict()
            sid_col = (
                "PRODUCT_SET_SID"
                if "PRODUCT_SET_SID" in df_display.columns
                else "ProductSetSid"
            )
            df_display["CAT_MAX_PRICE"] = df_display[sid_col].map(_cap_map)

        if (
            title == "Wrong Category"
            and "Suggested_Category" in df_flagged_sids.columns
        ):
            _sug_map = df_flagged_sids.set_index("ProductSetSid")[
                "Suggested_Category"
            ].to_dict()
            sid_col = (
                "PRODUCT_SET_SID"
                if "PRODUCT_SET_SID" in df_display.columns
                else "ProductSetSid"
            )
            df_display["AI Suggested Category"] = df_display[sid_col].map(_sug_map)

        _code_to_path = support_files.get("code_to_path", {})
        if _code_to_path and "CATEGORY_CODE" in df_display.columns:
            df_display["CATEGORY"] = resolve_category_paths(df_display, _code_to_path)
            df_display = df_display.drop(columns=["CATEGORY_CODE"])
        _final_cols = list(
            dict.fromkeys(
                [c for c in current_display_cols if c in df_display.columns]
                + ["Is_Zip"]
            )
        )
        df_display = df_display[_final_cols]
        if "NAME" in df_display.columns:
            df_display["NAME"] = df_display["NAME"].apply(
                lambda t: re.sub("<[^<]+?>", "", t) if isinstance(t, str) else t
            )

        if "GLOBAL_PRICE" in df_display.columns and "GLOBAL_SALE_PRICE" in df_display.columns:
            def _local_p(row):
                sp, rp = row.get("GLOBAL_SALE_PRICE"), row.get("GLOBAL_PRICE")
                val = sp if pd.notna(sp) and str(sp).strip() != "" else rp
                return format_local_price(val, country_validator.country)
            df_display.insert(
                df_display.columns.get_loc("GLOBAL_PRICE") + 1 if "GLOBAL_PRICE" in df_display.columns else len(df_display.columns),
                "Local Price",
                df_display.apply(_local_p, axis=1),
            )
            
        # Image previews are resolved lazily at render time (see
        # _resolve_preview_urls) so the display cache never carries base64
        # blobs for a toggle that is off by default.
        st.session_state.display_df_cache[cache_key] = df_display
    else:
        df_display = st.session_state.display_df_cache[cache_key]

    # Each flag's toggle owns its own state, keyed by flag.
    #
    # It previously passed `value=` from a single shared `show_table_images`
    # while keying the widget per flag. Those fight each other: a keyed widget's
    # own state wins after its first render, so `value=` never propagated — and
    # because every expander then wrote the shared variable back, whichever flag
    # rendered LAST silently overwrote it. Combined with render_flag_expander
    # being a fragment (toggling one flag reruns only that flag), the setting
    # appeared to work and then reverted on the next full rerun.
    show_table_images = st.toggle(
        "Show Image Previews",
        key=f"tg_img_{title}",
        help="Show a thumbnail for each row in this table.",
    )

    # All three controls sit with the rows they filter. They were briefly
    # hoisted into one bar above the whole flags list, which removed the
    # duplication but put them at the top of a very long page — so changing a
    # filter meant scrolling away from the table and back. A sticky bar would
    # have squared that circle, but position:sticky does not work inside
    # Streamlit's per-element wrappers (measured; it scrolls away like a
    # static element). Adjacent beats consolidated here.
    #
    # "_fs_"/"_ff_"/"_fc_" prefixes are what _reset_report_state sweeps, so a
    # new batch never opens with the last batch's filters hiding rows.
    _f1, _f2, _f3 = st.columns([2, 1.5, 1.5], gap="medium")
    with _f1:
        search_term = st.text_input(
            "Search these rows",
            placeholder="Name, brand or seller",
            icon=":material/search:",
            key=f"_fs_{title}",
        ).strip()
    with _f2:
        _seller_options = (
            sorted(df_display["SELLER_NAME"].dropna().astype(str).unique())
            if "SELLER_NAME" in df_display.columns else []
        )
        _sk = f"_ff_{title}"
        if _sk in st.session_state:
            _valid = [v for v in st.session_state[_sk] if v in _seller_options]
            if _valid != list(st.session_state[_sk]):
                st.session_state[_sk] = _valid
        seller_filter = st.multiselect(
            "Seller", _seller_options, key=_sk, placeholder="All sellers",
        )
    with _f3:
        _cat_options = (
            sorted(df_display["CATEGORY"].dropna().astype(str).unique())
            if "CATEGORY" in df_display.columns else []
        )
        _ck = f"_fc_{title}"
        if _ck in st.session_state:
            _valid = [v for v in st.session_state[_ck] if v in _cat_options]
            if _valid != list(st.session_state[_ck]):
                st.session_state[_ck] = _valid
        category_filter = st.multiselect(
            "Category", _cat_options, key=_ck, placeholder="All categories",
        )

    df_view = df_display.copy()
    if search_term:
        _search_cols = [
            c for c in ["NAME", "BRAND", "SELLER_NAME"] if c in df_view.columns
        ]
        if _search_cols:
            mask = (
                df_view[_search_cols]
                .apply(
                    lambda col: col.astype(str).str.contains(
                        search_term, case=False, na=False
                    )
                )
                .any(axis=1)
            )
            df_view = df_view[mask]
    if seller_filter and "SELLER_NAME" in df_view.columns:
        df_view = df_view[df_view["SELLER_NAME"].astype(str).isin(seller_filter)]
    if category_filter and "CATEGORY" in df_view.columns:
        df_view = df_view[df_view["CATEGORY"].astype(str).isin(category_filter)]
    if "CATEGORY" in df_view.columns:
        df_view = df_view.sort_values("CATEGORY", na_position="last")
    df_view = df_view.reset_index(drop=True)

    # The "Source" column is gone. ZIP provenance is already stated twice
    # around this table — in the expander label ("⚡ ZIP") and in the flag
    # header badge — so a whole column repeating it per row was spending the
    # narrowest resource on screen, horizontal space, on a constant.
    #
    # PRODUCT_SET_SID moves to the end and is pinned there: it is an
    # identifier you copy once you have decided something from the other
    # columns, not something you read first. Pinning keeps it reachable while
    # the row scrolls sideways.
    if "PRODUCT_SET_SID" in df_view.columns:
        _cols = [c for c in df_view.columns if c != "PRODUCT_SET_SID"]
        df_view = df_view[_cols + ["PRODUCT_SET_SID"]]

    if df_view.empty and not df_display.empty:
        st.info(
            f"None of the {len(df_display):,} products under this flag match "
            "the search above or the seller/category filters in the sidebar."
        )

    # Cap the table at one page of rows. style_rows below is a row-wise Python
    # Styler callback, so styling an unbounded df_view re-styled thousands of
    # rows on every keystroke/rerun in large flag buckets.
    _FLAG_TABLE_PAGE_SIZE = 500
    _total_view_rows = len(df_view)
    _flag_pg = 0
    if _total_view_rows > _FLAG_TABLE_PAGE_SIZE:
        _num_pages = (_total_view_rows + _FLAG_TABLE_PAGE_SIZE - 1) // _FLAG_TABLE_PAGE_SIZE
        # st.pagination (1.58) replaces a selectbox whose options were page
        # numbers and whose label carried the row range. A dropdown made you
        # open a menu to step one page; this is prev/next plus numbered pages
        # in one control. It is 1-based, the slice below is 0-based.
        _flag_pg = st.pagination(_num_pages, key=f"flag_tbl_pg_{title}") - 1
        _lo = _flag_pg * _FLAG_TABLE_PAGE_SIZE
        _hi = min(_lo + _FLAG_TABLE_PAGE_SIZE, _total_view_rows)
        st.caption(f"Rows {_lo + 1:,}–{_hi:,} of {_total_view_rows:,}")
        df_view = df_view.iloc[_lo:_hi].reset_index(drop=True)

    def style_rows(row):
        if row.get("Is_Zip"):
            return ["color: #ff4b4b; font-weight: 900;"] * len(row)
        return [""] * len(row)

    # Page number is part of the selection key so a positional selection made
    # on one page can never silently target different products on another page.
    _df_key = f"df_{title}_{st.session_state.get(f'df_ver_{title}', 0)}_p{_flag_pg}"

    _filters_active = bool(search_term or seller_filter)
    sel_all_col, sel_clear_col, _sel_spacer = st.columns([1, 1, 3])
    with sel_all_col:
        _sel_all_label = f"Select All ({len(df_view)} filtered)" if _filters_active else f"Select All ({len(df_view)})"
        if st.button(_sel_all_label, key=f"selall_{title}", disabled=df_view.empty, help="Select every row currently shown below"):
            st.session_state[_df_key] = {"selection": {"rows": list(range(len(df_view)))}}
    with sel_clear_col:
        if st.button("Clear Selection", key=f"selclear_{title}"):
            st.session_state[_df_key] = {"selection": {"rows": []}}
    df_kwargs = {
        "hide_index": True,
        "width": "stretch",
        "selection_mode": "multi-row",
        "on_select": "rerun",
        "key": _df_key,
    }
    if len(df_view) <= 2:
        df_kwargs["height"] = 150

    _col_cfg = {
            # Pinned and narrow, at the far right. NAME is deliberately not
            # pinned any more: it is the widest column in the table, so
            # freezing it ate most of the visible width and left very little
            # room for the columns a reviewer actually scrolls to reach.
            "PRODUCT_SET_SID": st.column_config.TextColumn(
                "SID", width="small", pinned=True,
                help="Product Set SID — pinned so it stays reachable while the row scrolls",
            ),
            "NAME": st.column_config.TextColumn("Product Name", width="medium"),
            "Detected Issue": st.column_config.TextColumn(
                "Detected Issue",
                width="large",
                help="What the check found and which field it was found in (e.g. NAME, DESCRIPTION)",
            ),
            "PARENTSKU": st.column_config.TextColumn(),
            "SELLER_SKU": st.column_config.TextColumn(),
            "SID": st.column_config.TextColumn(),
            "CATEGORY": st.column_config.TextColumn("Full Category", width="large"),
            "GLOBAL_SALE_PRICE": st.column_config.NumberColumn(
                "Sale Price (USD)", format="$%.2f"
            ),
            "GLOBAL_PRICE": st.column_config.NumberColumn(
                "Price (USD)", format="$%.2f"
            ),
            "Local Price": st.column_config.TextColumn(
                f"Local Price ({country_validator.country})"
            ),
            "CAT_MAX_PRICE": st.column_config.TextColumn(
                "Category Max Price",
                help="Maximum allowed price for this category in local currency",
            ),
            "AI Suggested Category": st.column_config.TextColumn(
                "AI Suggestion",
                width="large",
                help="AI predicted correct category path",
            ),
            "Is_Zip": None,
        }

    # The raw image column — image1 / MAIN_IMAGE / IMAGE1_ZIP, whichever the
    # file happens to carry — stays in the frame because _resolve_preview_urls
    # reads it, but it is a long opaque URL that tells a reviewer nothing and
    # costs a full column of width. Hidden. The visible thumbnail comes from
    # _Image_Preview below when the toggle is on.
    for _img_c in possible_img_cols:
        if _img_c in df_view.columns:
            _col_cfg[_img_c] = None

    # Resolve previews only now: previews are on, and df_view is already the
    # filtered, paginated slice, so this touches at most one page of rows
    # instead of every row in the bucket.
    if show_table_images:
        _preview_urls = _resolve_preview_urls(df_view)
        if any(u for u in _preview_urls):
            df_view = df_view.copy()
            df_view.insert(0, "_Image_Preview", _preview_urls)
            _col_cfg["_Image_Preview"] = st.column_config.ImageColumn(
                "Preview", help="Main image for this product"
            )
        else:
            st.caption(
                "No images available for these rows — the file has no usable "
                "image URL, and no matching image was found in the uploaded ZIP."
            )

    # A pandas Styler renders every cell as text, which stops ImageColumn from
    # drawing anything — so with previews on, the table goes through unstyled
    # and loses style_rows' red tint for ZIP rows. That costs nothing: the tint
    # only ever encoded Is_Zip, which the expander label ("⚡ ZIP") and the flag
    # header badge both state in text — the accessible way to carry it anyway.
    if show_table_images:
        event = st.dataframe(
            df_view,
            **df_kwargs,
            column_config=_col_cfg,
        )
    else:
        df_styled = df_view.style.apply(style_rows, axis=1)
        event = st.dataframe(
            df_styled,
            **df_kwargs,
            column_config=_col_cfg,
        )

    raw_selected = list(event.selection.rows)
    selected_indices = [i for i in raw_selected if i < len(df_view)]
    has_selection = len(selected_indices) > 0
    _sel_color = JUMIA_COLORS["primary_orange"] if has_selection else "#aaa"
    st.markdown(
        f"<div style='display:inline-block;background:{_sel_color};color:#fff;"
        f"padding:4px 14px;border-radius:9999px;font-size:13px;font-weight:700;"
        f"margin-bottom:8px;'>"
        f"{len(selected_indices)} / {len(df_view)} selected</div>",
        unsafe_allow_html=True,
    )

    _fm = support_files["flags_mapping"]
    _reason_options = [
        "Wrong Category", "Restricted brands", "Suspected Fake product", "Seller Not approved to sell Refurb",
        "Product Warranty", "Seller Approve to sell books", "Seller Approved to Sell Perfume", "Counterfeit Sneakers",
        "Suspected counterfeit Jerseys", "Prohibited products", "Unnecessary words in NAME", "Single-word NAME",
        "Generic BRAND Issues", "Fashion brand issues", "BRAND name repeated in NAME", "Wrong Variation",
        "Generic branded products with genuine brands", "Missing COLOR", "Missing Weight/Volume", "Incomplete Smartphone Name",
        "Duplicate product", "Poor images", "Image Stretched", "Image Blurry", "Image Mismatch",
        "Image Infringing", "Image Too Many things displayed", "Perfume Tester", "NG - Gift Card Seller",
        "NG - Books Seller", "NG - TV Brand Seller", "NG - HP Toners Seller", "NG - Apple Seller",
        "NG - Xmas Tree Seller", "NG - Rice Brand Seller", "NG - Powerbank Capacity", "Discount too high",
        "Category Max Price Exceeded", "Suspicious Discount", "Color Mismatch", "FDA", "Category Check", "Warranty Check",
        "Color Check", "Variation Check", "Brand Image Check", "Title Language Check", "Image Quality Check",
        "Product Name Brand Name – Brand Repeated In Title",
        "Product Name Brand Name – Inspired/Alternative Perfume Brand",
        "Product Name Brand Name – Generic/Placeholder Brand",
        "Product Name Brand Name – High-End Brand Counterfeit Suspected",
        "Product Name Brand Name – Other",
        "Title Language Check - Not In English",
        "Title Language Check - Other",
        "Other Reason (Custom)",
    ]

    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button(
            _t("approve_btn"),
            key=f"approve_sel_{title}",
            type="primary",
            width='stretch',
            disabled=not has_selection,
        ):
            sids_to_process = df_view.iloc[selected_indices]["PRODUCT_SET_SID"].tolist()
            subset = data[data["PRODUCT_SET_SID"].isin(sids_to_process)]
            _clear_flag_df_selection(title)
            bulk_approve_dialog(
                sids_to_process,
                title,
                subset,
                data_has_warranty_cols_check,
                support_files,
                country_validator,
                validation_runner,
            )

    with btn_col2:
        pop_ver = st.session_state.get(f"pop_ver_{title}", 0)
        popover_key = f"popover_rej_{title}_{pop_ver}"
        with st.popover(
            _t("reject_as"),
            width='stretch',
            disabled=not has_selection,
            key=popover_key,
        ):
            chosen_reason = st.selectbox(
                "Reason",
                _reason_options,
                key=f"rej_reason_dd_{title}",
                label_visibility="collapsed",
            )
            _cmt_lang = (
                "fr" if st.session_state.get("selected_country") == "Morocco" else "en"
            )

            if chosen_reason == "Other Reason (Custom)":
                custom_comment = st.text_area(
                    "Custom comment",
                    placeholder="Type your rejection reason here...",
                    key=f"custom_comment_{title}",
                    height=80,
                )
                if st.button(
                    "Apply",
                    key=f"apply_custom_{title}",
                    type="primary",
                    width='stretch',
                    disabled=not has_selection,
                ):
                    to_reject = df_view.iloc[selected_indices][
                        "PRODUCT_SET_SID"
                    ].tolist()
                    final_comment = (
                        custom_comment.strip()
                        if custom_comment.strip()
                        else "Other Reason"
                    )
                    apply_status_change(
                        to_reject,
                        status="Rejected",
                        reason="1000007 - Other Reason",
                        comment=final_comment,
                        flag="Other Reason (Custom)",
                        is_manual=True,
                        is_zip=False,
                    )
                    st.session_state.main_toasts.append(
                        f"{len(to_reject)} items rejected with custom reason."
                    )
                    st.session_state[f"exp_{title}"] = True
                    _clear_flag_df_selection(title)
                    st.session_state[f"pop_ver_{title}"] = pop_ver + 1
                    st.rerun()
            else:
                _rinfo = _fm.get(
                    chosen_reason,
                    {"reason": "1000007 - Other Reason", "en": chosen_reason},
                )
                _rcode = _rinfo["reason"]
                _rcmt = _rinfo.get(_cmt_lang, _rinfo.get("en"))
                st.info(f"**Seller message:** {_rcmt}", icon=":material/chat:")
                if st.button(
                    "Apply",
                    key=f"apply_dd_{title}",
                    type="primary",
                    width='stretch',
                    disabled=not has_selection,
                ):
                    to_reject = df_view.iloc[selected_indices][
                        "PRODUCT_SET_SID"
                    ].tolist()
                    apply_status_change(
                        to_reject,
                        status="Rejected",
                        reason=_rcode,
                        comment=_rcmt,
                        flag=chosen_reason,
                        is_manual=True,
                        is_zip=False,
                    )
                    st.session_state.main_toasts.append(
                        f"{len(to_reject)} items rejected as '{chosen_reason}'."
                    )

                    if (
                        chosen_reason == "Wrong Category"
                        and title != "Wrong Category"
                        and _CAT_MATCHER_AVAILABLE
                    ):
                        try:
                            engine = get_engine()
                            _cats = support_files.get("categories_names_list", [])
                            if engine is not None and _cats:
                                if not engine._tfidf_built:
                                    engine.build_tfidf_index(_cats)
                                learned = 0
                                for sid in to_reject:
                                    prod_row = data[
                                        data["PRODUCT_SET_SID"].astype(str).str.strip()
                                        == str(sid)
                                    ]
                                    if prod_row.empty:
                                        continue
                                    name = str(prod_row.iloc[0].get("NAME", "")).strip()
                                    if not name:
                                        continue
                                    engine.set_compiled_rules(
                                        st.session_state.get("compiled_json_rules", {})
                                    )
                                    predicted = engine.get_category_with_boost(name)
                                    if predicted and predicted.lower() not in (
                                        "nan",
                                        "none",
                                        "uncategorized",
                                        "",
                                    ):
                                        engine.apply_learned_correction(
                                            name, predicted, auto_save=False
                                        )
                                        learned += 1
                                if learned:
                                    engine.save_learning_db()
                                    st.session_state.main_toasts.append(
                                        f"Engine noted {learned} missed Wrong Category item(s)."
                                    )
                        except Exception as _le:
                            logger.warning(
                                "Wrong Category manual rejection learning failed: %s",
                                _le,
                            )

                    st.session_state[f"exp_{title}"] = True
                    _clear_flag_df_selection(title)
                    st.session_state[f"pop_ver_{title}"] = pop_ver + 1
                    st.rerun()


# Lives here, not in streamlit_app: importing the entry script from this
# module re-executes it, and Streamlit then re-runs every widget in it —
# duplicate element IDs and fragment errors on every grid render.
def sneaker_brand_claims(
    data: pd.DataFrame,
    sneaker_category_codes: list,
    sneaker_sensitive_brands: list,
) -> dict:
    """SID -> the protected sneaker brand a listing claims, for the review grid.

    Deliberately not a verdict. Most listings that claim Nike are textually
    identical to a genuine Nike listing — "ADIDAS Campus" with BRAND=ADIDAS is
    what the real thing looks like — so no rule can separate them and the
    photograph is the only evidence left. This marks them for a human to
    decide in the visual grid instead of guessing.

    Sub-brands resolve to the parent, so an Airmax listing reads as Nike.
    """
    if data.empty or not sneaker_category_codes or not sneaker_sensitive_brands:
        return {}
    _cats = {clean_category_code(c) for c in sneaker_category_codes}
    if "_cat_clean" in data.columns:
        _in = data["_cat_clean"].isin(_cats)
    elif "CATEGORY_CODE" in data.columns:
        _in = data["CATEGORY_CODE"].map(clean_category_code).isin(_cats)
    else:
        return {}
    d = data[_in]
    if d.empty or "PRODUCT_SET_SID" not in d.columns:
        return {}

    _terms = {}
    for b in sneaker_sensitive_brands:
        b = str(b).strip().lower()
        if b:
            _terms[b] = _SNEAKER_ALIASES.get(b, b)
    for a, parent in _SNEAKER_ALIASES.items():
        _terms.setdefault(a, parent)

    _tag_re = re.compile(r"<[^>]+>")
    _text = (
        d.get("NAME", pd.Series("", index=d.index)).astype(str) + " "
        + d.get("BRAND", pd.Series("", index=d.index)).astype(str) + " "
        + d.get("DESCRIPTION", pd.Series("", index=d.index)).astype(str) + " "
        + d.get("SHORT_DESCRIPTION", pd.Series("", index=d.index)).astype(str)
    ).str.replace(_tag_re, " ", regex=True).str.lower()

    out: dict = {}
    # Longest first so "air jordan" wins over "jordan" and the claim reads as
    # the most specific thing the listing actually said.
    for t in sorted(_terms, key=len, reverse=True):
        rx = re.compile(r"(?<!\w)" + re.escape(t) + r"(?!\w)", re.IGNORECASE)
        hit = _text.str.contains(rx, na=False)
        if not hit.any():
            continue
        for sid in d.loc[hit, "PRODUCT_SET_SID"].astype(str):
            out.setdefault(sid, _terms[t].title())
    return out


def build_fast_grid_html(
    page_data,
    flags_mapping,
    country,
    page_warnings,
    rejected_state,
    cols_per_row,
    poor_img_sids=None,
    prefetch_urls=None,
    scroll_to_top=False,
    show_images=True,
    support_files=None,
    curr_sort="",
    curr_flag="",
    items_per_page=50,
):
    if support_files is None: support_files = {}

    from translations import get_translation
    lang = st.session_state.get("ui_lang", "en")

    def _t(key): return get_translation(lang, key)

    O = JUMIA_COLORS["primary_orange"]
    G = JUMIA_COLORS["success_green"]
    R = JUMIA_COLORS["jumia_red"]
    # Orange splits by role inside the grid too: O is a fill and never sits
    # under white text (2.43:1), OT is the darkened text-safe orange (4.7:1),
    # and INK is what goes on top of an orange fill (7.2:1).
    OT = DT["accent_text"]
    INK = DT["ink_on_accent"]

    # Inline stroke icons. currentColor so they inherit each button's state
    # colours, and no braces anywhere so they drop into the f-string below
    # without escaping. Every icon-only control carries a title + aria-label —
    # an icon on its own is not a label.
    _SVG = ('<svg viewBox="0 0 24 24" width="15" height="15" fill="none" '
            'stroke="currentColor" stroke-width="2.1" stroke-linecap="round" '
            'stroke-linejoin="round" aria-hidden="true" focusable="false">')
    _ICON_UNDO = _SVG + '<path d="M3 8v6h6"/><path d="M3.5 14a9 9 0 1 0 2.2-9.5L3 8"/></svg>'
    _ICON_GLOBE = (_SVG + '<circle cx="12" cy="12" r="9"/><path d="M3 12h18"/>'
                   '<path d="M12 3c2.5 2.4 3.8 5.6 3.8 9S14.5 18.6 12 21c-2.5-2.4-3.8-5.6-3.8-9S9.5 5.4 12 3z"/></svg>')
    _ICON_TOP = _SVG + '<path d="M12 19V5"/><path d="M6 11l6-6 6 6"/></svg>'
    _ICON_BOTTOM = _SVG + '<path d="M12 5v14"/><path d="M6 13l6 6 6-6"/></svg>'
    def _js_json(v):
        return orjson.dumps(v).decode("utf-8").replace("</", "<\\/")

    committed_json = _js_json(rejected_state)
    poor_img_sids_json = _js_json(list(poor_img_sids or []))
    prefetch_json = _js_json(prefetch_urls or [])
    html_dir = "rtl" if st.session_state.get("ui_lang") == "ar" else "ltr"

    labels_dict = {
        "poor_img":       _t("poor_img"),
        "img_stretched":  _t("img_stretched"),
        "img_blurry":     _t("img_blurry"),
        "img_mismatch":   _t("img_mismatch"),
        "img_infringing": _t("img_infringing"),
        "img_too_many":   _t("img_too_many"),
        "wrong_cat":      _t("wrong_cat"),
        "fake_prod":      _t("fake_prod"),
        "restr_brand":    _t("restr_brand"),
        "wrong_brand":    _t("wrong_brand"),
        "prohibited":     _t("prohibited"),
        "missing_color":  _t("missing_color"),
        "other_custom":   _t("other_custom"),
        "more_options":   _t("more_options"),
        "undo":           _t("undo"),
        "approve":        _t("approve_btn"),
        "clear_sel":      _t("clear_sel"),
        "items_pending":  _t("items_pending"),
        "batch_reject":   _t("batch_reject"),
        "select_all":     _t("select_all"),
        "deselect_all":   _t("deselect_all"),
        "rejected":       str(_t("rejected") or "REJECTED").upper(),
        "sort_by_issue":      _t("sort_by_issue"),
        "most_flagged":       _t("most_flagged"),
        "no_issue_first":     _t("no_issue_first"),
        "filter_by_flag":     _t("filter_by_flag"),
        "all_rejected":       _t("all_rejected"),
        "clean_no_flags":     _t("clean_no_flags"),
        "grp_image":          _t("grp_image"),
        "grp_qc_flags":       _t("grp_qc_flags"),
        "grp_prefetch":       _t("grp_prefetch"),
        "sort_low_res":       _t("sort_low_res"),
        "sort_tall":          _t("sort_tall"),
        "sort_wide":          _t("sort_wide"),
        "sort_broken":        _t("sort_broken"),
        "sort_wrong_cat":     _t("sort_wrong_cat"),
        "sort_restr_brand":   _t("sort_restr_brand"),
        "sort_fake":          _t("sort_fake"),
        "sort_missing_color": _t("sort_missing_color"),
        "sort_warranty":      _t("sort_warranty"),
        "sort_duplicates":    _t("sort_duplicates"),
        "filter_brand_ocr":   _t("filter_brand_ocr"),
        "filter_duplicates":  _t("filter_duplicates"),
        "filter_manual":      _t("filter_manual"),
        "filter_color_mis":   _t("filter_color_mis"),
        "filter_brand_name":  _t("filter_brand_name"),
        "filter_unneeded":    _t("filter_unneeded"),
        "filter_prohibited":  _t("filter_prohibited"),
        "custom_reason_title": _t("custom_reason_title"),
        "custom_reason_ph":    _t("custom_reason_ph"),
        "custom_apply":        _t("custom_apply"),
        "custom_cancel":       _t("custom_cancel"),
        "search_grid":     _t("search_grid"),
        "products_label":  _t("products_label"),
        "dark_mode":       _t("dark_mode"),
    }
    labels_json = _js_json(labels_dict)

    _PLACEHOLDER_SVG = (
        "data:image/svg+xml;utf8,"
        "<svg xmlns='http://www.w3.org/2000/svg' width='300' height='180' viewBox='0 0 300 180'>"
        "<defs><linearGradient id='g' x1='0%' y1='0%' x2='100%' y2='100%'><stop offset='0%' stop-color='%23FFF8F2'/><stop offset='100%' stop-color='%23FFEFE5'/></linearGradient></defs>"
        "<rect width='300' height='180' rx='12' fill='url(%23g)'/>"
        "<text x='150' y='80' text-anchor='middle' font-family='sans-serif' font-size='34' "
        "font-weight='800' fill='%23FF8800' letter-spacing='-1'>JUMIA</text>"
        "<text x='150' y='110' text-anchor='middle' font-family='sans-serif' font-size='14' "
        "font-weight='600' fill='%23FF8800' opacity='0.7'>Loading...</text>"
        "</svg>"
    )

    # Persist across reruns/page-turns instead of rebuilding per call —
    # previously this was a fresh {} every call, so ZIP image lookups for
    # already-seen (name, brand, img_url) combos were redone on every
    # page turn / rerun instead of being cached.
    #
    # Bounded: entries are base64 data URIs (~1.33x the source image) and this
    # is a SECOND copy of what zip_image_store already holds, so leaving it
    # unbounded meant a long paging session carried two full unbounded copies of
    # every image it had ever displayed.
    if "_zip_img_cache" not in st.session_state:
        st.session_state._zip_img_cache = {}
    _zip_img_cache: dict = st.session_state._zip_img_cache
    _ZIP_IMG_CACHE_MAX = 300

    _zip_index_ss = st.session_state.get("_zip_sid_index")
    _zip_sid_set = set()
    if _zip_index_ss is not None and not _zip_index_ss.empty:
        _zip_sid_set = set(_zip_index_ss.index.astype(str).tolist())
    _fr_ss = st.session_state.get("final_report", pd.DataFrame())
    if not _zip_sid_set:
        if not _fr_ss.empty and "Is_Zip" in _fr_ss.columns and "ProductSetSid" in _fr_ss.columns:
            _zip_sid_set = set(
                _fr_ss[_fr_ss["Is_Zip"] == True]["ProductSetSid"].astype(str).tolist()
            )

    _zip_override_map = {}
    if not _fr_ss.empty and "zip_override" in _fr_ss.columns and "ProductSetSid" in _fr_ss.columns:
        _zip_override_map = _fr_ss.set_index("ProductSetSid")["zip_override"].fillna("").to_dict()

    # Products our OWN duplicate check rejected, as opposed to the ones the
    # ZIP merely observed.
    #
    # The two disagree in a way worth seeing: the ZIP marks every member of a
    # group (4,904 on a KE batch) and rejects none of them; our check rejects
    # all but one per group (4,606) so the product stays sellable. Showing them
    # in one colour would hide that, and hide the 37 our check found that the
    # ZIP missed.
    _sys_dup_sids = set()
    if not _fr_ss.empty and {"ProductSetSid", "FLAG"}.issubset(_fr_ss.columns):
        _sys_dup_sids = set(
            _fr_ss.loc[
                _fr_ss["FLAG"].astype(str).str.strip() == "Duplicate product",
                "ProductSetSid",
            ].astype(str).str.strip()
        )

    # Perfumes whose title names somebody's model rather than their house.
    #
    # Written by check_suspected_fake_perfume, which rejects on a house NAME
    # and defers on a model name — see the comment there. Read from session
    # state because importing streamlit_app here re-executes the entry script.
    _perfume_claims = st.session_state.get("_perfume_model_claims") or {}

    # Sneakers claiming a protected brand.
    #
    # Not a verdict — most are textually identical to the genuine article
    # ("ADIDAS Campus" with BRAND=ADIDAS is what a real one looks like), so
    # the photograph is the only evidence that separates them. The badge puts
    # them in front of a reviewer here instead of guessing in a rule.
    #
    # Computed once per page rather than per card: it scans four text columns.
    _brand_claims = {}
    try:
        _sfx = support_files or {}
        _brand_claims = sneaker_brand_claims(
            page_data,
            _sfx.get("sneaker_category_codes", []),
            _sfx.get("sneaker_sensitive_brands", []),
        )
    except Exception:
        logger.exception("could not resolve sneaker brand claims for the grid")

    cards_data = []
    for row in page_data.to_dict("records"):
        sid = str(row.get("PRODUCT_SET_SID", "")).strip()
        img_url = str(row.get("MAIN_IMAGE", "")).strip()
        if img_url.startswith("http"):
            img_url = img_url.replace("http://", "https://", 1)
        elif img_url:
            name = str(row.get("NAME", "")).strip()
            brand = str(row.get("BRAND", "")).strip()
            _zip_cache_key = (name, brand, img_url)
            if _zip_cache_key not in _zip_img_cache:
                _zip_img_cache[_zip_cache_key] = _get_image_from_zip(name, brand, img_url)
                if len(_zip_img_cache) > _ZIP_IMG_CACHE_MAX:
                    for _stale in list(_zip_img_cache.keys())[
                        : len(_zip_img_cache) - _ZIP_IMG_CACHE_MAX
                    ]:
                        _zip_img_cache.pop(_stale, None)
            img_data = _zip_img_cache[_zip_cache_key]
            if img_data:
                img_url = img_data
            else:
                img_url = ""

        if (not img_url or img_url == "") and "IMAGE1_ZIP" in row:
            _fallback = str(row.get("IMAGE1_ZIP", "")).strip()
            if _fallback.startswith("http"):
                img_url = _fallback.replace("http://", "https://", 1)

        # A missing sale price means the product simply is not on sale, so the
        # regular price is the one to show.
        #
        # The fallback was already here but only caught a real NaN. These
        # columns arrive from CSVs read with dtype=str, so an absent sale price
        # is the literal string "nan" — which passes pd.notna() and is not
        # empty, so it was taken as the price and rendered as "NaN". On a real
        # Kenya batch that was 2,955 of 4,059 cards showing NaN while the
        # regular price sat unused in the next column.
        def _usable_price(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            return None if s.lower() in ("", "nan", "none", "null", "n/a", "-") else v

        usd_val = _usable_price(row.get("GLOBAL_SALE_PRICE"))
        if usd_val is None:
            usd_val = _usable_price(row.get("GLOBAL_PRICE"))
        # Empty, not "NaN", when there is genuinely no price: the grid hides the
        # badge entirely for an empty string, which is better than a badge
        # announcing a broken number.
        price_str = ""
        if usd_val is not None:
            price_str = format_local_price(
                usd_val, st.session_state.get("selected_country", "Kenya")
            ) or ""

        color_val = str(row.get("COLOR", "")).strip()
        if color_val.lower() in ("nan", "none", "null"):
            color_val = ""

        color_ai = str(row.get("Color_AI_Normalized", "")).strip()
        if color_ai.lower() in ("nan", "none", "null", ""):
            color_ai = ""
        if not color_ai and _zip_index_ss is not None and sid in _zip_index_ss.index:
            _zr = _zip_index_ss.loc[sid]
            if hasattr(_zr, "iloc") and len(getattr(_zr, "shape", (1,))) == 2:
                _zr = _zr.iloc[0]
            _zai = str(_zr.get("Color_AI_Normalized", "")).strip()
            if _zai.lower() not in ("nan", "none", "null", ""):
                color_ai = _zai
        # Several colours declared in one field — "white,black", "Red / Blue".
        # One listing carrying several colours is the usual sign of a single
        # photo showing several products, which is the thing the image review
        # is looking for, so the card says so rather than leaving it to be
        # spotted in the Color line.
        _multi_colour = ""
        if color_val:
            _parts = [p.strip() for p in re.split(r"[,/;|]| and ", color_val) if p.strip()]
            if len(_parts) > 1:
                _multi_colour = ", ".join(_parts[:4]) + ("…" if len(_parts) > 4 else "")

        color_mismatch = ""
        if color_ai and color_val:
            # "multicolor" and "multicolour" are the same word, and the AI and
            # the seller do not agree on which spelling to use. Comparing them
            # literally reported a mismatch between a word and itself.
            def _norm_colour(s: str) -> str:
                s = s.lower().replace(" ", "")
                s = s.replace("colours", "color").replace("colour", "color")
                s = s.replace("colors", "color")
                return s

            _ai_n = _norm_colour(color_ai)
            _dec_n = _norm_colour(color_val)
            _both_multi = _ai_n.startswith("multi") and _dec_n.startswith("multi")
            if (not _both_multi
                    and _ai_n != _dec_n and _ai_n not in _dec_n and _dec_n not in _ai_n):
                color_mismatch = f"AI: '{color_ai}' vs declared: '{color_val}'"
        elif color_ai and not color_val:
            color_mismatch = f"AI detected color: '{color_ai}' (none declared)"

        dup_raw = str(row.get("Duplicate_Flag", "")).strip()
        is_duplicate = dup_raw.lower() not in ("", "nan", "none", "false")

        mr_raw = str(row.get("Manual_Review", "")).strip().lower()
        qc_skip = str(row.get("QC_Skip_Reason", "")).strip()
        if qc_skip.lower() in ("nan", "none", "null", ""):
            qc_skip = ""

        is_manual_review = (
            mr_raw in ("true", "1", "yes") 
            or "Manual review" in page_warnings.get(sid, [])
            or bool(qc_skip)
        )

        cat_reason = str(row.get("Category_Check_Rejection_Reason", "")).strip()
        if cat_reason.lower() in ("nan", "none", "rejected", ""):
            cat_reason = ""
        suggested_cats_raw = str(row.get("Suggested_Categories", "")).strip()
        suggested_cat = ""
        if suggested_cats_raw and suggested_cats_raw.lower() not in ("nan", "none", ""):
            first_pipe = suggested_cats_raw.split("|")[0]
            suggested_cat = re.sub(r"\s*\(\d+%\)\s*$", "", first_pipe).strip()

        ai_caption = str(row.get("AI_Product_Caption", "")).strip()
        if ai_caption.lower() in ("nan", "none", ""):
            ai_caption = ""

        # Generic reason snippet for any flag with a Comment (Brand Image
        # Mismatch, Off-Platform Contact, etc). cat_reason keeps its own
        # dedicated field/styling for Category Check since it's richer
        # (includes suggested category); this is the fallback for everything
        # else so a reviewer isn't left guessing why a card is flagged.
        flag_comment = str(row.get("Comment", "")).strip()
        if flag_comment.lower() in ("nan", "none", "", "manual rejection", "rejected"):
            flag_comment = ""

        cards_data.append(
            {
                "sid": sid,
                "img": img_url if show_images else _PLACEHOLDER_SVG,
                "name": str(row.get("NAME", "")),
                "brand": str(row.get("BRAND", "Unknown Brand")),
                "cat": str(row.get("Initial_Category_Path", row.get("CATEGORY", "Unknown Category"))),
                "seller": str(row.get("SELLER_NAME", "Unknown Seller")),
                "color": color_val,
                "brand_detected": str(
                    row.get(
                        "Brand_Detected_On_Product",
                        row.get(
                            "brand_detected_on_product", row.get("Detected_Brand", "")
                        ),
                    )
                ).strip()
                if (
                    pd.notna(row.get("Brand_Detected_On_Product"))
                    and str(row.get("Brand_Detected_On_Product")).lower()
                    not in ("nan", "none")
                )
                or (
                    pd.notna(row.get("brand_detected_on_product"))
                    and str(row.get("brand_detected_on_product")).lower()
                    not in ("nan", "none")
                )
                else "",
                "warnings": page_warnings.get(sid, []),
                "price": price_str,
                "data_name": str(row.get("NAME", "")).replace('"', "&quot;"),
                "data_brand": str(row.get("BRAND", "")).replace('"', "&quot;"),
                "data_sid": sid,
                "data_cat": str(row.get("CATEGORY", "")).replace('"', "&quot;"),
                "is_duplicate": is_duplicate,
                "sys_duplicate": sid in _sys_dup_sids,
                "is_manual_review": is_manual_review,
                "qc_skip_reason": qc_skip,
                "color_mismatch": color_mismatch,
                "multi_colour": _multi_colour,
                "color_ai": color_ai,
                "cat_reason": cat_reason,
                "suggested_cat": suggested_cat,
                "ai_caption": ai_caption,
                "flag_comment": flag_comment,
                "is_zip": sid in _zip_sid_set,
                "zip_override": str(_zip_override_map.get(sid, "")),
                "brand_claim": _brand_claims.get(sid, ""),
                "perfume_claim": str(_perfume_claims.get(sid, "")),
            }
        )

    # Split embedded ZIP images out of the card records.
    #
    # ZIP-sourced images are base64 data URIs: an 80KB photo becomes ~107KB of
    # text. Left inside `cards`, a 500-card page is a ~52MB payload that was
    # re-sent on every rerun and duplicated by the retry loop — the direct cause
    # of the browser hanging and the websocket dropping. The heavy bytes now
    # travel in their own map, sent only when the card set actually changes,
    # while `cards` itself stays small enough to resend freely.
    images_map = {}
    for _c in cards_data:
        _img = _c.get("img") or ""
        if _img.startswith("data:"):
            images_map[_c["sid"]] = _img
            _c["img"] = ""  # the iframe resolves it from IMAGES[sid]

    cards_json = orjson.dumps(cards_data).decode("utf-8").replace("</", "<\\/")
    images_json = orjson.dumps(images_map).decode("utf-8").replace("</", "<\\/")
    # Identifies this exact card set. The iframe already knew how to skip a
    # re-render when the signature is unchanged (_lastCardsSig), but Python
    # never sent one, so every sync forced a full re-render.
    cards_sig = hashlib.md5(cards_json.encode("utf-8")).hexdigest()

    # Restores into #card-grid rather than the window: the document no longer
    # scrolls, the grid container does.
    scroll_js = """
        window.addEventListener('DOMContentLoaded', function() {
            var savedInnerScroll = sessionStorage.getItem('__inner_iframe_scroll__');
            if (savedInnerScroll) {
                setTimeout(function() {
                    var g = document.getElementById('card-grid');
                    var y = parseInt(savedInnerScroll, 10) || 0;
                    if (g) { g.scrollTop = y; }
                    else { window.scrollTo({top: y, behavior: 'instant'}); }
                }, 50);
            }
        });
    """


    def _sel(val, curr): return "selected" if val == curr else ""
    sort_html = f'''
  <select class="reason-sel sort-sel" id="sort-sel-top" onchange="sendMsg('grid_sort_issue', this.value)" style="max-width:170px;" title="{labels_dict.get('sort_by_issue', 'Sort')}">
    <option value="" {_sel('', curr_sort)}>{labels_dict.get('sort_by_issue', 'Sort')}</option>
    <option value="most_flagged" {_sel('most_flagged', curr_sort)}>{labels_dict.get('most_flagged', 'Most Flagged')}</option>
    <option value="no_issue" {_sel('no_issue', curr_sort)}>{labels_dict.get('no_issue_first', 'No Issue')}</option>
    <option disabled>── {labels_dict.get('grp_image', 'Image')} ──</option>
    <option value="low_res" {_sel('low_res', curr_sort)}>{labels_dict.get('sort_low_res', 'Low Res')}</option>
    <option value="tall" {_sel('tall', curr_sort)}>{labels_dict.get('sort_tall', 'Tall')}</option>
    <option value="wide" {_sel('wide', curr_sort)}>{labels_dict.get('sort_wide', 'Wide')}</option>
    <option value="broken" {_sel('broken', curr_sort)}>{labels_dict.get('sort_broken', 'Broken')}</option>
    <option disabled>── {labels_dict.get('grp_qc_flags', 'QC')} ──</option>
    <option value="Wrong Category" {_sel('Wrong Category', curr_sort)}>{labels_dict.get('sort_wrong_cat', 'Wrong Cat')}</option>
    <option value="Restricted brands" {_sel('Restricted brands', curr_sort)}>{labels_dict.get('sort_restr_brand', 'Restricted')}</option>
    <option value="Suspected Fake product" {_sel('Suspected Fake product', curr_sort)}>{labels_dict.get('sort_fake', 'Fake')}</option>
    <option value="Missing COLOR" {_sel('Missing COLOR', curr_sort)}>{labels_dict.get('sort_missing_color', 'Color')}</option>
    <option value="Product Warranty" {_sel('Product Warranty', curr_sort)}>{labels_dict.get('sort_warranty', 'Warranty')}</option>
    <option value="Duplicate product" {_sel('Duplicate product', curr_sort)}>{labels_dict.get('sort_duplicates', 'Duplicate')}</option>
    <option disabled>── {labels_dict.get('grp_prefetch', 'Prefetch')} ──</option>
    <option value="Category Check" {_sel('Category Check', curr_sort)}>Category Check</option>
    <option value="Warranty Check" {_sel('Warranty Check', curr_sort)}>Warranty Check</option>
    <option value="FDA" {_sel('FDA', curr_sort)}>FDA</option>
    <option value="Color Check" {_sel('Color Check', curr_sort)}>Color Check</option>
    <option value="Variation Check" {_sel('Variation Check', curr_sort)}>Variation Check</option>
    <option value="Brand Image Check" {_sel('Brand Image Check', curr_sort)}>Brand Image Check</option>
    <option value="Title Language Check" {_sel('Title Language Check', curr_sort)}>Title Language Check</option>
    <option value="Image Quality Check" {_sel('Image Quality Check', curr_sort)}>Image Quality Check</option>
  </select>
'''
    filter_html = f'''
  <select class="reason-sel sort-sel" id="filter-sel-top" onchange="sendMsg('grid_filter_flag', this.value)" style="max-width:180px;" title="{labels_dict.get('filter_by_flag', 'Filter')}">
    <option value="" {_sel('', curr_flag)}>{labels_dict.get('filter_by_flag', 'Filter')}</option>
    <option value="brand_ocr" {_sel('brand_ocr', curr_flag)}>{labels_dict.get('filter_brand_ocr', 'Brand OCR')}</option>
    <option value="duplicates" {_sel('duplicates', curr_flag)}>{labels_dict.get('filter_duplicates', 'Duplicates')}</option>
    <option value="manual_review" {_sel('manual_review', curr_flag)}>{labels_dict.get('filter_manual', 'Manual Review')}</option>
    <option value="color_mismatch" {_sel('color_mismatch', curr_flag)}>{labels_dict.get('filter_color_mis', 'Color Mis')}</option>
    <option value="committed" {_sel('committed', curr_flag)}>{labels_dict.get('all_rejected', 'All Rejected')}</option>
    <option value="no_flags" {_sel('no_flags', curr_flag)}>{labels_dict.get('clean_no_flags', 'Clean')}</option>
    <option disabled>── {labels_dict.get('grp_qc_flags', 'QC')} ──</option>
    <option value="Wrong Category" {_sel('Wrong Category', curr_flag)}>{labels_dict.get('sort_wrong_cat', 'Wrong Cat')}</option>
    <option value="Restricted brands" {_sel('Restricted brands', curr_flag)}>{labels_dict.get('sort_restr_brand', 'Restricted')}</option>
    <option value="Suspected Fake product" {_sel('Suspected Fake product', curr_flag)}>{labels_dict.get('sort_fake', 'Fake')}</option>
    <option value="Missing COLOR" {_sel('Missing COLOR', curr_flag)}>{labels_dict.get('sort_missing_color', 'Color')}</option>
    <option value="Product Warranty" {_sel('Product Warranty', curr_flag)}>{labels_dict.get('sort_warranty', 'Warranty')}</option>
    <option value="Duplicate product" {_sel('Duplicate product', curr_flag)}>{labels_dict.get('sort_duplicates', 'Duplicate')}</option>
    <option value="BRAND name repeated in NAME" {_sel('BRAND name repeated in NAME', curr_flag)}>{labels_dict.get('filter_brand_name', 'Brand Name')}</option>
    <option value="Unnecessary words" {_sel('Unnecessary words', curr_flag)}>{labels_dict.get('filter_unneeded', 'Unneeded')}</option>
    <option value="Prohibited Words" {_sel('Prohibited Words', curr_flag)}>{labels_dict.get('filter_prohibited', 'Prohibited')}</option>
    <option value="Brand Image Mismatch" {_sel('Brand Image Mismatch', curr_flag)}>Brand Image Mismatch</option>
    <option value="Off-Platform Contact" {_sel('Off-Platform Contact', curr_flag)}>Off-Platform Contact</option>
    <option value="Specs Inconsistency" {_sel('Specs Inconsistency', curr_flag)}>Specs Inconsistency</option>
    <option disabled>── {labels_dict.get('grp_prefetch', 'Prefetch')} ──</option>
    <option value="Category Check" {_sel('Category Check', curr_flag)}>Category Check</option>
    <option value="Warranty Check" {_sel('Warranty Check', curr_flag)}>Warranty Check</option>
    <option value="FDA" {_sel('FDA', curr_flag)}>FDA</option>
    <option value="Color Check" {_sel('Color Check', curr_flag)}>Color Check</option>
    <option value="Variation Check" {_sel('Variation Check', curr_flag)}>Variation Check</option>
    <option value="Brand Image Check" {_sel('Brand Image Check', curr_flag)}>Brand Image Check</option>
    <option value="Title Language Check" {_sel('Title Language Check', curr_flag)}>Title Language Check</option>
    <option value="Image Quality Check" {_sel('Image Quality Check', curr_flag)}>Image Quality Check</option>
    <option value="Product Name Brand Name – Brand Repeated In Title" {_sel('Product Name Brand Name – Brand Repeated In Title', curr_flag)}>Name/Brand: Brand Repeated</option>
    <option value="Product Name Brand Name – Inspired/Alternative Perfume Brand" {_sel('Product Name Brand Name – Inspired/Alternative Perfume Brand', curr_flag)}>Name/Brand: Perfume Brand</option>
    <option value="Product Name Brand Name – Generic/Placeholder Brand" {_sel('Product Name Brand Name – Generic/Placeholder Brand', curr_flag)}>Name/Brand: Generic Brand</option>
    <option value="Product Name Brand Name – High-End Brand Counterfeit Suspected" {_sel('Product Name Brand Name – High-End Brand Counterfeit Suspected', curr_flag)}>Name/Brand: High-End Counterfeit</option>
    <option value="Product Name Brand Name – Other" {_sel('Product Name Brand Name – Other', curr_flag)}>Name/Brand: Other</option>
    <option value="Title Language Check - Not In English" {_sel('Title Language Check - Not In English', curr_flag)}>Title: Not in English</option>
    <option value="Title Language Check - Other" {_sel('Title Language Check - Other', curr_flag)}>Title: Language Other</option>
  </select>
'''

    # Additional rejection reasons that already have a reason-code/comment
    # backing them (REASON_MAP + flags_mapping) but were previously missing
    # from the manual reject dropdown — reviewers could not select them by
    # hand even though the app fully supports them end-to-end.
    extra_reason_options_html = '''
    <optgroup label="Category / Brand">
    <option value="REJECT_BRAND_IN_NAME">Brand Repeated In Name</option>
    <option value="REJECT_GENERIC_BRAND">Generic Brand Issues</option>
    <option value="REJECT_FASHION_BRAND">Fashion Brand Issues</option>
    <option value="REJECT_FAKE_PERFUME">Suspected Fake Perfume</option>
    <option value="REJECT_BRAND_MISMATCH">Brand Image Mismatch</option>
    </optgroup>
    <optgroup label="Compliance">
    <option value="REJECT_NSFW">Adult / NSFW Content</option>
    </optgroup>
    <optgroup label="Seller Approval">
    <option value="REJECT_REFURB">Seller Not Approved (Refurb)</option>
    <option value="REJECT_BOOKS_SELLER">Seller Not Approved (Books)</option>
    <option value="REJECT_PERFUME_SELLER">Seller Not Approved (Perfume)</option>
    <option value="REJECT_PERFUME_TESTER">Perfume Tester</option>
    <option value="REJECT_SNEAKERS">Counterfeit Sneakers</option>
    <option value="REJECT_JERSEYS">Counterfeit Jerseys</option>
    </optgroup>
    <optgroup label="Content / Title">
    <option value="REJECT_UNNECESSARY_WORDS">Unnecessary Words in Name</option>
    <option value="REJECT_SINGLE_WORD">Single-word Name</option>
    <option value="REJECT_SMARTPHONE_NAME">Incomplete Smartphone Name</option>
    <option value="REJECT_SPECS_INCONSISTENCY">Specs Inconsistency</option>
    <option value="REJECT_WEIGHT_VOL">Missing Weight/Volume</option>
    <option value="REJECT_TITLE_LANG">Title Not in English</option>
    <option value="REJECT_OFFPLATFORM">Off-Platform Contact</option>
    </optgroup>
    <optgroup label="Other">
    <option value="REJECT_WARRANTY">Product Warranty</option>
    <option value="REJECT_VARIATION">Wrong Variation</option>
    <option value="REJECT_SUSPICIOUS_DISCOUNT">Suspicious Discount</option>
    </optgroup>
'''

    # 3 and 4 exist because 5 was the floor and it did not fit a laptop: the
    # main content area is ~1066px at 1366x768 and ~980px at 1280x800, so five
    # columns leaves a 186-203px card holding an image, four metadata lines and
    # its buttons. Four columns gives 236-257px, three gives 318-347px.
    _cols_btns_parts = []
    for _n in [3, 4, 5, 6, 7]:
        _active = _n == cols_per_row
        _border = "var(--accent)" if _active else "var(--border)"
        _bg = "var(--accent)" if _active else "#fff"
        # Dark ink on the orange fill, not white: white-on-#F68B1E is 2.43:1
        # and fails AA, the same fix applied to every other control.
        _color = "var(--ink-on-accent)" if _active else "var(--text)"
        _title = (
            " title='Wide mode + 500 per page'" if _n >= 6
            else " title='Fewer, larger cards — better on a laptop'" if _n <= 4
            else ""
        )
        _cols_btns_parts.append(
            f'<button onclick="sendMsg(\'grid_cols_per_row\', {_n})"{_title} '
            f'aria-label="Show {_n} cards per row" '
            f'style="min-width:26px;padding:3px 9px;font-size:12px;font-weight:700;'
            f'border-radius:5px;border:1px solid {_border};background:{_bg};'
            f'color:{_color};cursor:pointer;line-height:1.5;">{_n}</button>'
        )
    _cols_btns = "".join(_cols_btns_parts)

    _grid_sync_data = (
        committed_json, poor_img_sids_json, prefetch_json, cards_json,
        images_json, cards_sig,
    )
    _html_str = f"""<!DOCTYPE html>
<html dir="{html_dir}">
<head>
<meta charset="utf-8">
<meta name="referrer" content="no-referrer">
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@500;600;700&display=swap');
  :root {{
    --bg: {DT["surface"]};
    --card: {DT["panel"]};
    --text: {DT["ink"]};
    --text-muted: {DT["ink_muted"]};
    --border: {DT["hairline"]};
    --accent: {O};
    --accent-text: {OT};
    --ink-on-accent: {INK};
    --sev-blocker: {SEVERITY["blocker"]["spine"]};
    --sev-judgment: {SEVERITY["judgment"]["spine"]};
    --sev-advisory: {SEVERITY["advisory"]["spine"]};
    --font-ui: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    --font-mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace;
  }}
  *{{box-sizing:border-box;margin:0;padding:0;font-family:var(--font-ui);}}
  /* ── Scrolling lives inside the grid, not on the page ──────────────────
     The batch bar is position:fixed, which resolves against the iframe's
     own viewport. Streamlit gives this iframe an explicit pixel height and
     the document used to be as tall as its content, so "fixed" pinned the
     bar to the bottom of a 750px+ box that was taller than a laptop screen
     — it scrolled out of reach instead of staying put. With the document
     clipped to the iframe box and #card-grid scrolling internally, fixed
     means what it says and the bar is always on screen. */
  html,body{{height:100%;overflow:hidden;}}
  #card-grid{{
    overflow-y:auto;overflow-x:hidden;
    /* --grid-top-h is measured in JS because the toolbar wraps to two rows
       on narrow screens; the fallback covers the first paint. */
    max-height:calc(100vh - var(--grid-top-h, 0px) - 16px);
    /* Measured, not fixed: the bottom bar now also carries language,
       sort and filter, so it wraps to two rows on narrow screens and
       a hardcoded 84px would let it cover the last row of cards. */
    padding-bottom:calc(var(--grid-bot-h, 84px) + 14px);
    scroll-behavior:smooth;
  }}
  /* Every numeric in a card — SID, price, dimensions — in tabular mono so a
     column of 12-digit SIDs is scannable rather than a wall of glyphs. */
  .num, .sid, .price-badge, .sel-count, .page-pill {{
    font-family:var(--font-mono);
    font-variant-numeric:tabular-nums slashed-zero;
  }}
  @media (prefers-reduced-motion: reduce) {{
    *, *::before, *::after {{
      animation-duration:.01ms !important;
      animation-iteration-count:1 !important;
      transition-duration:.01ms !important;
    }}
  }}
  body{{background:var(--bg);color:var(--text);padding:8px 8px 80px 8px;overflow-x:hidden;width:100%;transition:background .2s, color .2s;}}

  .ctrl-bar{{position:-webkit-sticky;position:sticky;top:0;z-index:99999;display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 12px;background:var(--card);backdrop-filter:blur(8px);border-bottom:2px solid var(--accent);border-radius:4px;margin-bottom:12px;box-shadow:0 4px 16px rgba(0,0,0,0.15);}}
  /* The .ctrl-bar.top-bar rules that were here — nowrap plus a horizontal
     scroll and three webkit-scrollbar rules to style it — went with the top
     bar itself. They matched nothing once it was removed. The bottom bar wraps
     by default, which is what the media query below was overriding them to do
     anyway.

     Below roughly a 13" laptop's content width the bar has more controls than
     room, and the reason dropdown has to give up its max-width to let the rest
     fit on the second row. */
  @media (max-width: 1100px) {{
    .rsearch-wrap{{max-width:none;}}
  }}

  #grid-search {{
    flex: 1;
    min-width: 200px;
    padding: 8px 14px;
    border-radius: 8px;
    border: 1px solid var(--border);
    font-size: 13px;
    outline: none;
    background: var(--bg);
    color: var(--text);
  }}
  .filter-group{{display:flex;flex-direction:column;gap:4px;min-width:180px;}}
  .filter-group .group-label{{display:flex;align-items:center;justify-content:space-between;gap:6px;font-size:11px;font-weight:800;color:var(--text);opacity:.86;white-space:nowrap;}}
  .filter-group .sel-count{{font-size:11px;font-weight:700;color:{OT};background:rgba(246,139,30,.14);padding:2px 6px;border-radius:999px;white-space:nowrap;}}
  .filter-group select[multiple]{{min-height:72px;padding:6px 10px;}}
  .filter-group select{{width:100%;}}
  .filter-group .hint{{font-size:10px;color:var(--text);opacity:.62;white-space:nowrap;}}
  .top-summary{{display:flex;flex-direction:column;gap:2px;min-width:180px;max-width:320px;}}
  .top-summary .main{{font-size:12px;font-weight:800;color:var(--text);white-space:nowrap;}}
  .top-summary .sub{{font-size:10px;color:var(--text);opacity:.72;white-space:nowrap;}}
  .toolbar-btn{{white-space:nowrap;}}
  .toolbar-btn.small{{padding:7px 10px;}}
  .page-nav{{display:flex;align-items:center;gap:6px;white-space:nowrap;}}
  .page-pill{{font-size:11px;font-weight:800;color:var(--text);opacity:.75;padding:0 4px;}}
  .empty-state{{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 20px;background:linear-gradient(135deg,rgba(246,139,30,.08),rgba(59,130,246,.06));border:1px dashed rgba(246,139,30,.35);border-radius:14px;margin:18px 0;}}
  .empty-state .title{{font-size:15px;font-weight:800;color:var(--text);margin-bottom:4px;}}
  .empty-state .desc{{font-size:12px;color:var(--text);opacity:.74;}}
  .empty-state .actions{{display:flex;gap:8px;flex-wrap:wrap;}}
  /* bottom:0 now that #cols-strip is gone. It used to be 26px, which was not
     slack but exactly the strip's height — the column-count buttons were a
     second fixed bar underneath this one, and dropping to 0 put the batch bar
     (z-index 10000) straight over them. The 3/4/5/6/7 buttons live in this bar
     now, so the strip and the offset both went. */
  .bottom-bar {{position: fixed; bottom: 0; left: 0; width: 100%; top: auto; border-bottom: none; border-top: 1px solid rgba(246, 139, 30, 0.2); margin: 0; z-index: 10000; box-shadow: 0 -4px 16px rgba(0,0,0,0.1); background: var(--card); padding: 10px 16px;}}
  /* A hairline before the column buttons, so a group of bare numbers reads as
     its own control rather than more batch actions. */
  .cols-group{{display:flex;align-items:center;gap:6px;padding-left:10px;margin-left:4px;border-left:1px solid var(--border);}}
  .cols-group .cols-label{{font-size:11px;font-weight:700;color:var(--text-muted);white-space:nowrap;letter-spacing:.04em;}}
  @media (max-width: 900px) {{ .cols-group .cols-label {{display:none;}} }}

  /* ── Floating mode ────────────────────────────────────────────────────
     Docked (default) the bar is opaque, full width, and #card-grid reserves
     its height so a card can never end up underneath it. Floating, it becomes
     a translucent island and the reservation drops to a few pixels, handing
     its whole height back to the cards. The bar is position:fixed either way
     — the only real difference is whether the grid reserves room for it. */
  .bottom-bar.floating{{
    left:12px; right:12px; width:auto; bottom:12px;
    border:1px solid rgba(246,139,30,.35); border-radius:12px;
    background:color-mix(in srgb, var(--card) 88%, transparent);
    backdrop-filter:blur(12px) saturate(1.1);
    -webkit-backdrop-filter:blur(12px) saturate(1.1);
    box-shadow:0 8px 28px rgba(26,25,23,.18);
  }}
  /* color-mix is recent; without it the rule above is dropped and the bar
     would be fully transparent over the cards. */
  @supports not (background: color-mix(in srgb, red 50%, transparent)) {{
    .bottom-bar.floating{{background:rgba(255,255,255,.9);}}
  }}
  /* Collapsed is a manual choice, never automatic. Select All, language, sort
     and cards-per-row all live in this bar, and those are exactly what you
     reach for when nothing is selected yet — so collapsing on an empty
     selection would hide the controls at the moment they are needed. */
  .bottom-bar.collapsed{{left:auto; right:12px; width:auto; padding:6px 10px;}}
  .bottom-bar.collapsed > *:not(.bar-toggle):not(.sel-count-text){{display:none !important;}}
  .bottom-bar.collapsed .sel-count-text{{margin-right:2px;}}
  .bar-toggle{{display:inline-flex;align-items:center;justify-content:center;
    min-width:28px;height:28px;padding:0 6px;border:1px solid var(--border);
    border-radius:6px;background:#fff;color:var(--text);cursor:pointer;
    font-size:13px;line-height:1;}}
  .bar-toggle:hover{{border-color:var(--accent);}}
  .bar-toggle[aria-pressed="true"]{{background:var(--accent);color:var(--ink-on-accent);border-color:var(--accent);}}

  .sel-count{{font-weight:700;color:{OT};font-size:13px;min-width:80px;font-family:var(--font-mono);font-variant-numeric:tabular-nums slashed-zero;}}
  .reason-sel{{flex:1;min-width:160px;padding:6px 10px;border:1px solid #ccc;border-radius:4px;font-size:12px;background:#fff;cursor:pointer;}}
  .rsearch-wrap{{position:relative;flex:1 0 auto;min-width:200px;max-width:260px;}}
  /* Reads as a picker, not a search field: chevron, hover, and a caret that
     turns when the list is open. */
  .rsearch-wrap::after{{content:'';position:absolute;right:10px;top:50%;width:7px;height:7px;
    border-right:2px solid {DT['ink_muted']};border-bottom:2px solid {DT['ink_muted']};
    transform:translateY(-70%) rotate(45deg);pointer-events:none;
    transition:transform .18s cubic-bezier(.2,.8,.3,1);}}
  .rsearch-wrap.open::after{{transform:translateY(-30%) rotate(-135deg);border-color:{OT};}}
  .rsearch-input{{width:100%;padding:6px 26px 6px 10px;border:1px solid {DT['hairline']};border-radius:6px;font-size:12px;background:#fff;cursor:pointer;box-sizing:border-box;
    text-overflow:ellipsis;transition:border-color .15s,box-shadow .15s;}}
  .rsearch-input:hover{{border-color:{O};}}
  .rsearch-input:focus{{outline:none;border-color:{O};box-shadow:0 0 0 3px rgba(246,139,30,.22);cursor:text;}}
  /* Fixed, and parented to <body> — the control bar it used to sit inside
     scrolls horizontally, which clipped the open list at the bar's edge. */
  .rsearch-panel{{position:fixed;width:280px;max-height:320px;overflow-y:auto;background:#fff;border:1px solid {DT['hairline']};border-radius:8px;box-shadow:0 12px 32px rgba(26,25,23,.18);z-index:2147483000;padding:4px 0;
    animation:rsearchIn .14s cubic-bezier(.2,.8,.3,1) both;}}
  @keyframes rsearchIn{{from{{opacity:0;transform:translateY(-4px) scale(.99);}}to{{opacity:1;transform:none;}}}}
  .rsearch-group-label{{padding:6px 10px 3px;font-size:10px;font-weight:800;color:#9ca3af;text-transform:uppercase;letter-spacing:.03em;}}
  .rsearch-item{{padding:7px 12px;font-size:12px;color:#1f2937;cursor:pointer;white-space:normal;}}
  .rsearch-item:hover, .rsearch-item.rsearch-hl{{background:{O};color:{INK};}}
  .rsearch-empty{{padding:10px 12px;font-size:12px;color:#9ca3af;}}
  /* nowrap + shrink:0 because the control bar is flex-wrap:nowrap: without
     them "Batch Reject Selected" wrapped to two lines and spilled out below
     its own orange fill once the bar ran short of room. */
  .batch-btn{{padding:7px 14px;background:{O};color:{INK};border:1px solid rgba(0,0,0,.12);border-radius:6px;font-weight:700;font-size:12px;cursor:pointer;
    white-space:nowrap;flex-shrink:0;line-height:1.25;
    transition:transform .12s cubic-bezier(.2,.8,.3,1),box-shadow .15s,background .15s;}}
  .batch-btn:hover{{background:{DT['accent_hover']};box-shadow:0 3px 10px rgba(246,139,30,.35);transform:translateY(-1px);}}
  .batch-btn:active{{transform:translateY(0) scale(.97);}}
  .batch-btn:focus-visible{{outline:2px solid {OT};outline-offset:2px;}}
  .desel-btn{{padding:7px 12px;background:#fff;color:{DT['ink']};border:1px solid {DT['hairline']};border-radius:6px;font-size:12px;cursor:pointer;
    white-space:nowrap;flex-shrink:0;line-height:1.25;
    transition:transform .12s cubic-bezier(.2,.8,.3,1),border-color .15s,background .15s;}}
  .desel-btn:hover{{background:{DT['accent_wash']};border-color:{O};color:{OT};}}
  .desel-btn:active{{transform:scale(.97);}}
  .desel-btn:focus-visible{{outline:2px solid {OT};outline-offset:2px;}}

  /* Icon-only controls. Square, same height as the text buttons so the bar
     keeps one baseline, and always paired with title + aria-label. */
  .icon-btn{{display:inline-flex;align-items:center;justify-content:center;
    width:32px;height:32px;flex-shrink:0;padding:0;background:#fff;
    color:{DT['ink_muted']};border:1px solid {DT['hairline']};border-radius:6px;
    cursor:pointer;transition:background .15s,border-color .15s,color .15s,transform .12s cubic-bezier(.2,.8,.3,1);}}
  .icon-btn:hover{{background:{DT['accent_wash']};border-color:{O};color:{OT};}}
  .icon-btn:active{{transform:scale(.94);}}
  .icon-btn:focus-visible{{outline:2px solid {OT};outline-offset:2px;}}
  .icon-btn svg{{display:block;}}

  /* Language: native <select> kept for accessibility, dressed as an icon
     control — globe glyph plus the two-letter code, no chrome. */
  .lang-wrap{{position:relative;display:inline-flex;align-items:center;flex-shrink:0;}}
  .lang-ico{{position:absolute;left:7px;display:flex;pointer-events:none;color:{DT['ink_muted']};}}
  .lang-wrap:hover .lang-ico{{color:{OT};}}
  .lang-sel{{appearance:none;-webkit-appearance:none;height:32px;
    padding:0 8px 0 27px;background:#fff;color:{DT['ink']};
    border:1px solid {DT['hairline']};border-radius:6px;cursor:pointer;
    font-size:11px;font-weight:700;letter-spacing:.04em;
    transition:background .15s,border-color .15s;}}
  .lang-wrap:hover .lang-sel{{background:{DT['accent_wash']};border-color:{O};}}
  .lang-sel:focus-visible{{outline:2px solid {OT};outline-offset:2px;}}
  .top-btn {{margin-left: auto; background: #313133; color: white; border-color: #313133; font-weight: bold;}}
  .top-btn:hover {{background: #000; color: white;}}

  .grid{{display:grid;grid-template-columns:repeat({cols_per_row},minmax(0,1fr));gap:12px;width:100%;}}
  .card{{border:2px solid var(--border);border-radius:10px;padding:10px;background:var(--card);position:relative;z-index:1;min-width:0;word-wrap:break-word;display:flex;flex-direction:column;min-height:360px;outline:none;
    transition:border-color .18s ease,box-shadow .18s ease,transform .18s cubic-bezier(.2,.8,.3,1);}}
  /* min-height, not height: a 360px floor forced every card to reserve
     space for a 180px image even when the column was half that wide. */
  @media (max-width: 1100px) {{ .card{{min-height:300px;}} }}
  @media (max-width: 900px)  {{ .card{{min-height:270px;padding:8px;}} }}
  /* There is deliberately NO entry animation on .card.
     renderAll() swaps the whole page in with a single innerHTML assignment,
     so every card is a new node on every render — page turn, filter change,
     even a selection sync. An entry animation therefore never plays "on
     open"; it replays for all 500 cards every time, which is what made the
     grid feel slow. Hover and selection stay as transitions, which only run
     on the one element being touched. */
  .card:hover{{transform:translateY(-3px);box-shadow:0 10px 26px rgba(26,25,23,.13);border-color:#D8D3CE;}}
  .card:focus {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(246,139,30,0.3); transform: translateY(-4px); }}
  /* A single flash when a card's decision changes, so a bulk action reads as
     something that happened rather than a silent repaint. */
  @keyframes stateFlash{{0%{{box-shadow:0 0 0 0 rgba(246,139,30,.55);}}100%{{box-shadow:0 0 0 14px rgba(246,139,30,0);}}}}
  @keyframes stateFlashRej{{0%{{box-shadow:0 0 0 0 rgba(179,38,30,.55);}}100%{{box-shadow:0 0 0 14px rgba(179,38,30,0);}}}}
  .card.just-changed{{animation:stateFlash .5s ease-out 1;}}
  .card.just-changed-rej{{animation:stateFlashRej .5s ease-out 1;}}
  .card.committed-rej{{transition:opacity .25s ease,filter .25s ease;}}
  .card-img{{transition:opacity .4s ease,transform .3s cubic-bezier(.2,.8,.3,1);}}
  .card:hover .card-img{{transform:scale(1.035);}}

  .card.selected{{border-color:{O};box-shadow:0 0 0 5px rgba(255,136,0,.35);background:rgba(255,136,0,.04);}}
  .card.staged-rej{{border-color:{R};box-shadow:0 0 0 4px rgba(231,60,23,.3);background:rgba(231,60,23,.04);}}
  .card.committed-rej{{border-color:#bbb;opacity:.6;}}
  .card.manual-review{{border-color:#dc2626;box-shadow:0 0 0 3px rgba(220,38,38,0.25);}}
  .card.zip-card{{border-left:4px solid #3b82f6;box-shadow:-4px 0 8px rgba(59,130,246,0.20);}}
  .card.zip-card.selected{{border-left:4px solid #3b82f6;box-shadow:-4px 0 8px rgba(59,130,246,0.20),0 0 0 5px rgba(255,136,0,.35);}}
  .card.zip-card.manual-review{{border-color:#dc2626;border-left:4px solid #3b82f6;box-shadow:-4px 0 8px rgba(59,130,246,0.20), 0 0 0 3px rgba(220,38,38,0.25);}}
  .ai-color-pill{{display:inline-block;background:#dbeafe;color:#1e40af;font-size:10px;font-weight:700;padding:2px 7px;border-radius:99px;border:1px solid #93c5fd;margin-top:3px;}}
  .ai-brand-pill{{display:inline-block;background:#fef9c3;color:#854d0e;font-size:10px;font-weight:700;padding:2px 7px;border-radius:99px;border:1px solid #fde68a;margin-top:3px;}}

  /* Was a hard 180px, which is most of a 186px-wide card at five columns on
     a 1280px laptop. Squaring to the card's own width lets the whole card
     shrink with the column count instead of the metadata being crushed. */
  .card-img-wrap{{position:relative;cursor:pointer;border-radius:8px;background:#fff;display:flex;align-items:center;justify-content:center;aspect-ratio:1/1;max-height:180px;min-height:104px;width:100%;overflow:hidden; border:1px solid {DT['hairline']};flex-shrink:0;}}
  .card-img-wrap::before{{content:'';position:absolute;inset:0;background:linear-gradient(90deg,#FFF8F2 25%,#FFEFE5 50%,#FFF8F2 75%);background-size:200% 100%;animation:shimmer 1.4s infinite;z-index:1;}}
  .card-img-wrap.img-loaded::before{{display:none;}}
  @keyframes shimmer{{0%{{background-position:200% 0}}100%{{background-position:-200% 0}}}}
  .card-img-placeholder{{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:1;}}
  .card-img{{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:2;opacity:0;transition:opacity .4s ease;}}
  .card-img.img-loaded{{opacity:1;}}
  .card.committed-rej .card-img{{filter:grayscale(80%);}}

  .warn-wrap{{position:absolute;top:8px;right:8px;display:flex;flex-direction:column;gap:6px;z-index:10;pointer-events:none;max-width:calc(100% - 16px);}}
  .warn-group{{display:flex;flex-direction:column;gap:4px;align-items:flex-end;}}
  .warn-group.inline{{flex-direction:row;flex-wrap:wrap;justify-content:flex-end;}}
  /* Three pulses, not infinite. The pulse earns its keep for a few seconds —
     "look here, this is new" — but a running animation stops the page ever
     going idle, so the compositor stays scheduled for the whole session for a
     badge nobody is reading after the first few seconds. .advisory, .info and
     .neutral already set animation:none; this finishes that decision for the
     base and .critical variants without losing the initial cue. */
  .warn-badge{{display:inline-flex;align-items:center;justify-content:center;max-width:100%;background:linear-gradient(90deg,#FFC107,#FF9800);color:#313133;font-size:11px;font-weight:800;padding:3px 8px;border-radius:9999px;box-shadow:0 2px 6px rgba(255,152,0,.3);animation:pulse 2s 3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
  .warn-badge.critical{{background:linear-gradient(90deg,#dc2626,#b91c1c);color:#fff;box-shadow:0 2px 6px rgba(220,38,38,.28);}}
  .warn-badge.advisory{{background:linear-gradient(90deg,#f59e0b,#f97316);color:#fff;box-shadow:0 2px 6px rgba(249,115,22,.22);animation:none;}}
  .warn-badge.info{{background:linear-gradient(90deg,#3b82f6,#2563eb);color:#fff;box-shadow:0 2px 6px rgba(37,99,235,.22);animation:none;}}
  .warn-badge.neutral{{background:linear-gradient(90deg,#6b7280,#4b5563);color:#fff;box-shadow:0 2px 6px rgba(75,85,99,.2);animation:none;}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:0.85}}}}
  .price-badge{{position:absolute;top:8px;left:8px;background:rgba(246,139,30,0.95);color:#fff;font-size:10px;font-weight:800;padding:3px 8px;border-radius:9999px;z-index:10;pointer-events:none;box-shadow:0 2px 6px rgba(0,0,0,.2);}}

  .meta{{font-size:11px;margin-top:8px;line-height:1.35;flex-grow:1;display:flex;flex-direction:column;gap:4px;}}
  .meta-core{{display:flex;flex-direction:column;gap:3px;}}
  .meta .nm{{font-weight:800;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:help;}}
  .meta .br{{color:{OT};font-weight:700;margin:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
  .meta .ct{{color:#666;font-size:10px;word-break:break-word;line-height:1.25;}}
  .meta .sl{{color:#6B6560;font-size:11px;margin-top:2px;border-top:1px dashed #eee;padding-top:4px;cursor:help;display:flex;align-items:center;gap:6px;min-width:0;}}
  .meta .co{{color:#555;font-size:10px;margin-top:4px;background:#f0f0f0;padding:3px 5px;border-radius:4px;display:inline-block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%;font-weight:600;}}
  .meta-extra{{border-top:1px solid #ececec;padding-top:4px;}}
  .meta-extra summary{{cursor:pointer;list-style:none;font-size:11px;font-weight:700;color:{OT};user-select:none;}}
  .meta-extra summary::-webkit-details-marker{{display:none;}}
  .meta-extra-body{{display:flex;flex-direction:column;gap:4px;padding-top:6px;}}
  .meta-extra .co{{margin-top:0;}}

  .acts{{display:flex;gap:4px;margin-top:auto;padding-top:8px;}}
  .act-btn{{flex:1;padding:6px;font-size:11px;border:1px solid rgba(0,0,0,.12);border-radius:4px;cursor:pointer;font-weight:700;color:{INK};background:{O};}}
  .act-more{{flex:1;font-size:11px;border:1px solid #ccc;border-radius:4px;outline:none;cursor:pointer;background:#fff;}}

  .zoom-btn{{position:absolute;bottom:6px;right:6px;width:22px;height:22px;background:rgba(0,0,0,0.4);color:#fff;border-radius:4px;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:25;border:none;transition:background .2s;}}
  .zoom-btn:hover{{background:rgba(0,0,0,0.7);}}
  .zoom-btn svg{{width:12px;height:12px;flex-shrink:0;}}

  .zoom-nav-btn {{ position: absolute; top: 50%; transform: translateY(-50%); background: rgba(0,0,0,0.5); color: white; border: none; font-size: 24px; padding: 10px; cursor: pointer; border-radius: 4px; z-index: 100001; }}
  .zoom-nav-btn:hover {{ background: rgba(0,0,0,0.8); }}
  .zoom-nav-btn.prev {{ left: -40px; }}
  .zoom-nav-btn.next {{ right: -40px; }}

  .tick{{position:absolute;bottom:6px;left:6px;width:22px;height:22px;border-radius:50%;background:rgba(0,0,0,.18);display:flex;align-items:center;justify-content:center;color:transparent;font-size:13px;font-weight:900;pointer-events:none;z-index:10;}}
  .card.selected .tick{{background:{O};color:{INK};}}
  .card.committed-rej.selected .tick{{z-index:25;background:{O};color:{INK};}}
  .card.committed-rej.selected{{box-shadow:0 0 0 4px {O},0 0 0 8px rgba(255,136,0,.25)!important;}}

  .rej-overlay{{display:none;position:absolute;inset:0;background:rgba(255,255,255,.90);border-radius:8px;flex-direction:column;align-items:center;justify-content:center;z-index:20;gap:8px;padding:12px;text-align:center;}}
  .card.committed-rej .rej-overlay{{display:flex;}}
  .card.committed-rej.poor-img-rej .rej-overlay{{background:rgba(0,0,0,.45);backdrop-filter:blur(1px);}}
  .card.committed-rej.poor-img-rej{{border-color:{R};opacity:1;}}
  .card.committed-rej.poor-img-rej .card-img{{filter:none;}}
  .card.committed-rej.poor-img-rej .rej-badge{{background:rgba(231,60,23,.9);}}
  .card.committed-rej.poor-img-rej .rej-label{{color:#fff;}}
  .card.committed-rej.poor-img-rej .undo-btn{{background:#fff;color:{R};}}
  .card.committed-rej.poor-img-rej .undo-btn:hover{{background:#f0f0f0;}}

  .card.staged-rej .rej-overlay.staged{{display:flex; background:rgba(211,47,47,0.85);}}
  .card.staged-rej .rej-badge.pending{{background:transparent; color:#fff; font-size:22px; font-weight:900; padding:0; letter-spacing:1px;}}
  .card.staged-rej .rej-label{{color:#fff; font-size:13px; font-weight:600; line-height:1.2; max-width:140px;}}

  .card.committed-rej .rej-badge{{background:{R};color:#fff;padding:6px 12px;border-radius:6px;font-size:15px;font-weight:800;letter-spacing:0.5px;}}
  .card.committed-rej .rej-label{{font-size:12px;color:{R};font-weight:700;max-width:130px;}}

  .undo-btn{{margin-top:8px;padding:6px 14px;background:#313133;color:#fff;border:none;border-radius:4px;font-size:11px;font-weight:bold;cursor:pointer;}}
  .undo-btn:hover{{background:#000;}}
  .card.staged-rej .undo-btn{{background:#fff; color:#D32F2F; box-shadow:0 2px 6px rgba(0,0,0,0.2);}}
  .card.staged-rej .undo-btn:hover{{background:#f0f0f0;}}

  .card.committed-rej.brand-image-rej .rej-badge {{ background: #2E7D32 !important; }}
  .card.committed-rej.brand-image-rej .rej-label {{ color: #2E7D32 !important; }}
  .card.committed-rej.brand-image-rej .rej-overlay {{ background: rgba(232, 245, 233, 0.6) !important; }}

  /* 🧠 Highlights */
  .hlt {{ background: #fee2e2; color: #b91c1c; font-weight: 800; border-radius: 2px; padding: 0 2px; }}

  /* Per-card undo shimmer */
  .card.undo-processing {{
    pointer-events: none;
  }}
  .card.undo-processing::after {{
    content: '';
    position: absolute;
    inset: 0;
    border-radius: 8px;
    background: rgba(255,255,255,0.55);
    backdrop-filter: blur(2px);
    z-index: 30;
    animation: undoPulse 0.5s ease-in-out infinite alternate;
  }}
  @keyframes undoPulse {{
    from {{ opacity: 0.4; }}
    to   {{ opacity: 0.85; }}
  }}

  #zoom-tooltip  .ctrl-bar {{
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 16px;
    background: rgba(255, 255, 255, 0.75);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border-bottom: 1px solid rgba(246, 139, 30, 0.2);
    position: sticky;
    top: 0;
    z-index: 100;
  }}


  @keyframes shimmer {{
    0% {{ background-position: -1000px 0; }}
    100% {{ background-position: 1000px 0; }}
  }}
  .skeleton {{
    background: #f6f7f8;
    background-image: linear-gradient(to right, #f6f7f8 0%, #edeef1 20%, #f6f7f8 40%, #f6f7f8 100%);
    background-repeat: no-repeat;
    background-size: 2000px 100%;
    animation: shimmer 2s infinite linear;
  }}

  .card {{
    background: #fff;
    border-radius: 12px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    border: 1px solid #eee;
    position: relative;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
  }}
  .card:hover {{
    transform: translateY(-6px) scale(1.01);
    box-shadow: 0 12px 30px rgba(0,0,0,0.12);
    z-index: 10;
  }}
  #zoom-backdrop {{
    display: none;
    position: fixed;
    top: 0; left: 0; width: 100vw; height: 100vh;
    background: rgba(0,0,0,0.6);
    z-index: 99999;
    cursor: pointer;
  }}
  #zoom-tooltip {{
    display: none;
    position: fixed;
    z-index: 100000;
    background: #fff;
    padding: 10px;
    border-radius: 8px;
    box-shadow: 0 10px 40px rgba(0,0,0,0.4);
    border: 1px solid #ccc;
    width: 360px;
    height: 360px;
    transition: opacity 0.2s ease;
  }}
  #tooltip-img {{
    width: 100%;
    height: 100%;
    object-fit: contain;
    display: block;
  }}
  .tooltip-close {{
    position: absolute;
    top: -12px;
    right: -12px;
    background: #333;
    color: #fff;
    border-radius: 50%;
    width: 28px;
    height: 28px;
    border: 2px solid #fff;
    cursor: pointer;
    font-size: 16px;
    line-height: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    box-shadow: 0 2px 6px rgba(0,0,0,0.3);
  }}
  .tooltip-close:hover {{ background: #000; }}

  #custom-reason-panel {{
    display: none;
    position: fixed;
    bottom: 80px;
    left: 50%;
    transform: translateX(-50%);
    background: #fff;
    border: 2px solid {O};
    border-radius: 8px;
    padding: 16px 20px;
    z-index: 999999;
    box-shadow: 0 8px 32px rgba(0,0,0,0.25);
    min-width: 340px;
    max-width: 480px;
  }}
  #custom-reason-panel h4 {{ margin: 0 0 10px 0; font-size: 13px; color: #333; }}
  #custom-reason-input {{
    width: 100%;
    padding: 8px 10px;
    border: 1px solid #ccc;
    border-radius: 4px;
    font-size: 13px;
    margin-bottom: 10px;
    box-sizing: border-box;
  }}
  #custom-reason-input:focus {{ outline: 2px solid {O}; border-color: {O}; }}
  .custom-panel-btns {{ display: flex; gap: 8px; }}
  .custom-panel-btns button {{ flex: 1; padding: 7px; border-radius: 4px; font-size: 12px; font-weight: 700; cursor: pointer; border: none; }}
  .custom-panel-confirm {{ background: {O}; color: #fff; }}
  .custom-panel-confirm:hover {{ opacity: 0.88; }}
  .custom-panel-cancel {{ background: #e0e0e0; color: #333; }}
  .custom-panel-cancel:hover {{ background: #ccc; }}

  input[type="search"]::-webkit-search-cancel-button {{
    -webkit-appearance: searchfield-cancel-button;
    cursor: pointer;
    width: 16px;
    height: 16px;
    margin-left: 8px;
  }}
</style>
</head>
<body>

<div id="custom-reason-panel">
  <h4>{labels_dict['custom_reason_title']}</h4>
  <input id="custom-reason-input" type="text" placeholder="{labels_dict['custom_reason_ph']}" maxlength="200">
  <div class="custom-panel-btns">
    <button class="custom-panel-confirm" onclick="confirmCustomReason()">{labels_dict['custom_apply']}</button>
    <button class="custom-panel-cancel" onclick="cancelCustomReason()">{labels_dict['custom_cancel']}</button>
  </div>
</div>

<div id="lang-loading" style="display:none;position:fixed;inset:0;background:rgba(255,255,255,0.8);z-index:9999999;align-items:center;justify-content:center;flex-direction:column;">
  <div style="width:40px;height:40px;border:4px solid #f3f3f3;border-top:4px solid {O};border-radius:50%;animation:spin 1s linear infinite;"></div>
  <style>@keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}</style>
  <div style="margin-top:16px;font-weight:700;color:#333;font-size:16px;">Updating Language...</div>
</div>

<!-- The top control bar is gone. Every batch control on it (reason, Batch
     Reject, Select All, Deselect All, undo) duplicated the fixed bottom bar,
     which is on screen at all times anyway. Its three unique controls -
     language, sort and filter - moved down there. That reclaims ~55px in a
     modal that had roughly 370px left for cards. -->

<div id="shortcut-help" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);
  z-index:9999999;align-items:center;justify-content:center;">
  <div style="background:var(--card);color:var(--text);border-radius:16px;padding:32px;min-width:280px;box-shadow:0 20px 50px rgba(0,0,0,0.3);">
    <h3 style="margin:0 0 16px">Keyboard Shortcuts</h3>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <tr style="border-bottom:1px solid var(--border);"><td style="padding:8px 0;"><kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">j</kbd> / <kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">→</kbd></td><td style="padding-left:10px;">Next card</td></tr>
      <tr style="border-bottom:1px solid var(--border);"><td style="padding:8px 0;"><kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">k</kbd> / <kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">←</kbd></td><td style="padding-left:10px;">Prev card</td></tr>
      <tr style="border-bottom:1px solid var(--border);"><td style="padding:8px 0;"><kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">a</kbd></td><td style="padding-left:10px;">Approve focused</td></tr>
      <tr style="border-bottom:1px solid var(--border);"><td style="padding:8px 0;"><kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">r</kbd></td><td style="padding-left:10px;">Reject focused</td></tr>
      <tr style="border-bottom:1px solid var(--border);"><td style="padding:8px 0;"><kbd style="background:#eee;padding:2px 6px;border-radius:4px;color:#000;">?</kbd></td><td style="padding-left:10px;">Toggle help</td></tr>
    </table>
    <button onclick="document.getElementById('shortcut-help').style.display='none'"
      style="margin-top:20px;width:100%;padding:10px;border-radius:8px;
      background:var(--accent);color:#fff;border:none;cursor:pointer;font-weight:700;">Got it!</button>
  </div>
</div>

<div class="grid" id="card-grid">
  <style>
    .sk-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;width:100%;}}
    .sk-card{{border-radius:12px;overflow:hidden;background:#f3f4f6;height:260px;animation:pulse 1.4s ease-in-out infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
  </style>
  <div style="font-family: sans-serif; color: #f97316; font-size: 14px; font-weight: bold; margin-bottom: 5px; padding-top: 10px;">Loading Page...</div>
  <div class="sk-grid">
    <div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div>
    <div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div>
    <div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div><div class="sk-card"></div>
  </div>
</div>

<div class="ctrl-bar bottom-bar">
  <span class="lang-wrap" title="Change language">
    <span class="lang-ico">{_ICON_GLOBE}</span>
    <select id="iframe-lang-sel" class="lang-sel" aria-label="Change language" onchange="document.getElementById('lang-loading').style.display='flex'; sendMsg('change_lang', this.value)">
      <option value="en" {"selected" if lang=="en" else ""}>EN</option>
      <option value="fr" {"selected" if lang=="fr" else ""}>FR</option>
      <option value="ar" {"selected" if lang=="ar" else ""}>AR</option>
    </select>
  </span>
  <span class="sel-count-text" style="font-weight:700; color:var(--accent-text); font-size:13px; font-family:var(--font-mono); font-variant-numeric:tabular-nums slashed-zero;">0 {labels_dict["items_pending"]}</span>
  <select class="reason-sel" id="batch-reason-bottom">
    <option value="REJECT_POOR_IMAGE">{labels_dict["poor_img"]}</option>
    <option value="REJECT_IMG_STRETCHED">Image Stretched</option>
    <option value="REJECT_IMG_BLURRY">Image Blurry</option>
    <option value="REJECT_IMG_MISMATCH">Image Mismatch</option>
    <option value="REJECT_IMG_INFRINGING">Image Infringing</option>
    <option value="REJECT_IMG_TOO_MANY">Image Too Many Things</option>
    <option value="REJECT_WRONG_CAT">{labels_dict["wrong_cat"]}</option>
    <option value="REJECT_FAKE">{labels_dict["fake_prod"]}</option>
    <option value="REJECT_BRAND">{labels_dict["restr_brand"]}</option>
    <option value="REJECT_WRONG_BRAND">{labels_dict["wrong_brand"]}</option>
    <option value="REJECT_PROHIBITED">{labels_dict["prohibited"]}</option>
    <option value="REJECT_COLOR">{labels_dict["missing_color"]}</option>
    <option value="REJECT_FDA">FDA</option>
    <option value="REJECT_DUPLICATE">{labels_dict["sort_duplicates"]}</option>
    {extra_reason_options_html}
    <option value="OTHER_CUSTOM">Other Reason (Custom)</option>
  </select>
  <button class="batch-btn" onclick="doBatchReject('bottom')">{labels_dict["batch_reject"]}</button>
  <button class="icon-btn" onclick="doBatchUndo()" title="{labels_dict["undo"]}" aria-label="{labels_dict["undo"]}">{_ICON_UNDO}</button>
  <button class="desel-btn" onclick="window.doSelectAll()">{labels_dict["select_all"]}</button>
  <button class="desel-btn" onclick="doDeselAll()">{labels_dict["deselect_all"]}</button>
  {sort_html}
  {filter_html}
  <span class="cols-group">
    <span class="cols-label">Cards per row</span>
    {_cols_btns}
  </span>
  <button class="icon-btn" style="margin-left:auto;" onclick="gridScrollTo('top')" title="Back to top" aria-label="Back to top">{_ICON_TOP}</button>
  <button class="bar-toggle" id="bar-float-btn" onclick="toggleBarFloat()" aria-pressed="false"
          title="Float the bar over the cards" aria-label="Float the bar over the cards">&#9679;</button>
  <button class="bar-toggle" id="bar-collapse-btn" onclick="toggleBarCollapse()" aria-expanded="true"
          title="Collapse the bar" aria-label="Collapse the bar">&#9660;</button>
</div>

<div id="zoom-backdrop" onclick="closeZoom()"></div>
<div id="zoom-tooltip">
  <button class="zoom-nav-btn prev" onclick="event.stopPropagation(); window.zoomMove(-1)" title="Previous">&#10094;</button>
  <img id="tooltip-img" alt="Zoomed product" referrerpolicy="no-referrer">
  <button class="zoom-nav-btn next" onclick="event.stopPropagation(); window.zoomMove(1)" title="Next">&#10095;</button>
  <button class="tooltip-close" onclick="closeZoom()" title="Close">×</button>
</div>

<div id="prefetch-status"></div>

<script>

// blockOutsideClicks removed — dismissible=False in Python handles this
// and the capture listeners were preventing Streamlit buttons from firing

function escapeHtml(u){{return(u||"").toString().replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#039;");}}
window.__IS_GRID_IFRAME__ = true;
var CARDS = [];
var COMMITTED = {{}};
var POOR_IMG_SIDS = new Set();
var PREFETCH_URLS = {{}};
var PLACEHOLDER = "{_PLACEHOLDER_SVG}";
var _lastCardsSig = null;
// Base64 ZIP images, keyed by SID and kept out of the card records so the card
// payload stays small. Retained across syncs: paging back to a visited page
// costs nothing because the bytes are already here.
var IMAGES = {{}};
// Single resolution point for a card's image source, so callers never have to
// know whether it came inline (http URL) or from the IMAGES store (ZIP).
function imgFor(card) {{
  if (!card) return PLACEHOLDER;
  return card.img || IMAGES[card.sid] || PLACEHOLDER;
}}

// Announce that the listener below is live.
//
// The broadcaster used to fire blind: four attempts, 250ms apart, 750ms in
// total. This document is ~110KB of HTML and ~65KB of JS, so on a cold Cloud
// container it is routinely still parsing when the last attempt goes out.
// Every message then lands on a frame with no listener, CARDS stays empty,
// and the grid draws "No products match your filters" over a batch of 863 —
// until any interaction triggers a rerun and a fresh send that happens to
// win the race.
//
// Same delivery route the SYNC_ACK below already uses: parent plus every
// sibling frame, because the broadcaster is a sibling iframe, not the parent.
function _announceGridReady() {{
  try {{
    var _rdy = {{type: 'GRID_READY'}};
    window.parent.postMessage(_rdy, '*');
    for (var _r = 0; _r < window.parent.frames.length; _r++) {{
      try {{ window.parent.frames[_r].postMessage(_rdy, '*'); }} catch(_e) {{}}
    }}
  }} catch(_e) {{}}
}}

window.addEventListener('message', function(e) {{
  if (e.data && e.data.type === 'SYNC_STATE') {{
    var cardsChanged = false;
    if (e.data.images) {{
      for (var _s in e.data.images) {{ IMAGES[_s] = e.data.images[_s]; }}
      // Hard ceiling. Each entry is a base64 data URI, so without this a long
      // paging session would grow the tab's memory without bound until it hung.
      var _keys = Object.keys(IMAGES);
      if (_keys.length > 600) {{
        for (var _k = 0; _k < _keys.length - 600; _k++) {{ delete IMAGES[_keys[_k]]; }}
      }}
    }}
    // Tell the sender it landed so it stops retrying. The old code fired the
    // full payload four times at every frame regardless of success.
    try {{
      var _ack = {{type: 'SYNC_ACK', sig: e.data.cards_sig || null}};
      window.parent.postMessage(_ack, '*');
      for (var _i = 0; _i < window.parent.frames.length; _i++) {{
        try {{ window.parent.frames[_i].postMessage(_ack, '*'); }} catch(_e) {{}}
      }}
    }} catch(_e) {{}}
    if (e.data.cards) {{
      // A new page/filter/sort of cards arrived over postMessage instead
      // of via a full srcdoc reload. This is what avoids the multi-flicker
      // on page turns: the iframe document itself never reloads, so
      // already-decoded images for repeated URLs aren't re-fetched, and
      // the browser doesn't tear down/rebuild the whole DOM.
      var sig = e.data.cards_sig || null;
      if (sig === null || sig !== _lastCardsSig) {{
        CARDS = e.data.cards;
        _lastCardsSig = sig;
        cardsChanged = true;
      }}
    }}
    if (e.data.committed) COMMITTED = e.data.committed;
    if (e.data.poor_img_sids) POOR_IMG_SIDS = new Set(e.data.poor_img_sids);
    if (e.data.prefetch) PREFETCH_URLS = e.data.prefetch;
    closeGhostOverlay();

    // Scroll state lives on #card-grid now, not the window.
    var _g = document.getElementById('card-grid');
    var oldScroll = _g ? _g.scrollTop : 0;
    renderAll();
    if (window.sizeGridScroll) window.sizeGridScroll();
    _g = document.getElementById('card-grid');
    if (_g) {{
      // Only reset to top on an actual page/filter change (cardsChanged); for
      // committed/poor_img/prefetch-only syncs, preserve scroll position.
      _g.scrollTop = (cardsChanged && e.data.scroll_to_top) ? 0 : oldScroll;
    }}
  }}
}});
// Listener is registered — safe to ask for the payload now. Repeated on
// DOMContentLoaded and load in case the broadcaster iframe had not yet
// executed when this ran.
_announceGridReady();
window.addEventListener('DOMContentLoaded', _announceGridReady);
window.addEventListener('load', _announceGridReady);
var LABELS = {labels_json};
var DEFAULT_PAGE_SIZE = {items_per_page};
var PAGE_SIZE_OPTIONS = [20, 50, 100, 200, 500];
var PAGE_STATE = {{
  pageSize: DEFAULT_PAGE_SIZE,
  page: 1,
  search: '',
  sellers: [],
  categories: []
}};

window._gridSelected = window._gridSelected || {{}};
window._stagedRejections = window._stagedRejections || {{}};
window.currentZoomSid = null;
window._imageIssues = window._imageIssues || {{}};
// URLs whose images have already loaded once in this iframe session. Cards with
// a cached URL are re-rendered fully visible (no shimmer/fade), so page
// turns don't flicker for images the browser already has.
window._loadedImgs = window._loadedImgs || new Set();
CARDS.forEach(c => {{
  if (c.warnings && c.warnings.length) {{
    if (!window._imageIssues[c.sid]) window._imageIssues[c.sid] = [];
    c.warnings.forEach(w => {{ if (!window._imageIssues[c.sid].includes(w)) window._imageIssues[c.sid].push(w); }});
  }}
}});
window._currentSort = window._currentSort || '';

window._pendingUndos = window._pendingUndos || {{}};
window._undoTimer = null;

var selected = window._gridSelected;
var staged = window._stagedRejections;

function showGhostOverlay(msgText, autoHideMs) {{
  var ghost = document.createElement('div');
  ghost.id = '__grid_ghost__';
  ghost.style.cssText = 'position:fixed;z-index:99999;inset:0;background:rgba(255,255,255,0.85);display:flex;align-items:center;justify-content:center;font-family:sans-serif;color:#FF8800;transition:opacity 0.4s ease;';
  ghost.innerHTML = '<div style="font-size:22px;font-weight:bold;">' + msgText + '</div>';
  var existing = document.getElementById('__grid_ghost__');
  if (existing) existing.remove();
  document.body.appendChild(ghost);
  // Default auto-hide: short overlays (notifications) use 4s, long ops use the caller's value.
  // For batch ops we use a long timeout so the overlay stays until Streamlit reloads the page.
  var hideDelay = (typeof autoHideMs === 'number') ? autoHideMs : 4000;
  setTimeout(function() {{
    var g = document.getElementById('__grid_ghost__');
    if (g) {{ g.style.opacity = '0'; setTimeout(function() {{ if(g && g.parentNode) g.remove(); }}, 400); }}
  }}, hideDelay);
}}

function closeGhostOverlay() {{
  var g = document.getElementById('__grid_ghost__');
  if (g) {{ g.style.opacity = '0'; setTimeout(function() {{ if(g && g.parentNode) g.remove(); }}, 400); }}
}}

function sendMsg(type, payload) {{
  try {{
    var par = window.parent;
    var inputs = par.document.querySelectorAll('input[type="text"]');
    var bridge = null;
    var targetPlaceholder = (type === 'change_lang') ? 'LANG_BRIDGE_DO_NOT_USE' : 'JTBRIDGE_UNIQUE_DO_NOT_USE';
    for (var i = 0; i < inputs.length; i++) {{
      if (inputs[i].placeholder === targetPlaceholder || (targetPlaceholder === 'JTBRIDGE_UNIQUE_DO_NOT_USE' && inputs[i].getAttribute('aria-label') === 'jtbridge')) {{
        bridge = inputs[i]; break;
      }}
    }}
    if (!bridge) {{ console.warn('sendMsg: bridge not found for', type); return; }}
    var msg = JSON.stringify({{action: type, payload: payload}});
    var nativeInputValueSetter = Object.getOwnPropertyDescriptor(par.HTMLInputElement.prototype, 'value').set;
    nativeInputValueSetter.call(bridge, msg);
    bridge.dispatchEvent(new par.Event('input', {{bubbles: true}}));
    bridge.focus({{preventScroll: true}});
    bridge.dispatchEvent(new par.KeyboardEvent('keydown', {{bubbles:true,cancelable:true,key:'Enter',keyCode:13}}));
    bridge.dispatchEvent(new par.KeyboardEvent('keyup',   {{bubbles:true,cancelable:true,key:'Enter',keyCode:13}}));
    bridge.blur();
  }} catch(ex) {{ console.error('jtbridge error:', ex); }}
}}

function scrollToTop() {{
  if (window.gridScrollTo) return window.gridScrollTo('top');
  window.scrollTo({{top: 0, behavior: 'smooth'}});
}}

function updateParentPagination() {{
  // Parent pagination control removed — Streamlit handles Prev/Next natively
}}

function onImgLoad(img, sid) {{
  // If data-lazy-src is still pending, this onload was for the PLACEHOLDER,
  // not the real image. Ignore it: marking img-loaded here faded in the gray
  // placeholder, then the real src swap hard-popped over it (double flash),
  // and the placeholder's dimensions could add bogus Low-Resolution warnings.
  if (img.dataset.lazySrc) return;
  window._loadedImgs.add(img.getAttribute('src'));
  img.classList.remove('skeleton');
  img.classList.add('img-loaded');
  var wrap = img.closest('.card-img-wrap');
  if (wrap) wrap.classList.add('img-loaded');
  var w = img.naturalWidth, h = img.naturalHeight;
  var warns = [];
  if (w > 0 && h > 0) {{
    if (w < 250 || h < 250) warns.push('Low Resolution');
    var ratio = h / w;
    /* Advisory band only. Anything past the reject bounds has already been
       rejected server-side by check_image_stretched and is therefore not in
       this grid at all, so badging it here would be dead code — which is
       exactly what the old single-threshold version was: it drew badges at
       the same numbers that got the product rejected out of the grid.
       Kept in sync with ASPECT_* in streamlit_app.py. */
    if (ratio > {_ASPECT_ADVISORY_TALL} && ratio <= {_ASPECT_REJECT_TALL}) warns.push('Tall image — check');
    else if (ratio < {_ASPECT_ADVISORY_WIDE} && ratio >= {_ASPECT_REJECT_WIDE}) warns.push('Wide image — check');
  }}
  if (warns.length) addWarnings(sid, warns);
}}

var _lazyObserver = null;
function getLazyObserver() {{
  if (_lazyObserver) return _lazyObserver;
  if (!('IntersectionObserver' in window)) return null;
  _lazyObserver = new IntersectionObserver(function(entries) {{
    entries.forEach(function(entry) {{
      if (!entry.isIntersecting) return;
      var img = entry.target;
      if (img.dataset.lazySrc) {{
        img.src = img.dataset.lazySrc;
        delete img.dataset.lazySrc;
        _lazyObserver.unobserve(img);
      }}
    }});
  // 200px was tuned for a grid that never actually scrolled. The iframe used
  // to be as tall as its content (750px+ and growing with the card count), so
  // every card sat inside the iframe's own viewport and the observer fired for
  // all of them at once — the page behaved as if nothing was lazy. Clipping
  // the document to the iframe box to fix the batch bar turned that into real
  // lazy loading, which is correct but meant images only started fetching as
  // they came into view. A viewport-sized preload margin restores the "already
  // there when you scroll to it" feel without going back to fetching every
  // image on every page.
  }}, {{rootMargin: '1200px 0px', threshold: 0.01}});
  return _lazyObserver;
}}

function activateLazyImages() {{
  var observer = getLazyObserver();
  if (!observer) return;
  // Only observe images we haven't already handed to the observer — this used
  // to rescan and re-observe the whole document on every render chunk (O(n²)
  // with 500 cards/page).
  document.querySelectorAll('img.card-img[data-lazy-src]:not([data-lz-obs])').forEach(function(img) {{
    img.setAttribute('data-lz-obs', '1');
    observer.observe(img);
  }});
}}

function onImgError(img, sid) {{
  var card = CARDS.find(c => c.sid === sid);
  var realSrc = img.dataset.lazySrc || imgFor(card);
  if (!img.dataset.triedProxy && realSrc && realSrc.startsWith('http')) {{
    img.dataset.triedProxy = 'true';
    delete img.dataset.lazySrc;
    img.src = "https://wsrv.nl/?url=" + encodeURIComponent(realSrc);
    return;
  }}
  img.onerror = null;
  delete img.dataset.lazySrc;
  img.src = PLACEHOLDER;
  img.classList.add('img-loaded');
  if (!window._imageIssues[sid]) window._imageIssues[sid] = [];
  if (!window._imageIssues[sid].includes('Broken Image')) window._imageIssues[sid].push('Broken Image');
  addWarnings(sid, ['Broken Image']);
  var debugDiv = document.getElementById('debug-' + escapeHtml(sid));
  if (debugDiv) {{
    debugDiv.style.display = 'block';
    debugDiv.innerHTML = "<b>FAILED URL:</b><br>" + escapeHtml(realSrc);
  }}
}}

function addWarnings(sid, warns) {{
  var wrap = document.querySelector('#card-' + escapeHtml(sid) + ' .warn-wrap');
  if (!wrap) return;
  warns.forEach(w => {{
    var badge = document.createElement('span');
    badge.className = 'warn-badge';
    badge.textContent = w;
    wrap.appendChild(badge);
  }});
  if (!window._imageIssues[sid]) window._imageIssues[sid] = [];
  warns.forEach(w => {{ if (!window._imageIssues[sid].includes(w)) window._imageIssues[sid].push(w); }});
}}

function buildCardActionsHtml(safeSid, warnings, cardData) {{
  var card = cardData || {{}};
  var FLAG_MAP = {{
    'Wrong Category':         ['REJECT_WRONG_CAT',     LABELS.wrong_cat],
    'Category Check':         ['REJECT_WRONG_CAT',     LABELS.wrong_cat],
    'Missing COLOR':          ['REJECT_COLOR',          LABELS.missing_color],
    'Color Check':            ['REJECT_COLOR',          LABELS.missing_color],
    'Restricted Brand':       ['REJECT_BRAND',          LABELS.restr_brand],
    'Restricted brands':      ['REJECT_BRAND',          LABELS.restr_brand],
    'Prohibited':             ['REJECT_PROHIBITED',     LABELS.prohibited],
    'Prohibited products':    ['REJECT_PROHIBITED',     LABELS.prohibited],
    'Wrong Brand':            ['REJECT_WRONG_BRAND',    LABELS.wrong_brand],
    'Suspected Fake product': ['REJECT_FAKE',           LABELS.fake_prod],
    'Poor images':            ['REJECT_POOR_IMAGE',     LABELS.poor_img],
    'Image Quality Check':    ['REJECT_POOR_IMAGE',     LABELS.poor_img],
    'Brand Image Check':      ['REJECT_POOR_IMAGE',     LABELS.poor_img],
    'Product Warranty':       ['REJECT_WARRANTY',       'Product Warranty'],
    'Warranty Check':         ['REJECT_WARRANTY',       'Product Warranty'],
    'FDA':                    ['REJECT_FDA',            'FDA'],
    'Duplicate product':      ['REJECT_DUPLICATE',      'Duplicate Product'],
    'Wrong Variation':        ['REJECT_VARIATION',      'Wrong Variation'],
    'Variation Check':        ['REJECT_VARIATION',      'Wrong Variation'],
    'BRAND name repeated in NAME': ['REJECT_BRAND_IN_NAME', 'Brand in Name'],
    'Product Name Brand Name – Brand Repeated In Title':                  ['REJECT_BRAND_IN_NAME', 'Brand Repeated In Title'],
    'Product Name Brand Name – Inspired/Alternative Perfume Brand':       ['REJECT_BRAND_IN_NAME', 'Inspired/Alternative Perfume'],
    'Product Name Brand Name – Generic/Placeholder Brand':                ['REJECT_BRAND_IN_NAME', 'Generic/Placeholder Brand'],
    'Product Name Brand Name – High-End Brand Counterfeit Suspected':     ['REJECT_BRAND_IN_NAME', 'High-End Brand Counterfeit'],
    'Product Name Brand Name – Other':                                    ['REJECT_BRAND_IN_NAME', 'Brand in Name (Other)'],
    'Title Language Check - Not In English':                              ['REJECT_TITLE_LANG',    'Title Language'],
    'Title Language Check - Other':                                       ['REJECT_TITLE_LANG',    'Title Language (Other)'],
    'Suspected Fake Perfume':      ['REJECT_FAKE_PERFUME',   'Suspected Fake Perfume'],
    'Suspicious Discount':         ['REJECT_SUSPICIOUS_DISCOUNT', 'Suspicious Discount'],
    'Brand Image Mismatch':        ['REJECT_BRAND_MISMATCH', 'Brand Image Mismatch'],
    'Off-Platform Contact':        ['REJECT_OFFPLATFORM',    'Off-Platform Contact'],
    'Seller Not approved to sell Refurb':  ['REJECT_REFURB',         'Seller Not Approved (Refurb)'],
    'Seller Approve to sell books':        ['REJECT_BOOKS_SELLER',   'Seller Not Approved (Books)'],
    'Seller Approved to Sell Perfume':     ['REJECT_PERFUME_SELLER', 'Seller Not Approved (Perfume)'],
    'Perfume Tester':              ['REJECT_PERFUME_TESTER',  'Perfume Tester'],
    'Counterfeit Sneakers':        ['REJECT_SNEAKERS',        'Counterfeit Sneakers'],
    'Suspected counterfeit Jerseys': ['REJECT_JERSEYS',       'Counterfeit Jerseys'],
    'Unnecessary words in NAME':   ['REJECT_UNNECESSARY_WORDS', 'Unnecessary Words in Name'],
    'Single-word NAME':            ['REJECT_SINGLE_WORD',     'Single-word Name'],
    'Generic BRAND Issues':        ['REJECT_GENERIC_BRAND',   'Generic Brand Issues'],
    'Fashion brand issues':        ['REJECT_FASHION_BRAND',   'Fashion Brand Issues'],
    'Missing Weight/Volume':       ['REJECT_WEIGHT_VOL',      'Missing Weight/Volume'],
    'Incomplete Smartphone Name':  ['REJECT_SMARTPHONE_NAME', 'Incomplete Smartphone Name'],
    'Specs Inconsistency':         ['REJECT_SPECS_INCONSISTENCY', 'Specs Inconsistency'],
  }};
  var defaultCode  = 'REJECT_POOR_IMAGE';
  var defaultLabel = LABELS.poor_img;
  for (var i = 0; i < (warnings||[]).length; i++) {{
    var match = FLAG_MAP[warnings[i]];
    if (match) {{ defaultCode = match[0]; defaultLabel = match[1]; break; }}
  }}
  var opts = [
    ['REJECT_POOR_IMAGE',    LABELS.poor_img],
    ['REJECT_IMG_STRETCHED', 'Image Stretched'],
    ['REJECT_IMG_BLURRY',    'Image Blurry'],
    ['REJECT_IMG_MISMATCH',  'Image Mismatch'],
    ['REJECT_IMG_INFRINGING','Image Infringing'],
    ['REJECT_IMG_TOO_MANY',  'Image Too Many Things'],
    ['REJECT_WRONG_CAT',     escapeHtml(LABELS.wrong_cat)],
    ['REJECT_FAKE',          escapeHtml(LABELS.fake_prod)],
    ['REJECT_BRAND',         escapeHtml(LABELS.restr_brand)],
    ['REJECT_PROHIBITED',    escapeHtml(LABELS.prohibited)],
    ['REJECT_COLOR',         escapeHtml(LABELS.missing_color)],
    ['REJECT_WRONG_BRAND',   escapeHtml(LABELS.wrong_brand)],
    ['REJECT_FDA',           'FDA'],
    ['REJECT_DUPLICATE',     'Duplicate Product'],
    ['REJECT_BRAND_IN_NAME', 'Brand Repeated In Name'],
    ['REJECT_FAKE_PERFUME',  'Suspected Fake Perfume'],
    ['REJECT_BRAND_MISMATCH','Brand Image Mismatch'],
    ['REJECT_OFFPLATFORM',   'Off-Platform Contact'],
    ['REJECT_REFURB',        'Seller Not Approved (Refurb)'],
    ['REJECT_BOOKS_SELLER',  'Seller Not Approved (Books)'],
    ['REJECT_PERFUME_SELLER','Seller Not Approved (Perfume)'],
    ['REJECT_PERFUME_TESTER','Perfume Tester'],
    ['REJECT_SNEAKERS',      'Counterfeit Sneakers'],
    ['REJECT_JERSEYS',       'Counterfeit Jerseys'],
    ['REJECT_UNNECESSARY_WORDS', 'Unnecessary Words in Name'],
    ['REJECT_SINGLE_WORD',   'Single-word Name'],
    ['REJECT_GENERIC_BRAND', 'Generic Brand Issues'],
    ['REJECT_FASHION_BRAND', 'Fashion Brand Issues'],
    ['REJECT_WEIGHT_VOL',    'Missing Weight/Volume'],
    ['REJECT_SMARTPHONE_NAME','Incomplete Smartphone Name'],
    ['REJECT_SPECS_INCONSISTENCY', 'Specs Inconsistency'],
    ['REJECT_WARRANTY',      'Product Warranty'],
    ['REJECT_VARIATION',     'Wrong Variation'],
    ['REJECT_TITLE_LANG',    'Title Not in English'],
    ['REJECT_SUSPICIOUS_DISCOUNT', 'Suspicious Discount'],
    ['OTHER_CUSTOM',         'Other Reason (Custom)'],
  ];
  var optionsHtml = opts.map(function(o) {{
    return `<option value="${{o[0]}}">${{o[1]}}</option>`;
  }}).join('');

  var autoCommentHtml = '';
  if (defaultCode === 'REJECT_WRONG_CAT' && (card.ai_caption || card.suggested_cat || card.cat_reason)) {{
    var parts = [];
    if (card.cat_reason) parts.push(card.cat_reason);
    else if (card.ai_caption) parts.push(card.ai_caption);
    if (card.suggested_cat) parts.push('Suggested: ' + card.suggested_cat);
    var autoTxt = parts.join(' | ').slice(0, 250);
    autoCommentHtml = `<textarea class="auto-comment" id="ac-${{safeSid}}" onclick="event.stopPropagation()" rows="2" style="width:100%;font-size:10px;margin-top:4px;padding:4px 6px;border-radius:6px;border:1px solid #e5e7eb;resize:vertical;background:#fffbf5;color:#333;">${{escapeHtml(autoTxt)}}</textarea>`;
  }}

  return (
    `<div class="acts">` +
      `<button class="act-btn" onclick="event.stopPropagation();window.stageRejectWithComment('${{safeSid}}','${{defaultCode}}')">` +
        escapeHtml(defaultLabel) +
      `</button>` +
      `<select class="act-more" onchange="if(this.value){{event.stopPropagation();window.stageRejectWithComment('${{safeSid}}',this.value);this.value=''}}">` +
        `<option value="">${{escapeHtml(LABELS.more_options)}}</option>` +
        optionsHtml +
      `</select>` +
      autoCommentHtml +
    `</div>`
  );
}}

var UNNECESSARY_WORDS = {_js_json(support_files.get("unnecessary_words", []))};
var PROHIBITED_WORDS = {_js_json(support_files.get("prohibited_words", []))};

function getHighlightedName(card) {{
  var name = card.name;
  var warns = card.warnings || [];
  var words = [];
  if (warns.includes("Unnecessary words")) words = words.concat(UNNECESSARY_WORDS);
  if (warns.includes("Prohibited Words")) words = words.concat(PROHIBITED_WORDS);

  if (warns.includes("BRAND name repeated in NAME")) words.push(card.brand);

  if (words.length === 0) return card.name.length > 38 ? escapeHtml(card.name.slice(0,38)) + '\u2026' : escapeHtml(card.name);

  words.sort((a,b) => b.length - a.length);
  var regex = new RegExp('(' + words.map(w => w.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&')).join('|') + ')', 'gi');
  // Escape the name BEFORE inserting highlight <span> markup \u2014 product names are
  // seller-supplied data, so without this a name containing raw HTML/script would
  // execute in the reviewer's browser via innerHTML.
  var hName = escapeHtml(name).replace(regex, '<span class="hlt">$1</span>');

  return hName;
}}

function renderCard(card) {{
  var sid = card.sid;
  var safeSid = sid.replace(/'/g, "\\\\'");
  var isCommitted = sid in COMMITTED;
  var isStaged = sid in staged;
  var isSelected = sid in selected;
  var isPoorImgRej = isCommitted && POOR_IMG_SIDS.has(sid);
  var isBrandImgRej = isCommitted && (String(COMMITTED[sid]).includes('Brand Image Check'));
  var cls = 'card'
    + (isCommitted ? ' committed-rej' + (isPoorImgRej ? ' poor-img-rej' : '') + (isBrandImgRej ? ' brand-image-rej' : '') : isStaged ? ' staged-rej' : '')
    + (card.is_manual_review && !isCommitted && !isStaged && !isSelected ? ' manual-review' : '')
    + (isSelected ? ' selected' : '')
    + (card.is_zip ? ' zip-card' : '');

  var _src = imgFor(card);
  var safeImgSrcForHtml = _src ? _src.replace(/'/g, "%27").replace(/"/g, "%22") : PLACEHOLDER;
  var shortName = card.name.length > 38 ? escapeHtml(card.name.slice(0,38)) + '\u2026' : escapeHtml(card.name);
  var warnHtml = (card.warnings || []).map(w => `<span class="warn-badge">${{escapeHtml(w)}}</span>`).join('');
  // Purple = the ZIP observed it. It never rejects on this, so on its own it
  // is information, not a verdict.
  if (card.is_duplicate) warnHtml += `<span class="warn-badge" style="background:#7c3aed;color:#fff;font-weight:800;" title="The ZIP marked this a duplicate (same seller + product name). The ZIP does not reject on it.">⧉ DUPLICATE</span>`;
  // Blue = our own check rejected it. Different colour on purpose: this is the
  // one that changed the verdict. Both badges together mean the two agree.
  if (card.sys_duplicate) warnHtml += `<span class="warn-badge" style="background:#1d4ed8;color:#fff;font-weight:800;" title="${{card.is_duplicate ? 'Rejected by our duplicate check — the ZIP flagged it too.' : 'Rejected by our duplicate check. The ZIP did not flag this one.'}} One listing per group is kept; the rest are rejected.">⧉ DUPLICATE — system</span>`;
  if (card.is_manual_review) {{
    var mrText = card.qc_skip_reason ? `👁 MANUAL REVIEW: ${{escapeHtml(card.qc_skip_reason)}}` : `👁 MANUAL REVIEW`;
    warnHtml += `<span class="warn-badge" style="background:#dc2626;color:#fff;font-weight:800;" title="${{escapeHtml(card.qc_skip_reason || 'Manual Review')}}">${{mrText}}</span>`;
  }}
  if (card.color_mismatch) warnHtml += `<span class="warn-badge" style="background:#b45309;color:#fff;" title="${{escapeHtml(card.color_mismatch)}}">⚠ Color Mismatch</span>`;
  // Several colours on one listing usually means one photo showing several
  // products, which is what the image review is looking for.
  if (card.multi_colour) warnHtml += `<span class="warn-badge" style="background:#7c3aed;color:#fff;" title="Declared colours: ${{escapeHtml(card.multi_colour)}}">⚠ Too many things?</span>`;
  // A sneaker claiming a protected brand. No rule can tell these from the
  // genuine article, so the call belongs to whoever is looking at the photo.
  if (card.brand_claim) warnHtml += `<span class="warn-badge" style="background:#0f766e;color:#fff;" title="Claims ${{escapeHtml(card.brand_claim)}} — check the photo: no text rule can tell a fake from the real thing here">👟 ${{escapeHtml(card.brand_claim)}}?</span>`;
  // A perfume naming somebody's MODEL rather than their house. Not rejected —
  // many models are ordinary words and these brands sell their own catalogue,
  // so the call belongs to whoever is looking at the bottle.
  if (card.perfume_claim) warnHtml += `<span class="warn-badge" style="background:#7e22ce;color:#fff;" title="${{escapeHtml(card.perfume_claim)}} — check the bottle: the matched word may just be this house's own product name">🧴 Perfume?</span>`;
  var priceText = String(card.price || '').trim();
  var priceHtml = priceText ? `<div class="price-badge">${{escapeHtml(priceText)}}</div>` : '';

  var colorLabel = card.is_manual_review ? 'Color-M' : 'Color';
  var colorHtml = card.color ? `<div class="co" title="${{colorLabel}}: ${{escapeHtml(card.color)}}">${{colorLabel}}: ${{escapeHtml(card.color)}}</div>` : '';
  var aiColorHtml = (card.color_ai && card.color_ai !== card.color) ? `<div class="ai-color-pill" title="AI detected: ${{escapeHtml(card.color_ai)}}">🎨 AI Color: ${{escapeHtml(card.color_ai)}}</div>` : '';
  var colorMismatchHtml = card.color_mismatch ? `<div class="co" style="color:#b45309;border-color:#fde68a;" title="${{escapeHtml(card.color_mismatch)}}">⚠ ${{escapeHtml(card.color_mismatch)}}</div>` : '';
  var catReasonHtml = (card.cat_reason && (card.warnings||[]).some(w => w.includes('Category'))) ?
    `<div class="co" style="color:#9333ea;font-size:10px;white-space:normal;line-height:1.3;" title="${{escapeHtml(card.cat_reason)}}">${{escapeHtml(card.cat_reason.length > 80 ? card.cat_reason.slice(0,80)+'…' : card.cat_reason)}}</div>` : '';
  // Generic reason snippet for any other flag that has a Comment (Brand Image
  // Mismatch, Off-Platform Contact, Restricted Brands, etc) — only shown when
  // the richer category-specific reason above isn't already covering it, so
  // reviewers can see WHY a card is flagged without opening the flag table.
  var flagCommentHtml = (!catReasonHtml && card.flag_comment) ?
    `<div class="co" style="color:#b91c1c;font-size:10px;white-space:normal;line-height:1.3;" title="${{escapeHtml(card.flag_comment)}}">${{escapeHtml(card.flag_comment.length > 90 ? card.flag_comment.slice(0,90)+'…' : card.flag_comment)}}</div>` : '';
  var suggestedCatHtml = card.suggested_cat ? `<div class="co" style="color:#0369a1;" title="AI suggests: ${{escapeHtml(card.suggested_cat)}}">→ ${{escapeHtml(card.suggested_cat.length > 50 ? card.suggested_cat.slice(0,50)+'…' : card.suggested_cat)}}</div>` : '';
  var aiBrandHtml = (card.brand_detected && card.brand_detected.toLowerCase() !== card.brand.toLowerCase()) ? `<div class="ai-brand-pill" title="AI detected brand: ${{escapeHtml(card.brand_detected)}}">🏷 AI Brand: ${{escapeHtml(card.brand_detected)}}</div>` : '';
  var brandDetectedHtml = (isBrandImgRej && card.brand_detected) ? '<div class="co" style="background:#E8F5E9;color:#2E7D32;border:1px solid #C8E6C9;" title="Brand Detected: ' + escapeHtml(card.brand_detected) + '">Detected Brand: ' + escapeHtml(card.brand_detected) + '</div>' : '';
  var zipBadgeHtml = card.is_zip ? '<span style="background:linear-gradient(135deg,#3b82f6,#1d4ed8);color:#fff;font-size:10px;font-weight:900;padding:2px 8px;border-radius:6px;box-shadow:0 2px 4px rgba(0,0,0,0.15);margin-left:8px;display:inline-block;">ZIP</span>' : '';
  if (card.zip_override) {{
    var overrideType = card.zip_override === 'color' ? 'Color' : (card.zip_override === 'volume' ? 'Weight/Volume' : 'Warranty');
    zipBadgeHtml += '<span style="background:linear-gradient(135deg,#10b981,#059669);color:#fff;font-size:10px;font-weight:900;padding:2px 8px;border-radius:6px;box-shadow:0 2px 4px rgba(0,0,0,0.15);margin-left:4px;display:inline-block;" title="Auto-approved by main ' + overrideType.toLowerCase() + ' check">🔓 ' + overrideType + ' Overridden</span>';
  }}

  var zoomHtml = `<button class="zoom-btn" onclick="event.stopPropagation();showZoom('${{safeSid}}', event)" title="Preview">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
      <line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>
    </svg></button>`;

  var imgIdx = CARDS.indexOf(card);
  // Already loaded once this session → render it fully visible immediately:
  // direct src, no shimmer, no 0.4s fade. This is what stops the whole grid
  // from flickering on page turns when the browser already has the images.
  var isCachedImg = window._loadedImgs.has(safeImgSrcForHtml);
  // Four rows eager, not two: the iframe is now sized to the real viewport
  // (calc(100vh - 190px)), so on a large screen more than two rows are visible
  // before any scrolling happens — at two rows the bottom of the first screen
  // was still filling in.
  var isEager = isCachedImg || imgIdx < {cols_per_row * 4};
  var loadingAttr = isEager ? 'eager' : 'lazy';
  var priorityAttr = isEager ? 'fetchpriority="high"' : 'fetchpriority="low"';
  var imgSrcAttr = isEager
    ? `src="${{safeImgSrcForHtml}}"`
    : `src="${{PLACEHOLDER}}" data-lazy-src="${{safeImgSrcForHtml}}"`;
  var loadedCls = isCachedImg ? ' img-loaded' : '';

  var overlayHtml = '', actHtml = '';
    if (isCommitted) {{
      var rejReason = (COMMITTED[sid]||'').replace(/_/g,' ');
      var actionLabel = isBrandImgRej ? LABELS.approve : LABELS.undo;
      var extraInfo = '';
      if (isBrandImgRej && card.brand_detected) {{
        extraInfo = `<div style="margin-top:auto; padding:6px 8px; background:rgba(211,47,47,0.75); border-radius:0 0 8px 8px; color:#fff; font-weight:800; font-size:12px; width:100%; text-align:center; position:absolute; bottom:0; left:0;">Detected Brand: ${{escapeHtml(card.brand_detected)}}</div>`;
      }}

      if (isBrandImgRej) {{
        overlayHtml = `<div class="rej-overlay">
          <div class="rej-badge">${{escapeHtml(LABELS.rejected)}}</div>
          <div class="rej-label">${{escapeHtml(rejReason)}}</div>
          ${{extraInfo}}
        </div>`;
        actHtml = `<div class="acts">
          <button class="act-btn" style="background:#4CAF50; color:#fff; flex:1;" onclick="event.stopPropagation();window.undoReject('${{safeSid}}')">${{escapeHtml(actionLabel)}}</button>
        </div>`;
      }} else {{
        overlayHtml = `<div class="rej-overlay">
          <div class="rej-badge">${{escapeHtml(LABELS.rejected)}}</div>
          <div class="rej-label">${{escapeHtml(rejReason)}}</div>
          <button class="undo-btn" onclick="event.stopPropagation();window.undoReject('${{safeSid}}')">${{escapeHtml(actionLabel)}}</button>
          ${{extraInfo}}
        </div>`;
      }}
  }} else if (isStaged) {{
    overlayHtml = `<div class="rej-overlay staged">
      <div class="rej-badge pending">${{escapeHtml(LABELS.rejected)}}</div>
      <div class="rej-label">Pending reason:<br>${{escapeHtml((staged[sid]||'').replace(/_/g,' '))}}</div>
      <button class="undo-btn" onclick="event.stopPropagation();window.clearStaged('${{safeSid}}')">${{escapeHtml(LABELS.clear_sel)}}</button>
    </div>`;
  }} else {{
    actHtml = buildCardActionsHtml(safeSid, card.warnings, card);
  }}

  var dataAttrs = 'data-sid="' + escapeHtml(String(card.data_sid||'')) + '" data-name="' + escapeHtml(String(card.data_name||'')) + '" data-brand="' + escapeHtml(String(card.data_brand||'')) + '" data-cat="' + escapeHtml(String(card.data_cat||'')) + '"';
  return `<div class="${{cls}}" id="card-${{escapeHtml(sid)}}" ${{dataAttrs}} tabindex="0" onclick="window.toggleSelect('${{safeSid}}',event)">
    <div class="card-img-wrap${{loadedCls}}">
      ${{priceHtml}}
      <div class="warn-wrap">${{warnHtml}}</div>
      <div id="debug-${{escapeHtml(sid)}}" class="debug-hud"></div>
      <img class="card-img-placeholder" src="${{PLACEHOLDER}}" alt="">
      <img class="card-img${{loadedCls}}" ${{imgSrcAttr}} decoding="async" loading="${{loadingAttr}}" ${{priorityAttr}} referrerpolicy="no-referrer"
            onload="onImgLoad(this,'${{safeSid}}')" onerror="onImgError(this,'${{safeSid}}')">
      ${{zoomHtml}}
      ${{overlayHtml}}
      <div class="tick">\u2714</div>
    </div>
    <div class="meta">
      <div class="nm" title="${{escapeHtml(card.name)}}">${{getHighlightedName(card)}}</div>
      <div class="br" title="${{escapeHtml(card.brand)}}">Brand${{card.is_manual_review ? '-M' : ''}}: ${{escapeHtml(card.brand)}}</div>
      ${{aiBrandHtml}}
      <div class="ct" title="${{escapeHtml(card.cat)}}">Category${{card.is_manual_review ? '-M' : ''}}: ${{escapeHtml(card.cat)}}</div>
      <div class="sl" title="${{escapeHtml(card.seller)}}" style="display:flex;align-items:center;">
        <span style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">Seller: ${{escapeHtml(card.seller)}}</span>
        ${{zipBadgeHtml}}
      </div>
      ${{colorHtml}}
      ${{aiColorHtml}}
      ${{colorMismatchHtml}}
      ${{catReasonHtml}}
      ${{flagCommentHtml}}
      ${{suggestedCatHtml}}
      ${{brandDetectedHtml}}
    </div>
    ${{actHtml}}
  </div>`;
}}

window.showZoom = function(sid, event) {{
  var tooltip = document.getElementById('zoom-tooltip');
  if (tooltip.style.display === 'block' && window.currentZoomSid === sid) {{
    closeZoom();
    return;
  }}
  var card = CARDS.find(c => c.sid === sid);
  if (!card) return;
  var img = document.getElementById('tooltip-img');
  img.src = imgFor(card);
  img.onerror = function() {{ img.src = PLACEHOLDER; img.onerror = null; }};
  document.getElementById('zoom-backdrop').style.display = 'block';
  tooltip.style.display = 'block';
  window.currentZoomSid = sid;
  var tw = 360, th = 360;
  var x = event.clientX, y = event.clientY;
  var vw = window.innerWidth, vh = window.innerHeight;
  var left = x + 15;
  if (left + tw > vw - 10) left = x - tw - 15;
  if (left < 10) left = 10;
  var top = y - (th / 2);
  if (top < 10) top = 10;
  if (top + th > vh - 10) top = vh - th - 10;
  tooltip.style.position = 'fixed';
  tooltip.style.left = left + 'px';
  tooltip.style.top = top + 'px';
}};

window.closeZoom = function() {{
  document.getElementById('zoom-tooltip').style.display = 'none';
  document.getElementById('zoom-backdrop').style.display = 'none';
  window.currentZoomSid = null;
}};

window.zoomMove = function(dir) {{
  if (!window.currentZoomSid) return;
  var displayCards = getDisplayCards();
  var idx = displayCards.findIndex(c => c.sid === window.currentZoomSid);
  if (idx < 0) return;
  var nextIdx = idx + dir;
  if (nextIdx < 0) nextIdx = displayCards.length - 1;
  if (nextIdx >= displayCards.length) nextIdx = 0;
  var nextSid = displayCards[nextIdx].sid;

  var el = document.getElementById('card-' + escapeHtml(nextSid));
  if (el) el.scrollIntoView({{ block: 'nearest', inline: 'nearest' }});

  var fakeEvent = {{ clientX: window.innerWidth/2, clientY: window.innerHeight/2 }};
  window.currentZoomSid = null;
  window.showZoom(nextSid, fakeEvent);
}};

document.addEventListener('click', function(e) {{
  var tooltip = document.getElementById('zoom-tooltip');
  if (tooltip.style.display === 'block' && !tooltip.contains(e.target) && !e.target.closest('.zoom-btn')) {{
    closeZoom();
  }}
}});

function updateSelCount() {{
  var pendingCount = (Object.keys(selected).length + Object.keys(staged).length);
  var pendingText = pendingCount + ' ' + LABELS.items_pending;
  document.querySelectorAll('.sel-count-text').forEach(el => el.textContent = pendingText);
  updateParentPagination();
}}

window._currentFilter = window._currentFilter || '';

function _shortText(text, maxLen) {{
  var t = String(text || '').trim();
  if (!t) return '';
  return t.length > maxLen ? t.slice(0, maxLen) + '\u2026' : t;
}}

function _readMultiSelectValues(id) {{
  var el = document.getElementById(id);
  if (!el) return [];
  return Array.from(el.selectedOptions || []).map(function(opt) {{ return opt.value; }}).filter(Boolean);
}}

function _setMultiSelectValues(id, values) {{
  var el = document.getElementById(id);
  if (!el) return;
  var set = new Set(values || []);
  Array.from(el.options).forEach(function(opt) {{ opt.selected = set.has(opt.value); }});
}}

function _pageSizeValue() {{
  return Math.max(1, parseInt(PAGE_STATE.pageSize || DEFAULT_PAGE_SIZE, 10) || DEFAULT_PAGE_SIZE);
}}

function getSortedCards() {{
  var sort = window._currentSort;
  if (!sort) return CARDS;
  var ISSUE_MAP = {{ 'low_res':'Low Resolution','tall':'Tall (Screenshot?)','wide':'Wide Aspect','broken':'Broken Image' }};
  var sorted = CARDS.slice();
  if (sort === 'no_issue') {{
    sorted.sort(function(a,b) {{ return ((window._imageIssues[a.sid]||[]).length>0?1:0) - ((window._imageIssues[b.sid]||[]).length>0?1:0); }});
  }} else if (sort === 'most_flagged') {{
    sorted.sort(function(a,b) {{ return (b.warnings||[]).length - (a.warnings||[]).length; }});
  }} else {{
    var target = ISSUE_MAP[sort] || sort;
    sorted.sort(function(a,b) {{ return ((window._imageIssues[a.sid]||[]).includes(target)?0:1) - ((window._imageIssues[b.sid]||[]).includes(target)?0:1); }});
  }}
  return sorted;
}}

function getBaseFilteredCards() {{
  var cards = getSortedCards();
  var f = window._currentFilter;
  if (f) {{
    if (f === 'committed') cards = cards.filter(function(c) {{ return c.sid in COMMITTED; }});
    else if (f === 'brand_ocr') cards = cards.filter(function(c) {{ return c.sid in COMMITTED && (COMMITTED[c.sid]||'').includes('Brand Image Check'); }});
    else if (f === 'no_flags') cards = cards.filter(function(c) {{ return !(c.warnings||[]).length && !(c.sid in COMMITTED) && !(c.sid in staged); }});
    // Either source — filtering to the ZIP's observation alone would hide the
    // ones our own check caught and the ZIP missed.
    else if (f === 'duplicates') cards = cards.filter(function(c) {{ return c.is_duplicate || c.sys_duplicate; }});
    else if (f === 'manual_review') cards = cards.filter(function(c) {{ return c.is_manual_review; }});
    else if (f === 'color_mismatch') cards = cards.filter(function(c) {{ return !!c.color_mismatch; }});
    else cards = cards.filter(function(c) {{
      var inWarnings = (c.warnings||[]).some(function(w) {{ return w === f; }});
      var inCommitted = c.sid in COMMITTED && (COMMITTED[c.sid]||'').replace(/_/g,' ').toLowerCase() === f.replace(/_/g,' ').toLowerCase();
      return inWarnings || inCommitted;
    }});
  }}
  var q = (PAGE_STATE.search || '').toLowerCase().trim();
  if (q) {{
    cards = cards.filter(function(c) {{
      var text = [c.name, c.brand, c.sid, c.cat, c.seller].join(' ').toLowerCase();
      return text.includes(q);
    }});
  }}
  if (PAGE_STATE.sellers && PAGE_STATE.sellers.length) {{
    var sellerSet = new Set(PAGE_STATE.sellers);
    cards = cards.filter(function(c) {{ return sellerSet.has(c.seller); }});
  }}
  if (PAGE_STATE.categories && PAGE_STATE.categories.length) {{
    var catSet = new Set(PAGE_STATE.categories);
    cards = cards.filter(function(c) {{ return catSet.has(c.cat); }});
  }}
  return cards;
}}

function getDisplayCards() {{
  var cards = getBaseFilteredCards();
  var pageSize = _pageSizeValue();
  var totalPages = Math.max(1, Math.ceil(cards.length / pageSize));
  if (PAGE_STATE.page > totalPages) PAGE_STATE.page = totalPages;
  if (PAGE_STATE.page < 1) PAGE_STATE.page = 1;
  var start = (PAGE_STATE.page - 1) * pageSize;
  return cards.slice(start, start + pageSize);
}}

function _updateFilterSummary(filteredCount) {{
  var sellerCount = (PAGE_STATE.sellers || []).length;
  var catCount = (PAGE_STATE.categories || []).length;
  var searchActive = !!(PAGE_STATE.search || '').trim();
  var searchCount = document.getElementById('search-count');
  if (searchCount) searchCount.textContent = searchActive ? 'Active' : '0';
  var sellerCountEl = document.getElementById('seller-selected-count');
  if (sellerCountEl) sellerCountEl.textContent = 'Selected: ' + sellerCount;
  var catCountEl = document.getElementById('category-selected-count');
  if (catCountEl) catCountEl.textContent = 'Selected: ' + catCount;
  var pageCountEl = document.getElementById('page-count');
  if (pageCountEl) pageCountEl.textContent = 'Page ' + PAGE_STATE.page;
  var summaryEl = document.getElementById('filter-summary');
  if (summaryEl) {{
    var parts = [];
    if (searchActive) parts.push('Name: ' + _shortText(PAGE_STATE.search, 24));
    parts.push('Seller: ' + sellerCount + ' selected');
    parts.push('Category: ' + catCount + ' selected');
    summaryEl.textContent = parts.join(' | ');
  }}
}}

function _updatePageControls(totalCount) {{
  var pageSize = _pageSizeValue();
  var totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  if (PAGE_STATE.page > totalPages) PAGE_STATE.page = totalPages;
  if (PAGE_STATE.page < 1) PAGE_STATE.page = 1;
  var pageInfo = document.getElementById('page-info');
  if (pageInfo) pageInfo.textContent = 'Page ' + PAGE_STATE.page + ' / ' + totalPages;
  var prev = document.getElementById('page-prev');
  var next = document.getElementById('page-next');
  if (prev) prev.disabled = PAGE_STATE.page <= 1;
  if (next) next.disabled = PAGE_STATE.page >= totalPages;
}}

window.goPage = function(delta) {{
  var total = getBaseFilteredCards().length;
  var totalPages = Math.max(1, Math.ceil(total / _pageSizeValue()));
  PAGE_STATE.page = Math.max(1, Math.min(totalPages, PAGE_STATE.page + delta));
  renderAll();
}};

window.resetAllFilters = function() {{
  PAGE_STATE.search = '';
  PAGE_STATE.sellers = [];
  PAGE_STATE.categories = [];
  PAGE_STATE.page = 1;
  _setMultiSelectValues('seller-filter', []);
  _setMultiSelectValues('category-filter', []);
  var searchEl = document.getElementById('grid-search');
  if (searchEl) searchEl.value = '';
  renderAll();
}};

window._syncFilterState = function() {{
  PAGE_STATE.search = (document.getElementById('grid-search') || {{ value: '' }}).value || '';
  PAGE_STATE.sellers = _readMultiSelectValues('seller-filter');
  PAGE_STATE.categories = _readMultiSelectValues('category-filter');
  var pageSizeSel = document.getElementById('page-size-sel');
  if (pageSizeSel) PAGE_STATE.pageSize = parseInt(pageSizeSel.value, 10) || DEFAULT_PAGE_SIZE;
  PAGE_STATE.page = 1;
  renderAll();
}};

window.applySort = function(val) {{
  window._currentSort = val;
  ['sort-sel-top','sort-sel-bottom'].forEach(function(id) {{ var el=document.getElementById(id); if(el) el.value=val; }});
  renderAll();
}};

window.applyFilter = function(val) {{
  window._currentFilter = val;
  ['filter-sel-top','filter-sel-bottom'].forEach(function(id) {{ var el=document.getElementById(id); if(el) el.value=val; }});
  renderAll();
}};

var _renderSeq = 0;
function renderAll() {{
  var seq = ++_renderSeq;
  var cards = getDisplayCards();
  var countEl = document.getElementById('grid-count');
  if (countEl) countEl.textContent = cards.length + ' products' + (window._currentFilter ? ' (filtered)' : '');

  var grid = document.getElementById('card-grid');

  // Drop observations on the nodes we're about to replace so the observer
  // doesn't accumulate dead entries across page turns.
  if (_lazyObserver) _lazyObserver.disconnect();

  if (cards.length === 0) {{
    var hasFilters = !!(PAGE_STATE.search || '').trim() || (PAGE_STATE.sellers||[]).length || (PAGE_STATE.categories||[]).length || window._currentFilter;
    grid.innerHTML = '<div class="empty-state"><div><div class="title">No products match your filters</div>' +
      '<div class="desc">' + (hasFilters ? 'Try clearing the search, seller, or category filters.' : 'There are no products to show here.') + '</div></div>' +
      (hasFilters ? '<div class="actions"><button class="toolbar-btn small" onclick="window.resetAllFilters()">Clear filters</button></div>' : '') +
      '</div>';
    return;
  }}

  // Build the page's HTML in rAF-spaced chunks (keeps the main thread
  // responsive for 500-card pages) but insert it in ONE swap at the end.
  // The old approach cleared the grid immediately (blank flash + page height
  // collapse + scroll jump) and then painted 50 cards per frame — 10 visible
  // "waves" of cards popping in on every page turn.
  var chunkSize = 50;
  var idx = 0;
  var parts = [];

  function buildChunk() {{
    if (seq !== _renderSeq) return; // aborted by newer render
    var chunk = cards.slice(idx, idx + chunkSize);
    parts.push(chunk.map(renderCard).join(''));
    idx += chunkSize;
    if (idx < cards.length) {{
      requestAnimationFrame(buildChunk);
    }} else {{
      grid.innerHTML = parts.join('');
      activateLazyImages();
      updateSelCount();
    }}
  }}

  buildChunk();
}}

function replaceCard(sid) {{
  var el = document.getElementById('card-' + escapeHtml(sid));
  if (!el) return;
  var card = CARDS.find(c => c.sid === sid);
  if (card) {{ var t = document.createElement('div'); t.innerHTML = renderCard(card); el.replaceWith(t.firstElementChild); activateLazyImages(); }}
}}

window.doSelectAll = function() {{
  // Mark all non-staged cards as selected without a full renderAll().
  // renderAll() rebuilds ALL card HTML which blocks the main thread for 500+ cards.
  // Instead we just flip the CSS class on existing DOM nodes — O(n) DOM attr set vs O(n) innerHTML rebuild.
  var grid = document.getElementById('card-grid');
  CARDS.forEach(function(c) {{
    if (c.sid in staged) return;
    selected[c.sid] = true;
    var el = grid ? grid.querySelector('#card-' + escapeHtml(c.sid)) : null;
    if (el && !el.classList.contains('selected')) el.classList.add('selected');
  }});
  updateSelCount();
}};

window.toggleSelect = function(sid, e) {{
  var t = e && e.target;
  if (t && (t.tagName === 'SELECT' || t.tagName === 'OPTION' || t.tagName === 'BUTTON' || t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.closest('select,button,input,textarea,a'))) return;
  if (sid in staged) delete staged[sid];
  else if (sid in selected) delete selected[sid];
  else selected[sid] = true;
  replaceCard(sid); updateSelCount();
}};

window.stageRejectWithComment = function(sid, r) {{
  var safeSid = sid.replace(/'/g, "\\\\'");
  var ta = document.getElementById('ac-' + safeSid);
  if (ta && ta.value.trim()) {{
    window._autoComments = window._autoComments || {{}};
    window._autoComments[sid] = ta.value.trim();
  }}
  window.stageReject(sid, r);
}};

window.stageReject = function(sid, r) {{
  var currentCard = CARDS.find(c => c.sid === sid);
  var toStage = [sid];

  if (currentCard && (r === 'REJECT_POOR_IMAGE' || r.startsWith('REJECT_IMG_'))) {{
      CARDS.forEach(c => {{
          if (c.sid !== sid && (imgFor(c) === imgFor(currentCard) || (c.hash && c.hash === currentCard.hash))) {{
              toStage.push(c.sid);
          }}
      }});
  }}

  if (r === 'REJECT_WRONG_CAT' && currentCard) {{
      var nameTokens = currentCard.name.toLowerCase().split(/[^\\w]+/).filter(w => w.length > 4);
      if (nameTokens.length > 0) {{
          CARDS.forEach(c => {{
              if (c.sid !== sid && !(c.sid in staged)) {{
                  var cName = c.name.toLowerCase();
                  var matchCount = nameTokens.filter(t => cName.includes(t)).length;
                  if (matchCount >= 2 || (nameTokens.length === 1 && cName.includes(nameTokens[0]))) {{
                      addWarnings(c.sid, ["Potential Category Issue"]);
                      var el = document.getElementById('card-' + escapeHtml(c.sid));
                      if (el) {{
                          el.style.border = '2px solid #d97706';
                          setTimeout(() => {{ if (el) el.style.border = ''; }}, 4000);
                      }}
                  }}
              }}
          }});
      }}
  }}

  if (r === 'OTHER_CUSTOM') {{
    showCustomReasonPanel(function(cmt) {{
      if (!cmt) return;
      var reason = "Other Reason (Custom): " + cmt;
      toStage.forEach(s => {{
          if (s in selected) delete selected[s];
          staged[s] = reason;
          replaceCard(s);
      }});
      updateSelCount();
    }});
    return;
  }}

  toStage.forEach(s => {{
      if (s in selected) delete selected[s];
      staged[s] = r;
      replaceCard(s);
  }});
  updateSelCount();
}};

window.clearStaged = function(sid) {{
    delete staged[sid];
    var cardEl = document.getElementById('card-' + escapeHtml(sid));
    if (cardEl) {{
        cardEl.classList.remove('staged-rej');
        var overlay = cardEl.querySelector('.rej-overlay.staged');
        if (overlay) overlay.remove();
    }}
    updateSelCount();
}};

window.undoReject = function(sid) {{
  delete COMMITTED[sid];
  window._pendingUndos[sid] = true;
  if (sid in selected) delete selected[sid];

  var safeSid = sid.replace(/'/g, "\\\\'");
  var cardEl = document.getElementById('card-' + escapeHtml(sid));

  if (cardEl) {{
      cardEl.classList.remove('committed-rej', 'poor-img-rej');

      var overlay = cardEl.querySelector('.rej-overlay');
      if (overlay) overlay.remove();

      var acts = cardEl.querySelector('.acts');
      if (acts) acts.remove();
      var _c = CARDS.find(c=>c.sid===safeSid)||{{}};
      cardEl.insertAdjacentHTML('beforeend', buildCardActionsHtml(safeSid, _c.warnings, _c));

      cardEl.classList.add('undo-processing');
  }}

  updateSelCount();

  try {{
    var fe = window.frameElement;
    if (fe) {{
      fe.dataset.pinnedHeight = fe.offsetHeight;
      fe.style.setProperty('min-height', fe.offsetHeight + 'px', 'important');
      if (fe.parentElement) {{
          fe.parentElement.style.setProperty('min-height', fe.offsetHeight + 'px', 'important');
      }}
    }}
  }} catch(e) {{}}

  if (window._undoTimer) clearTimeout(window._undoTimer);
  window._undoTimer = setTimeout(function() {{
    var payload = Object.assign({{}}, window._pendingUndos);
    window._pendingUndos = {{}};
    if (!Object.keys(payload).length) return;

    requestAnimationFrame(function() {{
      requestAnimationFrame(function() {{
        sendMsg('undo', payload);

        setTimeout(function() {{
          try {{
            var fe = window.frameElement;
            if (fe) {{
              fe.style.removeProperty('min-height');
              delete fe.dataset.pinnedHeight;
              if (fe.parentElement) fe.parentElement.style.removeProperty('min-height');
            }}
          }} catch(e) {{}}
          document.querySelectorAll('.card.undo-processing').forEach(function(c) {{
            c.classList.remove('undo-processing');
          }});
        }}, 1000);
      }});
    }});
  }}, 400);
}};

// pos is vestigial — there was a second copy of these controls in a top bar,
// and every call site passes 'bottom' now. Kept in the signature so the inline
// onclick in the markup does not have to change; ignored.
window.doBatchReject = function(pos) {{
  var sel = document.getElementById('batch-reason-bottom');
  if (!sel) return;
  var br = sel.value;
  if (br === 'OTHER_CUSTOM') {{
    showCustomReasonPanel(function(cmt) {{
      if (!cmt) {{ sel.value = "REJECT_POOR_IMAGE"; sel.dispatchEvent(new Event('change', {{bubbles: true}})); return; }}
      _applyBatchReject("Other Reason (Custom): " + cmt);
      sel.value = "REJECT_POOR_IMAGE";
      sel.dispatchEvent(new Event('change', {{bubbles: true}}));
    }});
    return;
  }}
  _applyBatchReject(br);
}};

function _applyBatchReject(br) {{
  var payload = {{}}, count = 0;
  var autoC = window._autoComments || {{}};
  for (var s in staged) {{ payload[s] = staged[s]; count++; }}
  for (var s in selected) {{
    payload[s] = br; count++;
  }}
  var commentPayload = {{}};
  for (var s in payload) {{ if (autoC[s]) commentPayload[s] = autoC[s]; }}
  if (Object.keys(commentPayload).length) sendMsg('reject_comments', commentPayload);
  if (count === 0) {{
    for (var s in selected) delete selected[s];
    for (var s in staged) delete staged[s];
    updateSelCount();
    return;
  }}
  var allSids = Object.assign({{}}, selected, staged);
  for (var s in payload) {{ COMMITTED[s] = payload[s]; }}
  for (var s in allSids) {{ delete selected[s]; delete staged[s]; }}
  // Show overlay — keep it up for 90 s so it stays visible while Streamlit processes.
  // The page will reload (st.rerun) when done, which naturally removes the overlay.
  showGhostOverlay('Applying rejections…', 90000);
  // Update card classes directly instead of full renderAll() to avoid blocking the
  // main thread for 500 cards. Streamlit will do a full page reload after processing.
  var grid = document.getElementById('card-grid');
  for (var s in payload) {{
    var el = grid ? grid.querySelector('#card-' + escapeHtml(s)) : null;
    if (!el) continue;
    el.classList.remove('selected', 'staged-rej');
    el.classList.add('committed-rej');
    // Dim the card visually so the user gets instant feedback
    el.style.opacity = '0.55';
    // One-shot ring so a bulk reject reads as an event, not a silent
    // repaint. Removed on animationend so a later change can replay it.
    el.classList.remove('just-changed-rej');
    void el.offsetWidth;  // restart the animation
    el.classList.add('just-changed-rej');
    el.addEventListener('animationend', function handler() {{
      el.classList.remove('just-changed-rej');
      el.removeEventListener('animationend', handler);
    }});
  }}
  updateSelCount();
  sendMsg('reject', payload);
}}

var _customReasonCallback = null;
function showCustomReasonPanel(callback) {{
  _customReasonCallback = callback;
  var panel = document.getElementById('custom-reason-panel');
  var input = document.getElementById('custom-reason-input');
  input.value = '';
  panel.style.display = 'block';
  setTimeout(function() {{ input.focus(); }}, 50);
}}
function confirmCustomReason() {{
  var input = document.getElementById('custom-reason-input');
  var val = input.value.trim();
  document.getElementById('custom-reason-panel').style.display = 'none';
  if (_customReasonCallback) {{ _customReasonCallback(val); _customReasonCallback = null; }}
}}
function cancelCustomReason() {{
  document.getElementById('custom-reason-panel').style.display = 'none';
  _customReasonCallback = null;
}}
document.getElementById('custom-reason-input').addEventListener('keydown', function(e) {{
  if (e.key === 'Enter') confirmCustomReason();
  if (e.key === 'Escape') cancelCustomReason();
}});

window.doBatchUndo = function() {{
  if (window._undoTimer) {{ clearTimeout(window._undoTimer); window._undoTimer = null; }}
  var payload = Object.assign({{}}, window._pendingUndos);
  window._pendingUndos = {{}};
  var count = 0;
  for (var s in selected) {{
    if (s in COMMITTED) {{ payload[s] = true; count++; }}
  }}
  if (Object.keys(payload).length === 0) {{
    for (var s in selected) delete selected[s];
    updateSelCount();
    return;
  }}
  for (var s in payload) {{ delete COMMITTED[s]; }}
  for (var s in selected) {{ delete selected[s]; }}

  renderAll();
  updateSelCount();
  sendMsg('undo', payload);
}};

window.doDeselAll = function() {{ for (var k in selected) delete selected[k]; for (var k in staged) delete staged[k]; renderAll(); updateSelCount(); }};

(function() {{
  if (!PREFETCH_URLS || !PREFETCH_URLS.length) return;
  var statusEl = document.getElementById('prefetch-status');
  var i = 0, done = 0, total = PREFETCH_URLS.length;
  function loadNext() {{
    if (i >= total) return;
    var url = PREFETCH_URLS[i++];
    var img = new Image();
    img.referrerPolicy = "no-referrer";
    img.onload = img.onerror = function() {{
      done++;
      if (statusEl) statusEl.textContent = 'Prefetched ' + done + '/' + total;
      loadNext();
    }};
    img.src = url;
  }}
  for (var j = 0; j < 8; j++) {{
    loadNext();
  }}
}})();

// Listens on the grid container, not the window: the document is clipped to
// the iframe box now, so window never fires a scroll event and the restore
// on the next rerun would always come back 0.
(function() {{
  function _bindScrollMemory() {{
    var g = document.getElementById('card-grid');
    if (!g || g.dataset.scrollMemory) return;
    g.dataset.scrollMemory = '1';
    g.addEventListener('scroll', function() {{
      sessionStorage.setItem("__inner_iframe_scroll__", g.scrollTop);
    }}, {{passive: true}});
  }}
  _bindScrollMemory();
  window.addEventListener('DOMContentLoaded', _bindScrollMemory);
}})();

{scroll_js}

var _focusedSid = null;
var _lastReason = 'REJECT_POOR_IMAGE';

function _getCardSids() {{
  return getSortedCards().map(function(c) {{ return c.sid; }});
}}

function _moveFocus(dir) {{
  var sids = _getCardSids();
  if (!sids.length) return;
  var idx = _focusedSid ? sids.indexOf(_focusedSid) : -1;
  idx = Math.max(0, Math.min(sids.length - 1, idx + dir));
  _focusedSid = sids[idx];
  document.querySelectorAll('.card').forEach(function(c) {{ c.style.borderLeft = ''; }});
  var el = document.getElementById('card-' + escapeHtml(_focusedSid));
  if (el) {{
    el.style.borderLeft = '4px solid #2196F3';
    el.scrollIntoView({{ block: 'nearest', inline: 'nearest' }});
  }}
}}

document.addEventListener('keydown', function(e) {{
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
  if (document.getElementById('custom-reason-panel').style.display === 'block') return;
  if (e.key === 'ArrowRight') {{ e.preventDefault(); _moveFocus(1); }}
  else if (e.key === 'ArrowLeft') {{ e.preventDefault(); _moveFocus(-1); }}
  else if ((e.key === 'a' || e.key === 'A') && _focusedSid) {{
    delete selected[_focusedSid]; delete staged[_focusedSid];
    replaceCard(_focusedSid); updateSelCount();
  }}
  else if ((e.key === 'r' || e.key === 'R') && _focusedSid) {{
    var sid = _focusedSid;
    if (_lastReason === 'OTHER_CUSTOM') {{
      showCustomReasonPanel(function(cmt) {{
        if (!cmt) return;
        staged[sid] = 'Other Reason (Custom): ' + cmt;
        replaceCard(sid); updateSelCount();
      }});
    }} else {{
      if (sid in selected) delete selected[sid];
      staged[sid] = _lastReason;
      replaceCard(sid); updateSelCount();
    }}
  }}
  else if (e.key === ' ' && _focusedSid) {{
    e.preventDefault();
    window.toggleSelect(_focusedSid, e);
  }}
}});

(function() {{
  var el = document.getElementById('batch-reason-bottom');
  if (el) el.addEventListener('change', function() {{ _lastReason = this.value; }});
}})();

// Searchable reject-reason dropdown. The dropdown grew past 30 options once
// every check got a manual reason — scrolling a native <select> that long is
// real friction for a reviewer doing hundreds of rejects a day. This layers a
// search box + "Recently used" section on top of the existing <select>
// without touching how selections are read: the underlying <select>'s value
// is still what doBatchReject() reads, so nothing downstream had to change.
var RECENT_REASONS_KEY = 'pimqc_recent_reasons';
function _getRecentReasons() {{
  try {{ return JSON.parse(sessionStorage.getItem(RECENT_REASONS_KEY) || '[]'); }}
  catch(e) {{ return []; }}
}}
function _pushRecentReason(value, label) {{
  var recent = _getRecentReasons().filter(function(r) {{ return r.value !== value; }});
  recent.unshift({{value: value, label: label}});
  recent = recent.slice(0, 5);
  try {{ sessionStorage.setItem(RECENT_REASONS_KEY, JSON.stringify(recent)); }} catch(e) {{}}
}}

function enhanceReasonSelect(selectId) {{
  var sel = document.getElementById(selectId);
  if (!sel || sel.dataset.rsearchEnhanced) return;
  sel.dataset.rsearchEnhanced = '1';

  // Flatten <option>/<optgroup> into {{value, label, group}} once, in DOM order.
  var items = [];
  Array.prototype.forEach.call(sel.children, function(node) {{
    if (node.tagName === 'OPTGROUP') {{
      Array.prototype.forEach.call(node.children, function(opt) {{
        items.push({{value: opt.value, label: opt.textContent, group: node.label}});
      }});
    }} else if (node.tagName === 'OPTION') {{
      items.push({{value: node.value, label: node.textContent, group: null}});
    }}
  }});
  var byValue = {{}};
  items.forEach(function(it) {{ byValue[it.value] = it; }});

  sel.style.display = 'none';
  var wrap = document.createElement('div');
  wrap.className = 'rsearch-wrap';
  var input = document.createElement('input');
  input.className = 'rsearch-input';
  input.type = 'text';
  input.placeholder = 'Search reasons…';
  input.autocomplete = 'off';
  var panel = document.createElement('div');
  panel.className = 'rsearch-panel';
  panel.style.display = 'none';
  wrap.appendChild(input);
  // Panel is appended to <body>, not to wrap — see positionPanel() below for
  // why it cannot live inside the scrolling control bar.
  document.body.appendChild(panel);
  sel.parentNode.insertBefore(wrap, sel.nextSibling);

  var initial = byValue[sel.value];
  if (initial) input.value = initial.label;

  var hlIndex = -1;
  var visibleItems = [];
  var pristine = true;

  function renderPanel() {{
    // `pristine` is true from the moment the list opens until the reviewer
    // actually types. Without it the box's own text — which is pre-filled
    // with the current selection, e.g. "Poor Image" — was being used as the
    // search query, so opening the list filtered it down to the single
    // option that was already chosen. Opening shows everything; typing
    // filters. That is what a combobox is expected to do.
    var q = pristine ? '' : input.value.trim().toLowerCase();
    var recent = q ? [] : _getRecentReasons().filter(function(r) {{ return byValue[r.value]; }});
    var filtered = items.filter(function(it) {{
      return !q || it.label.toLowerCase().indexOf(q) !== -1;
    }});
    visibleItems = [];
    var html = '';
    if (recent.length) {{
      html += '<div class="rsearch-group-label">Recently used</div>';
      recent.forEach(function(it) {{ visibleItems.push(it); }});
    }}
    var lastGroup = recent.length ? '\\u0000' : null;
    filtered.forEach(function(it) {{
      if (it.group !== lastGroup) {{ lastGroup = it.group; }}
      visibleItems.push(it);
    }});
    if (!visibleItems.length) {{
      panel.innerHTML = '<div class="rsearch-empty">No matching reasons</div>';
      return;
    }}
    var seenGroup = null;
    var idx = 0;
    visibleItems.forEach(function(it, i) {{
      var isRecent = recent.indexOf(it) !== -1;
      var group = isRecent ? 'Recently used' : it.group;
      if (group !== seenGroup && !isRecent) {{
        html += '<div class="rsearch-group-label">' + escapeHtml(group || 'Other') + '</div>';
      }}
      seenGroup = isRecent ? seenGroup : group;
      html += '<div class="rsearch-item" data-idx="' + i + '">' + escapeHtml(it.label) + '</div>';
    }});
    panel.innerHTML = html;
    hlIndex = -1;
  }}

  // The panel lives on <body>, not next to the input.
  //
  // It used to be position:absolute inside .ctrl-bar.top-bar, which sets
  // overflow-x:auto / overflow-y:hidden to let the toolbar scroll sideways.
  // That makes the bar a clipping context, and a clip cannot be escaped by
  // z-index — so the open list was sliced off at the toolbar's edge and the
  // reasons underneath were impossible to see or click. Portalling to <body>
  // with position:fixed puts it outside every ancestor's overflow.
  function positionPanel() {{
    var r = input.getBoundingClientRect();
    panel.style.left = r.left + 'px';
    panel.style.width = Math.max(r.width, 260) + 'px';
    // Flip above the input when there is not room below it.
    var below = window.innerHeight - r.bottom;
    if (below < 220 && r.top > below) {{
      panel.style.top = 'auto';
      panel.style.bottom = (window.innerHeight - r.top + 4) + 'px';
      panel.style.maxHeight = Math.min(320, r.top - 12) + 'px';
    }} else {{
      panel.style.bottom = 'auto';
      panel.style.top = (r.bottom + 4) + 'px';
      panel.style.maxHeight = Math.min(320, below - 12) + 'px';
    }}
  }}
  function openPanel() {{
    renderPanel();
    panel.style.display = 'block';
    wrap.classList.add('open');
    // Whatever is in the box is the current selection, not a query — select
    // it so the first keystroke replaces it instead of appending to it.
    if (pristine) {{ try {{ input.select(); }} catch(e) {{}} }}
    positionPanel();
    window.addEventListener('scroll', positionPanel, true);
    window.addEventListener('resize', positionPanel);
  }}
  function closePanel() {{
    panel.style.display = 'none';
    wrap.classList.remove('open');
    pristine = true;
    window.removeEventListener('scroll', positionPanel, true);
    window.removeEventListener('resize', positionPanel);
    var curr = byValue[sel.value];
    input.value = curr ? curr.label : '';
  }}
  function selectItem(it) {{
    sel.value = it.value;
    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
    input.value = it.label;
    _pushRecentReason(it.value, it.label);
    panel.style.display = 'none';
    wrap.classList.remove('open');
    pristine = true;
  }}
  function setHighlight(i) {{
    var nodes = panel.querySelectorAll('.rsearch-item');
    nodes.forEach(function(n) {{ n.classList.remove('rsearch-hl'); }});
    if (i >= 0 && i < nodes.length) {{
      nodes[i].classList.add('rsearch-hl');
      nodes[i].scrollIntoView({{block: 'nearest'}});
    }}
    hlIndex = i;
  }}

  // Keep the visible search box in sync whenever the underlying select's
  // value changes from elsewhere (e.g. doBatchReject resetting it after the
  // custom-reason panel closes) — selectItem() also fires 'change', so this
  // is the single source of truth for the display text.
  sel.addEventListener('change', function() {{
    if (panel.style.display !== 'none') return; // avoid fighting live typing
    var curr = byValue[sel.value];
    input.value = curr ? curr.label : '';
  }});

  input.addEventListener('focus', openPanel);
  input.addEventListener('mousedown', function() {{
    // Clicking the closed box should open the full list, not re-filter.
    if (panel.style.display === 'none') pristine = true;
  }});
  input.addEventListener('input', function() {{ pristine = false; openPanel(); }});
  input.addEventListener('keydown', function(e) {{
    if (e.key === 'ArrowDown') {{ e.preventDefault(); if (panel.style.display === 'none') openPanel(); setHighlight(Math.min(hlIndex + 1, visibleItems.length - 1)); }}
    else if (e.key === 'ArrowUp') {{ e.preventDefault(); setHighlight(Math.max(hlIndex - 1, 0)); }}
    else if (e.key === 'Enter') {{ e.preventDefault(); if (hlIndex >= 0 && visibleItems[hlIndex]) selectItem(visibleItems[hlIndex]); else if (visibleItems.length === 1) selectItem(visibleItems[0]); }}
    else if (e.key === 'Escape') {{ closePanel(); input.blur(); }}
  }});
  panel.addEventListener('mousedown', function(e) {{
    var itemEl = e.target.closest('.rsearch-item');
    if (!itemEl) return;
    e.preventDefault();
    var it = visibleItems[parseInt(itemEl.dataset.idx, 10)];
    if (it) selectItem(it);
  }});
  document.addEventListener('click', function(e) {{
    // The panel is no longer a descendant of wrap, so it has to be tested
    // separately or clicking an option would immediately close the list.
    if (!wrap.contains(e.target) && !panel.contains(e.target)) closePanel();
  }});
}}
// batch-reason-top went with the top bar; only the bottom select remains.
enhanceReasonSelect('batch-reason-bottom');

// #card-grid is the scroll container, so its height has to be the iframe
// height minus whatever the toolbar currently occupies — and the toolbar
// wraps to a second row under 1100px, so that is measured rather than
// assumed. Re-measured on resize and after the grid re-renders.
window.sizeGridScroll = function() {{
  // --grid-top-h is 0 now: the top bar is gone and nothing sits above the
  // cards inside the iframe. The variable stays because #card-grid's
  // max-height still subtracts it, and a future header would only have to
  // set it here.
  document.documentElement.style.setProperty('--grid-top-h', '0px');
  var bot = document.querySelector('.ctrl-bar.bottom-bar');
  var bh = bot ? Math.ceil(bot.getBoundingClientRect().height) : 84;
  /* Floating means the grid stops reserving the bar's height and the cards
     scroll underneath it — that reclaim is the entire point of the mode. A
     few pixels are still held back so the last row clears the bar's shadow
     rather than touching it. */
  if (bot && bot.classList.contains('floating')) bh = 10;
  document.documentElement.style.setProperty('--grid-bot-h', bh + 'px');
}};

/* ── Dock / float / collapse ──────────────────────────────────────────────
   Kept in localStorage rather than session state: it changes nothing outside
   this iframe, so a round trip to Python would cost a rerun for a purely
   visual preference. That is the opposite of the old in-grid dark mode toggle,
   which was removed precisely because it themed only the iframe and left the
   grid dark inside a light app — this has no counterpart outside to fall out
   of step with. */
window.applyBarMode = function() {{
  var bot = document.querySelector('.ctrl-bar.bottom-bar');
  if (!bot) return;
  var floating = false, collapsed = false;
  try {{
    floating = localStorage.getItem('gridBarFloat') === '1';
    collapsed = localStorage.getItem('gridBarCollapsed') === '1';
  }} catch (e) {{}}
  collapsed = collapsed && floating;   // docked and collapsed would just be a gap
  bot.classList.toggle('floating', floating);
  bot.classList.toggle('collapsed', collapsed);
  var fb = document.getElementById('bar-float-btn');
  if (fb) {{
    fb.setAttribute('aria-pressed', floating ? 'true' : 'false');
    fb.title = floating ? 'Dock the bar below the cards' : 'Float the bar over the cards';
    fb.setAttribute('aria-label', fb.title);
  }}
  var cb = document.getElementById('bar-collapse-btn');
  if (cb) {{
    cb.style.display = floating ? '' : 'none';   // nothing to collapse when docked
    cb.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
    cb.innerHTML = collapsed ? '&#9650;' : '&#9660;';
    cb.title = collapsed ? 'Expand the bar' : 'Collapse the bar';
    cb.setAttribute('aria-label', cb.title);
  }}
  window.sizeGridScroll();
}};
window.toggleBarFloat = function() {{
  try {{
    var on = localStorage.getItem('gridBarFloat') === '1';
    localStorage.setItem('gridBarFloat', on ? '0' : '1');
    if (on) localStorage.setItem('gridBarCollapsed', '0');  // don't return collapsed
  }} catch (e) {{}}
  window.applyBarMode();
}};
window.toggleBarCollapse = function() {{
  try {{
    var on = localStorage.getItem('gridBarCollapsed') === '1';
    localStorage.setItem('gridBarCollapsed', on ? '0' : '1');
  }} catch (e) {{}}
  window.applyBarMode();
}};
window.addEventListener('resize', window.sizeGridScroll);
window.addEventListener('load', window.sizeGridScroll);
sizeGridScroll();
// After sizeGridScroll exists, so the reservation is right on first paint.
window.applyBarMode();
try {{
  // Same-document observer, so this one does fire — unlike the cross-frame
  // attempt further down. It catches the bar wrapping to a second row.
  var _bot = document.querySelector('.ctrl-bar.bottom-bar');
  if (_bot && window.ResizeObserver) new ResizeObserver(window.sizeGridScroll).observe(_bot);
}} catch(e) {{}}

/* ── Size the iframe to the space the dialog actually leaves ───────────────
   Python cannot read the browser height, so st.iframe(height=...) is a guess
   sized for the smallest supported screen. The dialog backdrop
   (div[data-testid="stDialog"]) is overflow-y:auto and the panel inside it is
   overflow:visible with no max-height, so the panel simply grows to fit its
   content and the backdrop scrolls it. Measured at 1366x768: panel 968px in a
   768px viewport, putting the iframe's bottom edge 168px below the fold. The
   batch bar is position:fixed against the *iframe* viewport, so it was pinned
   correctly the whole time — it just went off screen with the iframe.

   CSS alone cannot fix this. Streamlit sizes the stElementContainer wrapper
   from the Python height argument, so setting only the iframe's height leaves
   a 468px iframe inside a 620px wrapper and an empty gap below the cards.
   Both boxes have to move together.

   st.iframe renders a srcdoc iframe, which inherits the parent's origin, so
   window.frameElement and the parent document are both reachable — verified
   against the live DOM. That makes the grid the only place with all three
   facts: the real viewport height, its own offset within the dialog, and how
   much dialog chrome sits below it. So it sizes itself.

   Everything is measured, not assumed. Nothing here depends on a Streamlit
   emotion class or on the number of control rows above the grid. */
var MIN_H = 320, MAX_H = 1600, GAP = 16;
var _lastFitW = -1, _lastFitH = -1;
window.fitToViewport = function(force) {{
  var fe;
  try {{ fe = window.frameElement; }} catch (e) {{ return; }}   // cross-origin
  if (!fe) return;                                            // not embedded
  var pw = window.parent, wrap = fe.parentElement;

  /* Bail out before touching layout unless the viewport actually changed.
     innerWidth/innerHeight are plain property reads; getBoundingClientRect
     below forces a synchronous layout, which on a 500-card grid measures
     3ms — 6ms/s at two ticks a second, sustained for as long as the modal is
     open, and 20ms/s while the DOM is being mutated. Gating on the cheap
     reads takes the idle cost of the timer to roughly nothing while still
     catching every resize. Callers that know the geometry moved for another
     reason — the open animation, a re-render clobbering our styles — pass
     force and skip the gate. */
  if (!force && pw.innerWidth === _lastFitW && pw.innerHeight === _lastFitH) return;
  _lastFitW = pw.innerWidth; _lastFitH = pw.innerHeight;

  var top = fe.getBoundingClientRect().top;

  /* Whatever the dialog puts below the grid — the close row, its padding —
     measured as the gap between the wrapper's bottom and the panel's. Taken
     from current geometry so it stays correct when those rows change height,
     and it is a distance between two elements that both move together, so it
     does not drift as this function resizes things. */
  var below = 0;
  try {{
    var panel = pw.document.querySelector('[data-testid="stDialog"] section[role="dialog"]');
    if (panel) below = Math.max(0, panel.getBoundingClientRect().bottom
                                   - (wrap || fe).getBoundingClientRect().bottom);
  }} catch (e) {{}}

  var h = Math.round(pw.innerHeight - top - below - GAP);
  h = Math.max(MIN_H, Math.min(MAX_H, h));
  var px = h + 'px';
  if (fe.style.height !== px) {{
    fe.style.setProperty('height', px, 'important');
    fe.style.setProperty('max-height', px, 'important');
  }}
  /* The wrapper needs flex-basis, not just height. Streamlit gives it
     flex: 0 0 <python height>px inside a column flex container, where the
     basis is the main size and wins over the height property outright —
     setting height alone left a 356px iframe in a 620px wrapper with the
     inline rule marked !important and losing anyway. Setting both collapses
     the wrapper with the iframe, which is what lets the dialog shrink:
     measured overflow 296px -> 32px, and the 32px that remains is the
     backdrop's own padding, not hidden content. */
  if (wrap && wrap.style.height !== px) {{
    wrap.style.setProperty('flex', '0 0 ' + px, 'important');
    wrap.style.setProperty('height', px, 'important');
  }}
  return h;
}};

/* React re-renders overwrite inline styles it set itself — observed once in
   testing, the wrapper snapping back to its Python height. Watch the style
   attribute and re-apply. The height check inside fitToViewport() makes this
   idempotent, so re-entry from our own write cannot loop. */
(function() {{
  var poll  = function() {{ try {{ window.fitToViewport(false); }} catch (e) {{}} }};
  var force = function() {{ try {{ window.fitToViewport(true);  }} catch (e) {{}} }};
  force();
  window.addEventListener('load', force);
  window.addEventListener('resize', force);
  try {{ window.parent.addEventListener('resize', force); }} catch (e) {{}}
  /* A resize event alone is not enough: it fires while the dialog is still
     relaying out, so the measurement reads stale geometry, computes the height
     it already has, and the guard in fitToViewport turns it into a no-op — the
     grid then sits at its old size until something else nudges it. Measured:
     resizing 768 -> 1080 left the grid at 356px.

     A ResizeObserver was the obvious answer and does not work here. Observing a
     parent-document element from inside the iframe never fired, and neither did
     one constructed in the parent — 0 callbacks either way, not even the initial
     one that observe() is supposed to deliver. So it re-checks on a timer.

     The timer is the cheap variant: fitToViewport() returns immediately unless
     window.innerWidth/innerHeight changed, which are property reads that do not
     touch layout. Only the events and observers below, which know the geometry
     moved for a reason the viewport size cannot show, force a real measurement.
     Without that gate this cost 3ms of forced layout per tick on a 500-card
     grid — 6ms/s idle and 20ms/s while the DOM was being mutated, for as long
     as the modal stayed open. */
  setInterval(poll, 500);
  try {{
    var fe = window.frameElement;
    if (fe && window.MutationObserver) {{
      // React clobbering our inline styles leaves the viewport unchanged, so
      // this has to force — the gate would otherwise swallow the recovery.
      var mo = new MutationObserver(force);
      mo.observe(fe, {{attributes: true, attributeFilter: ['style']}});
      if (fe.parentElement) mo.observe(fe.parentElement, {{attributes: true, attributeFilter: ['style']}});
    }}
  }} catch (e) {{}}
  /* The dialog animates open, so the first measurement can land before the
     panel has settled. These force for the same reason: the viewport is not
     changing, the panel underneath it is. Four reads over the first second. */
  [60, 180, 400, 800].forEach(function(t) {{ setTimeout(force, t); }});
}})();

// The page no longer scrolls, the grid does — so the nav buttons have to
// move the container, not the window.
window.gridScrollTo = function(where) {{
  var g = document.getElementById('card-grid');
  if (!g) return;
  g.scrollTo({{top: where === 'bottom' ? g.scrollHeight : 0, behavior: 'smooth'}});
}};

(function() {{
  var _gs = document.getElementById('grid-search');
  if (_gs) _gs.addEventListener('input', function() {{
    var q = this.value.toLowerCase().trim();
    document.querySelectorAll('.card').forEach(function(card) {{
      var text = (card.dataset.name + ' ' + card.dataset.brand + ' ' +
                  card.dataset.sid + ' ' + card.dataset.cat).toLowerCase();
      card.style.display = (!q || text.includes(q)) ? '' : 'none';
    }});
    var vis = document.querySelectorAll('.card:not([style*="none"])').length;
    var el = document.getElementById('grid-count');
    if (el) el.textContent = q ? vis + ' matching' : vis + ' products';
  }});
}})();

// The grid used to carry its own dark-mode toggle, persisted in
// localStorage. It themed only this iframe, so a reviewer who clicked it
// once got a permanently dark grid inside a permanently light app —
// .streamlit/config.toml pins base = "light".
//
// It also fought the design tokens: applyDark(false) ran on every load and
// wrote the old hardcoded greys (#f9fafb, #ffffff, #111827, #e5e7eb) as
// inline styles on documentElement, which beat the :root values by
// specificity. A real dark theme needs the whole app, not one iframe.

window.batchApproveSingle = function(sid) {{
  window.parent.postMessage({{type:'staged_reject', sid:sid, reason:'Approved'}}, '*');
}}

window.batchApprove = function() {{
  var sids = Object.keys(selected).filter(s => !(s in COMMITTED));
  if (sids.length === 0) return;
  if (confirm('Approve ' + sids.length + ' selected items?')) {{
    sids.forEach(sid => window.batchApproveSingle(sid));
    window.doDeselAll();
  }}
}}

try {{
  renderAll();
}} catch(e) {{
  document.getElementById('card-grid').innerHTML = '<div style="color:red;padding:20px;font-size:14px;font-family:monospace;white-space:pre-wrap;background:#fff3f3;border:2px solid red;border-radius:8px;margin:20px;">&#x26A0; JS ERROR in renderAll():<br>' + String(e) + '<br><br>Stack:<br>' + (e.stack||'') + '</div>';
}}

</script>

<!-- #cols-strip lived here: a second fixed bar, 26px tall, holding only the
     3/4/5/6/7 buttons and their label. Five small buttons do not need a band
     of their own, and it forced the batch bar to sit at bottom:26px to clear
     it. The buttons are in .cols-group inside the batch bar now. -->

</body>
</html>"""
    return _html_str, *_grid_sync_data


@st.dialog(
    "Visual Review Mode", width="large", icon=":material/pageview:", dismissible=False
)
def visual_review_modal(support_files):
    scroll_top_flag = st.session_state.get("do_scroll_top", False)
    st.session_state.do_scroll_top = False
    # st.dialog(width="large") is documented as a hard 1280px maximum, so at 6
    # or 7 columns the grid was squeezed into 1280px no matter how big the
    # screen was. This lifts that cap.
    #
    # The previous selector — `[data-testid="stDialog"] > div > div[role=...]`
    # — matched nothing on Streamlit 1.60: the panel is a direct child div of
    # the backdrop, and role="dialog" sits on a <section> one level further in.
    # Measured against the live DOM at 1920px: panel stayed 1280px with the old
    # rule, goes to 1843px (96vw) with this one.
    #
    # Anchored to data-testid and element structure only. The emotion class on
    # the panel (st-emotion-cache-11wjdxt) is a build hash — it differs between
    # Streamlit versions, so targeting it would work locally and silently fail
    # on Cloud whenever the two drift apart.
    _wide_cols = st.session_state.get("grid_cols_per_row", 4) >= 6
    if _wide_cols:
        st.markdown("""
        <style>
        [data-testid="stDialog"] > div {
            width: 96vw !important;
            max-width: 96vw !important;
        }
        [data-testid="stDialog"] section[role="dialog"] {
            width: 100% !important;
            max-width: none !important;
        }
        </style>""", unsafe_allow_html=True)

    # ── ZIP images lost with the session ──────────────────────────────────
    # The uploaded bytes live only in session state; the report is checkpointed
    # to disk. So a dropped websocket or a container restart on Cloud leaves
    # the report and the grid perfectly intact and every ZIP image gone —
    # _prepare_lazy_zip_images() rebuilds its index from cached_uploaded_files,
    # which is empty by then, and silently produces an empty one.
    #
    # The cards then render blank with no explanation, which reads as the app
    # losing the images rather than the session. Say so instead, and say what
    # fixes it: re-attaching the same ZIP restores them without redoing the
    # review, because the decisions are journalled separately.
    # Keyed on the ZIP being absent from the upload, NOT on the image index
    # being empty. An empty index does not mean the images were lost:
    # _index_zip_images only counts entries under a top-level "images/" folder,
    # so a ZIP that lays its pictures out differently — or one that carries only
    # QC results and no images at all, which is a normal way to use this —
    # indexes to nothing while being perfectly present. Warning on that told
    # people their images had been lost when both files were sitting right
    # there in the uploader.
    #
    # What the message is actually about is the session dropping the uploaded
    # bytes, and the honest signal for that is that no ZIP is uploaded any more
    # while the report still says rows came from one.
    # Third condition, and the one that matters most: the cards must actually
    # need the archive. A card only falls back to the ZIP when its MAIN_IMAGE
    # is not an http URL — see the img_url branch in build_fast_grid_html. A
    # batch whose images are all URLs renders identically with or without the
    # ZIP, so telling that reviewer their images are unavailable is nonsense
    # while the pictures are on screen in front of them. Is_Zip only means the
    # row was touched by the ZIP's QC results, which says nothing about images.
    _zip_uploaded = any(
        str(f.get("name", "")).lower().endswith(".zip")
        for f in (st.session_state.get("cached_uploaded_files") or [])
    )
    _needs_zip_images = False
    if not _zip_uploaded:
        _dm = st.session_state.get("all_data_map")
        if isinstance(_dm, pd.DataFrame) and "MAIN_IMAGE" in _dm.columns:
            _mi = _dm["MAIN_IMAGE"].fillna("").astype(str).str.strip()
            _needs_zip_images = bool((_mi.ne("") & ~_mi.str.startswith("http")).any())
    if _needs_zip_images:
        st.warning(
            "**Product images are unavailable.** This batch came from a ZIP, and its "
            "images are held in the session rather than on disk — a reconnect or a "
            "restart clears them. Re-attach the same ZIP in the uploader to bring them "
            "back; your approvals and rejections are stored separately and are not affected.",
            icon=":material/image_not_supported:",
        )

    def _clear_grid_search_n():
        st.session_state["grid_search_n"] = ""
    def _clear_grid_filter_sellers():
        st.session_state["grid_filter_sellers"] = []
    def _clear_grid_filter_categories():
        st.session_state["grid_filter_categories"] = []
    def _clear_grid_filter_flag():
        st.session_state["grid_filter_flag"] = ""
    def _clear_grid_sort_issue():
        st.session_state["grid_sort_issue"] = ""

    fr = st.session_state.final_report
    data = st.session_state.all_data_map
    all_rows = st.session_state.get("all_data_rows", data)
    committed_rej_sids = set(st.session_state.get("quick_rejects", {}).keys())

    poor_img_rej_sids = set(
        fr[
            (fr["Status"] == "Rejected")
            & (
                fr["FLAG"].isin(["Poor images", "Image Stretched", "Image Blurry"])
                | fr["FLAG"].str.contains("Brand Image Check", na=False, case=False)
                | fr["Comment"].str.contains("Brand Image Check", na=False, case=False)
            )
        ]["ProductSetSid"]
        .astype(str)
        .str.strip()
        .unique()
    )
    valid_grid_df = fr[
        (fr["Status"] == "Approved")
        | (fr["ProductSetSid"].isin(committed_rej_sids))
        | (fr["ProductSetSid"].isin(poor_img_rej_sids))
    ]

    if "MAIN_IMAGE" not in data.columns:
        data["MAIN_IMAGE"] = ""

    _cached_review = st.session_state.get("_grid_review_data_cache")
    _cache_valid = (
        _cached_review is not None
        and not committed_rej_sids
        and not poor_img_rej_sids
        and len(_cached_review) > 0
    )
    if _cache_valid:
        review_data = _cached_review.copy()
    else:
        available_cols = [c for c in GRID_COLS if c in data.columns]
        if "CATEGORY_CODE" in data.columns and "CATEGORY_CODE" not in available_cols:
            available_cols.append("CATEGORY_CODE")
        if "IMAGE1_ZIP" in data.columns:
            available_cols.append("IMAGE1_ZIP")
        if "Brand_Detected_On_Product" in data.columns:
            available_cols.append("Brand_Detected_On_Product")
        review_data = pd.merge(
            valid_grid_df[["ProductSetSid"]],
            data[available_cols],
            left_on="ProductSetSid",
            right_on="PRODUCT_SET_SID",
            how="left",
        )
        _code_to_path = support_files.get("code_to_path", {})
        if _code_to_path and "CATEGORY_CODE" in review_data.columns:
            review_data = review_data.copy()
            review_data["CATEGORY"] = resolve_category_paths(review_data, _code_to_path)

    curr_search_n = st.session_state.get("grid_search_n", "")
    curr_sellers = st.session_state.get("grid_filter_sellers", [])
    curr_categories = st.session_state.get("grid_filter_categories", [])

    seller_base_df = review_data
    if curr_categories and "CATEGORY" in seller_base_df.columns:
        seller_base_df = seller_base_df[seller_base_df["CATEGORY"].astype(str).isin(curr_categories)]
    
    seller_opts = sorted(
        {
            str(v).strip()
            for v in seller_base_df.get("SELLER_NAME", pd.Series(dtype=str)).dropna().astype(str)
            if str(v).strip() and str(v).strip().lower() != "nan"
        }
    )

    category_base_df = review_data
    if curr_sellers and "SELLER_NAME" in category_base_df.columns:
        category_base_df = category_base_df[category_base_df["SELLER_NAME"].astype(str).isin(curr_sellers)]
    
    category_opts = sorted(
        {
            str(v).strip()
            for v in category_base_df.get("CATEGORY", pd.Series(dtype=str)).dropna().astype(str)
            if str(v).strip() and str(v).strip().lower() != "nan"
        }
    )

    for s in curr_sellers:
        if s not in seller_opts: seller_opts.append(s)
    for c in curr_categories:
        if c not in category_opts: category_opts.append(c)
    seller_opts.sort()
    category_opts.sort()

    # Search stays inline — it is used constantly. Seller and category move
    # into a popover: they are set occasionally, and as a permanent row they
    # cost a full band of a modal that had ~370px left for cards. The button
    # carries the active count so a hidden filter can never be forgotten.
    _n_active = len(curr_sellers or []) + len(curr_categories or [])
    c1, c2, c4 = st.columns([2.2, 1.2, 0.9], gap="medium", vertical_alignment="bottom")
    with c1:
        c1a, c1b = st.columns([6, 1], vertical_alignment="bottom", gap="small")
        with c1a:
            search_n = st.text_input(
                "Search by Name, Brand, or SID", placeholder="Product name, brand, SID…", icon=":material/search:",
                label_visibility="collapsed",
                key="grid_search_n",
            )
        with c1b:
            st.button("✖", key="clr_n", help="Clear search", on_click=_clear_grid_search_n, disabled=not bool(curr_search_n))
    with c2:
        with st.popover(
            f"Filters ({_n_active})" if _n_active else "Filters",
            use_container_width=True,
            help="Filter the grid by seller or category",
        ):
            fa, fb = st.columns([6, 1], vertical_alignment="bottom", gap="small")
            with fa:
                search_sellers = st.multiselect(
                    "Seller", placeholder="All sellers",
                    options=seller_opts, default=curr_sellers,
                    key="grid_filter_sellers",
                )
            with fb:
                st.button("✖", key="clr_sellers", help="Clear seller filter",
                          on_click=_clear_grid_filter_sellers, disabled=not bool(curr_sellers))
            ga, gb = st.columns([6, 1], vertical_alignment="bottom", gap="small")
            with ga:
                search_categories = st.multiselect(
                    "Category", placeholder="All categories",
                    options=category_opts, default=curr_categories,
                    key="grid_filter_categories",
                )
            with gb:
                st.button("✖", key="clr_categories", help="Clear category filter",
                          on_click=_clear_grid_filter_categories, disabled=not bool(curr_categories))

    curr_flag = st.session_state.get("grid_filter_flag", "")
    curr_sort = st.session_state.get("grid_sort_issue", "")

    with c4:
        # The modal had two close buttons, one here and one under the footer.
        # This one also sat alone on its own row: c4 stacks its children, and
        # the search and filter columns are bottom-aligned to the slider below,
        # so the whole left of this band was empty. Dropping it takes the band
        # with it. Close Review at the bottom is the survivor.
        #
        # 500 per page is only offered in wide mode (6 or 7 columns). More
        # columns means smaller cards, so 500 of them stays a sensible page; at
        # 5 columns the same 500 cards make the grid iframe roughly 36,000px
        # tall, which is where the browser starts to struggle.
        _cols_now = st.session_state.get("grid_cols_per_row", 4)
        _allow_500 = _cols_now in (6, 7)
        _ipp_opts = [20, 50, 100, 200] + ([500] if _allow_500 else [])

        _slider_key = "grid_ipp_slider"

        # Anything already selected that this column count no longer allows has
        # to be clamped BEFORE the widget is created — select_slider raises if
        # its stored value is not one of the options.
        _current_ipp = st.session_state.get("grid_items_per_page", 50)
        if _current_ipp not in _ipp_opts:
            _current_ipp = _ipp_opts[-1]
            st.session_state.grid_items_per_page = _current_ipp
            st.session_state.grid_page = 0
        if st.session_state.get(_slider_key) not in _ipp_opts:
            st.session_state[_slider_key] = _current_ipp

        def _on_ipp_change():
            st.session_state.grid_items_per_page = st.session_state[_slider_key]

        # In a popover rather than inline. A select_slider carries its label
        # above the track, making it the tallest widget in this row and setting
        # the height of the whole band; every other control here is one input
        # high. Behind a button the band collapses to that single height, and
        # page size is a setting you change occasionally, not a control you
        # need under the cursor.
        with st.popover(
            f"View · {st.session_state.get('grid_items_per_page', 50)}",
            use_container_width=True,
            help="How many products to show per page",
        ):
            st.select_slider(
                "Items per page",
                options=_ipp_opts,
                key=_slider_key,
                on_change=_on_ipp_change,
                help=("500 per page is available in wide mode (6 or 7 columns)."
                      if _allow_500 else
                      "Switch to 6 or 7 columns to unlock 500 per page."),
            )
        st.session_state.grid_items_per_page = st.session_state[_slider_key]

    if "_grid_page_contexts" not in st.session_state:
        st.session_state._grid_page_contexts = {}
    _curr_ctx = (
        search_n or "",
        tuple(sorted(search_sellers)) if search_sellers else (),
        tuple(sorted(search_categories)) if search_categories else (),
        curr_flag,
        curr_sort,
    )
    _prev_ctx = st.session_state.get("_grid_last_ctx", ("", "", "", "", ""))
    if _curr_ctx != _prev_ctx:
        st.session_state._grid_page_contexts[_prev_ctx] = st.session_state.get("grid_page", 0)
        st.session_state.grid_page = st.session_state._grid_page_contexts.get(_curr_ctx, 0)
        st.session_state["_grid_last_ctx"] = _curr_ctx

    if search_n:
        n_mask = review_data["NAME"].astype(str).str.contains(search_n, case=False, na=False)
        b_mask = review_data.get("BRAND", pd.Series(dtype=str, index=review_data.index)).astype(str).str.contains(search_n, case=False, na=False)
        s_mask = review_data.get("ProductSetSid", pd.Series(dtype=str, index=review_data.index)).astype(str).str.contains(search_n, case=False, na=False)
        review_data = review_data[n_mask | b_mask | s_mask]
    if search_sellers:
        review_data = review_data[
            review_data["SELLER_NAME"].astype(str).isin(search_sellers)
        ]
    if search_categories and "CATEGORY" in review_data.columns:
        review_data = review_data[
            review_data["CATEGORY"].astype(str).isin(search_categories)
        ]

    # --- Shared warning computation -------------------------------------
    # Single source of truth for per-SID warnings, used both for the
    # flag/sort filter pass (over review_data, potentially many rows) and
    # for the per-page display pass (over just page_data, ~ipp rows).
    # Previously these were two separately-written implementations that
    # could silently drift out of sync, and the page-display version did
    # an O(n) DataFrame scan (`fr[fr["ProductSetSid"] == sid]`) per row
    # instead of a dict lookup.
    _fr_flag_map = dict(zip(fr["ProductSetSid"].astype(str), fr["FLAG"])) if "FLAG" in fr.columns else {}
    _fr_comment_map = dict(zip(fr["ProductSetSid"].astype(str), fr["Comment"])) if "Comment" in fr.columns else {}
    # Carry the flag's specific Comment (e.g. "Restricted brand 'Nike' detected
    # on product image..." from Brand Image Mismatch, or the matched phone/URL
    # from Off-Platform Contact) through to the card so reviewers see WHY a
    # product was flagged without leaving the grid — previously only the
    # Category Check reason had a dedicated slot on the card.
    if _fr_comment_map:
        review_data["Comment"] = review_data["ProductSetSid"].astype(str).map(_fr_comment_map)
    _zip_index = st.session_state.get("_zip_sid_index")
    _zip_status_cols = st.session_state.get("_zip_status_cols", [])
    _zip_prefetch_map = st.session_state.get("_zip_prefetch_map", {})
    _staged_sids = set(st.session_state.get("_stagedRejections", {}).keys())

    def _compute_warnings(sid):
        w = []
        comment = str(_fr_comment_map.get(sid, "")).lower()
        if "stretched" in comment or "tall" in comment: w.append("Tall (Screenshot?)")
        if "stretched" in comment or "wide" in comment: w.append("Wide Aspect")
        if "blurry" in comment or "low res" in comment or "resolution" in comment or "small" in comment: w.append("Low Resolution")

        flag = _fr_flag_map.get(sid)
        if pd.notna(flag) and flag not in ("Approved", "Manual review", "Approved by User"):
            w.append(flag)

        if _zip_index is not None and sid in _zip_index.index:
            zrow = _zip_index.loc[sid]
            if hasattr(zrow, "iloc") and hasattr(zrow, "shape") and len(zrow.shape) == 2:
                zrow = zrow.iloc[0]
            for zcol in _zip_status_cols:
                if str(zrow.get(zcol, "")).lower() == "rejected":
                    zflag = _zip_prefetch_map.get(zcol, zcol.replace("_Status", "").replace("_", " ").title())
                    if zflag not in w: w.append(zflag)
        
        # Robust deduplication (ignoring trailing spaces)
        unique_w = []
        seen = set()
        for flag in w:
            cleaned = str(flag).strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                unique_w.append(cleaned)
        return unique_w

    if curr_flag or curr_sort:
        review_data["_warnings"] = review_data["ProductSetSid"].apply(_compute_warnings)

    if curr_flag:
        if curr_flag == "committed":
            review_data = review_data[review_data["ProductSetSid"].isin(committed_rej_sids)]
        elif curr_flag == "brand_ocr":
            review_data = review_data[review_data["ProductSetSid"].isin(committed_rej_sids) & review_data["_warnings"].apply(lambda w: "Brand Image Check" in w)]
        elif curr_flag == "no_flags":
            review_data = review_data[(~review_data["ProductSetSid"].isin(committed_rej_sids)) & (~review_data["ProductSetSid"].isin(_staged_sids)) & review_data["_warnings"].apply(lambda w: len(w) == 0)]
        elif curr_flag == "duplicates":
            review_data = review_data[review_data.get("is_duplicate", pd.Series(False, index=review_data.index)).astype(bool)]
        elif curr_flag == "manual_review":
            review_data = review_data[review_data.get("is_manual_review", pd.Series(False, index=review_data.index)).astype(bool)]
        elif curr_flag == "color_mismatch":
            review_data = review_data[review_data.get("color_mismatch", pd.Series(False, index=review_data.index)).astype(bool)]
        else:
            def has_flag(sid, w):
                qrs = st.session_state.get("quick_rejects", {})
                return curr_flag in w or (sid in qrs and str(qrs.get(sid, "")).replace("_", " ").lower() == curr_flag.replace("_", " ").lower())
            review_data = review_data[review_data.apply(lambda r: has_flag(r["ProductSetSid"], r["_warnings"]), axis=1)]

    if curr_sort:
        if curr_sort == "most_flagged":
            review_data["_w_count"] = review_data["_warnings"].apply(len)
            review_data = review_data.sort_values(by=["_w_count"], ascending=False).drop(columns=["_w_count"])
        elif curr_sort == "no_issue":
            review_data["_has_issue"] = review_data["_warnings"].apply(lambda w: 1 if len(w) > 0 else 0)
            review_data = review_data.sort_values(by=["_has_issue"], ascending=True).drop(columns=["_has_issue"])
        else:
            issue_map = {'low_res':'Low Resolution','tall':'Tall (Screenshot?)','wide':'Wide Aspect','broken':'Broken Image'}
            target = issue_map.get(curr_sort, curr_sort)
            review_data["_has_target"] = review_data["_warnings"].apply(lambda w: 0 if target in w else 1)
            review_data = review_data.sort_values(by=["_has_target"], ascending=True).drop(columns=["_has_target"])
    else:
        review_data = review_data.sort_values(
            by=["SELLER_NAME", "NAME"], na_position="last"
        ).reset_index(drop=True)

    ipp = st.session_state.get("grid_items_per_page", 50)
    total_pages = max(1, (len(review_data) + ipp - 1) // ipp)
    if st.session_state.get("grid_page", 0) >= total_pages:
        # Clamp to the nearest valid page instead of bouncing all the way
        # back to page 0 — e.g. a user on page 9 who applies a filter that
        # shrinks the result set to 7 pages should land on page 7, not 1.
        st.session_state.grid_page = total_pages - 1

    # 'Total items' used to sit on its own line above a separate 'Page (of N)'
    # label — two rows saying one thing. The count now rides on the page
    # control at the bottom.

    # ------------------------------------------------------------------
    # Pagination controls
    #
    # IMPORTANT: `jump_top` and `jump_bot` are widget-backed session_state
    # keys (they belong to the two st.number_input widgets below). Once a
    # widget with a given key has been instantiated in a run, Streamlit
    # forbids writing to that key again in the *same* run (it raises
    # StreamlitAPIException). The old code tried to do exactly that from
    # the "Prev"/"Next" button branches (`if st.button(...): st.session_state.jump_top = ...`)
    # which broke as soon as both widgets existed on the page.
    #
    # The fix: drive all page changes through on_click / on_change
    # callbacks. Callbacks run *before* the script body (and therefore
    # before any widget is instantiated for that run), so it's always
    # safe to set jump_top / jump_bot / grid_page there. Streamlit
    # automatically reruns the script after a callback fires, so no
    # manual st.rerun() is needed either.
    # ------------------------------------------------------------------

    def _goto_page(new_page_0idx):
        new_page_0idx = max(0, min(total_pages - 1, new_page_0idx))
        st.session_state.grid_page = new_page_0idx
        st.session_state.jump_top = new_page_0idx + 1
        st.session_state.jump_bot = new_page_0idx + 1
        st.session_state.do_scroll_top = True

    def _prev_page():
        _goto_page(st.session_state.get("grid_page", 0) - 1)

    def _next_page():
        _goto_page(st.session_state.get("grid_page", 0) + 1)

    def _jump_from_widget(source_key):
        # Called on_change of whichever number_input the user actually typed into.
        new_page_0idx = st.session_state[source_key] - 1
        _goto_page(new_page_0idx)

    # Keep jump_top/jump_bot initialized (only before their widgets exist,
    # i.e. only if this is the very first time we see these keys).
    if "jump_top" not in st.session_state:
        st.session_state.jump_top = st.session_state.get("grid_page", 0) + 1
    if "jump_bot" not in st.session_state:
        st.session_state.jump_bot = st.session_state.get("grid_page", 0) + 1

    # Top pagination removed. Prev/Next, the page number and the progress bar
    # were rendered twice — once here and again below the cards — costing
    # ~105px of a modal that only had ~370px left for cards. The bottom pair
    # is kept because paging is something you do *after* reviewing a screen.

    with st.spinner("Loading new page..."):
        page_start = st.session_state.grid_page * ipp
        page_data = review_data.iloc[page_start : page_start + ipp]

        # Reuse the same _compute_warnings() used for the flag/sort filter
        # pass above, instead of a second, slightly-different
        # implementation. This also replaces an O(n) DataFrame scan per
        # row (`fr[fr["ProductSetSid"] == sid]`) with the dict lookups
        # already built in _compute_warnings/_fr_flag_map.
        page_warnings = {}
        for _sid in page_data["PRODUCT_SET_SID"].astype(str):
            _warns = _compute_warnings(_sid)
            if _warns:
                page_warnings[_sid] = _warns

        # Bounded prefetch cache: previously each distinct
        # (page, len(review_data), ipp) combo added a brand-new permanent
        # top-level session_state key that was never evicted, so a long
        # session paging/filtering around would leak session_state
        # entries indefinitely. Store all entries under one dict with a
        # simple FIFO cap instead.
        _PREFETCH_CACHE_MAX_ENTRIES = 30
        if "_prefetch_cache" not in st.session_state:
            st.session_state._prefetch_cache = OrderedDict()
        _prefetch_cache = st.session_state._prefetch_cache

        _prefetch_cache_key = f"{st.session_state.grid_page}_{len(review_data)}_{ipp}"
        if _prefetch_cache_key not in _prefetch_cache:
            prefetch_urls = []
            _already_warm = set(st.session_state.get("_grid_warm_urls", []))
            seen_urls = set(_already_warm)
            for prefetch_page in [
                st.session_state.grid_page + 1,
                st.session_state.grid_page + 2,
                st.session_state.grid_page + 3,
            ]:
                if prefetch_page >= total_pages:
                    break
                p_start = prefetch_page * ipp
                for url in review_data.iloc[p_start : p_start + ipp]["MAIN_IMAGE"].astype(
                    str
                ):
                    url = url.strip().replace("http://", "https://", 1)
                    if url.startswith("https") and url not in seen_urls:
                        seen_urls.add(url)
                        prefetch_urls.append(url)
            _prefetch_cache[_prefetch_cache_key] = prefetch_urls
            _prefetch_cache.move_to_end(_prefetch_cache_key)
            while len(_prefetch_cache) > _PREFETCH_CACHE_MAX_ENTRIES:
                _prefetch_cache.popitem(last=False)
        else:
            _prefetch_cache.move_to_end(_prefetch_cache_key)
            prefetch_urls = _prefetch_cache[_prefetch_cache_key]

        qrs = st.session_state.get("quick_rejects", {})
        rejected_state = {
            sid.strip(): qrs[sid.strip()]
            for sid in page_data["PRODUCT_SET_SID"].astype(str)
            if sid.strip() in qrs
        }

        _page_sids = page_data.get(
            "PRODUCT_SET_SID", page_data.get("ProductSetSid", pd.Series())
        ).astype(str)
        _needs_poor_img_lookup = any(
            s.strip() in poor_img_rej_sids and s.strip() not in rejected_state for s in _page_sids
        )
        if _needs_poor_img_lookup:
            # Single stripped-key dict built once for this page instead of a full
            # fr[...] scan repeated per row (was O(page_size * len(fr))).
            _fr_flag_map_stripped = dict(zip(fr["ProductSetSid"].astype(str).str.strip(), fr["FLAG"])) if "FLAG" in fr.columns else {}
            _fr_comment_map_stripped = dict(zip(fr["ProductSetSid"].astype(str).str.strip(), fr["Comment"])) if "Comment" in fr.columns else {}
        else:
            _fr_flag_map_stripped = {}
            _fr_comment_map_stripped = {}

        for _sid_raw in _page_sids:
            _sid = _sid_raw.strip()
            if _sid in poor_img_rej_sids and _sid not in rejected_state:
                _flag = str(_fr_flag_map_stripped.get(_sid, ""))
                _comment = str(_fr_comment_map_stripped.get(_sid, ""))
                if "Brand Image Check" in _flag or "Brand Image Check" in _comment:
                    rejected_state[_sid] = "Brand Image Check"
                else:
                    rejected_state[_sid] = "Poor images"

        cols_per_row = st.session_state.get("grid_cols_per_row", 4)

        grid_html = build_fast_grid_html(
            page_data=page_data,
            flags_mapping=support_files.get("flags_mapping", {}),
            country=st.session_state.get("selected_country", "Kenya"),
            page_warnings=page_warnings,
            rejected_state=rejected_state,
            cols_per_row=cols_per_row,
            poor_img_sids=poor_img_rej_sids,
            prefetch_urls=prefetch_urls,
            scroll_to_top=scroll_top_flag,
            show_images=st.session_state.get("show_images", True),
            support_files=support_files,
            curr_sort=curr_sort,
            curr_flag=curr_flag,
            items_per_page=ipp,
        )

    # Unpack the grid html and its sync data
    (_grid_html_str, _committed_json, _poor_img_sids_json, _prefetch_json,
     _cards_json, _images_json, _cards_sig) = grid_html

    # Only ship the bulky halves when they are actually needed. Committed state
    # and poor-image flags are tiny and always sent; cards and the base64 image
    # map are not. A rerun from an unrelated widget now costs a few hundred
    # bytes instead of the whole page of cards and images.
    #
    # Two things force a resend. The obvious one is a changed card set. The
    # other is a changed grid document: Streamlit re-creates the iframe when its
    # srcdoc changes (e.g. only the columns-per-row buttons differ), and a fresh
    # iframe has empty CARDS/IMAGES. Keying off the card set alone would leave
    # that reloaded iframe with nothing to draw.
    _grid_html_sig = hashlib.md5(_grid_html_str.encode("utf-8")).hexdigest()
    _iframe_reloaded = st.session_state.get("_grid_last_html_sig") != _grid_html_sig
    _send_bulk = (
        st.session_state.get("_grid_last_sent_sig") != _cards_sig or _iframe_reloaded
    )
    st.session_state._grid_last_sent_sig = _cards_sig
    st.session_state._grid_last_html_sig = _grid_html_sig

    # Images for the current page are sent whenever the card set changes, and
    # the iframe holds them in a size-capped store. Tracking what the browser
    # already has and sending only the difference would save bytes, but it means
    # the browser accumulates every image the session ever displayed — trading a
    # server-side memory leak for a client-side one. A predictable ceiling
    # matters more here than avoiding a resend on revisit.
    _cards_field = f"cards: {_cards_json}," if _send_bulk else ""
    _images_field = f"images: {_images_json}," if _send_bulk else ""

    st.markdown("""
    <style>
    /* st.container(key=...) emits class="st-key-<key>". It does NOT emit a
       data-element-key attribute — measured against the live 1.60 DOM, that
       selector matches zero elements. This block was written with it and had
       therefore never applied: the iframe's height came entirely from the
       st.iframe(height=...) argument below, and both the calc(100vh - 190px)
       and the calc(100vh - 300px) that replaced it were dead letters. */
    div[data-testid="stElementContainer"]:has(iframe),
    div.st-key-grid_iframe_container,
    div.st-key-grid_iframe_container iframe {
        width: 100% !important;
        max-width: 100% !important;
    }
    /* Height is NOT set here. Streamlit sizes the stElementContainer wrapper
       from the Python height argument, so styling the iframe alone leaves the
       wrapper at its original height and opens an empty gap below the grid
       (measured: iframe 468px inside a 620px wrapper). Both boxes have to move
       together, and only the browser knows the viewport — so the grid sizes
       itself from inside via frameElement. See fitToViewport() in the grid JS.
       min-height guards the first paint, before that script has run. */
    div.st-key-grid_iframe_container iframe {
        min-height: 320px !important;
        max-height: 1600px !important;
    }
    </style>
    """, unsafe_allow_html=True)
    with st.container(key="grid_iframe_container"):
        _num_cards = len(page_data) if ('page_data' in locals() and isinstance(page_data, pd.DataFrame) and not page_data.empty) else 50
        _cols_n = max(1, st.session_state.get("grid_cols_per_row", 4))
        _num_rows = max(1, (_num_cards + _cols_n - 1) // _cols_n)
        _card_h = 360 if st.session_state.get("show_images", True) else 180

        # The iframe is a window onto the grid, not a container sized to hold
        # all of it. It used to be max(750, rows*card_h + 150) — a 750px floor
        # against ~612px of usable height on a 1366x768 laptop, so the grid
        # could never fit and the position:fixed batch bar sat below the fold.
        # Now the document inside scrolls (see #card-grid) and this only has to
        # be a comfortable viewport. Python cannot read the browser height, so
        # this is sized to fit the smallest screen the tool is used on and
        # short content still shrinks to fit.
        _GRID_VIEWPORT_H = 620
        _content_h = _num_rows * _card_h + 150
        _dynamic_iframe_h = min(_GRID_VIEWPORT_H, max(420, _content_h))
        st.iframe(_grid_html_str, height=_dynamic_iframe_h)
        # Inject a zero-height broadcaster that pushes state into the iframe via postMessage.
        # Sending cards via postMessage prevents the entire iframe DOM from being torn down
        # and rebuilt (which causes severe flickering) when changing pages.
        # The payload is built ONCE and reused by any retry. The old version
        # rebuilt this object literal on each of four attempts and posted it to
        # every frame with no success check — at 500 ZIP-image cards that was
        # hundreds of MB of structured-clone per render, which is what hung the
        # browser and dropped the websocket. Now: send, wait for the grid's
        # SYNC_ACK, and stop. Retries only happen if nothing acknowledged, which
        # covers the genuine race where the grid iframe has not booted yet.
        _sync_html = f"""
        <script>
        (function() {{
          var PAYLOAD = {{
            type: 'SYNC_STATE',
            committed: {_committed_json},
            poor_img_sids: {_poor_img_sids_json},
            prefetch: {_prefetch_json},
            {_cards_field}
            {_images_field}
            cards_sig: {json.dumps(_cards_sig)},
            scroll_to_top: {'true' if scroll_top_flag else 'false'}
          }};
          var acked = false;
          var timer = null;
          window.addEventListener('message', function(ev) {{
            if (!ev.data) return;
            if (ev.data.type === 'SYNC_ACK') {{
              acked = true;
              if (timer) {{ clearTimeout(timer); timer = null; }}
            }} else if (ev.data.type === 'GRID_READY') {{
              // The grid finished parsing and is listening. Deliver now
              // rather than hoping a blind retry coincides with it.
              send(0);
            }}
          }});
          function send(attemptsLeft) {{
            if (acked) return;
            try {{
              for (var i = 0; i < window.parent.frames.length; i++) {{
                try {{ window.parent.frames[i].postMessage(PAYLOAD, '*'); }} catch(e2) {{}}
              }}
            }} catch(e) {{}}
            if (attemptsLeft > 0) {{
              timer = setTimeout(function() {{ send(attemptsLeft - 1); }}, 400);
            }}
          }}
          // GRID_READY is the real trigger; this is the safety net for the
          // case where the grid announced itself before this script existed.
          // 12 attempts at 400ms covers ~5s of cold-start parsing, against
          // the 750ms the old blind loop allowed.
          send(12);
        }})();
        </script>
        """
        st.iframe(_sync_html, height=1)

    # One footer row instead of four stacked bands. Measured on a real batch,
    # the count line, the Prev/Next row, the progress bar and Close Review came
    # to ~180px between them, plus a horizontal rule and the gap left by the
    # zero-height sync iframe above. All of it now fits on one line, and since
    # the grid sizes itself to the space the dialog leaves, every pixel saved
    # here becomes a pixel of cards.
    #
    # The rule is gone: a divider immediately above a bordered button row draws
    # a line next to a line.
    _page_now = st.session_state.get("grid_page", 0)
    _pg = st.columns([0.9, 1.5, 0.9, 2.2, 1.1], vertical_alignment="center", gap="small")
    with _pg[0]:
        st.button(
            "⬅ Prev",
            key="prev_bot",
            use_container_width=True,
            disabled=_page_now == 0,
            on_click=_prev_page,
        )
    with _pg[1]:
        # Label collapsed — the caption beside it already says which page this
        # is and how many there are, so a label would repeat it.
        st.number_input(
            f"{len(review_data):,} items · page of {total_pages}",
            min_value=1,
            max_value=max(1, total_pages),
            key="jump_bot",
            on_change=_jump_from_widget,
            args=("jump_bot",),
            label_visibility="collapsed",
        )
    with _pg[2]:
        st.button(
            "Next ➡",
            key="next_bot",
            use_container_width=True,
            disabled=st.session_state.grid_page >= total_pages - 1,
            on_click=_next_page,
        )
    with _pg[3]:
        st.caption(f"{len(review_data):,} items · page {_page_now + 1} of {total_pages}")
        st.progress(min(1.0, (_page_now + 1) / total_pages))
    with _pg[4]:
        if st.button("✖ Close", key="close_bot_fallback", use_container_width=True, type="secondary"):
            st.session_state.show_review_modal = False
            # Tells the next run to cover the page while it rebuilds — see
            # render_grid_closing_overlay(). Cleared at the bottom of that run.
            st.session_state["_grid_closing"] = True
            st.rerun()


@st.fragment
def render_manual_review_buttons(support_files):
    _fr = st.session_state.get("final_report", pd.DataFrame())
    if _fr.empty or st.session_state.get("file_mode") == "post_qc":
        return

    if not _fr.empty and "Status" in _fr.columns and "FLAG" in _fr.columns:
        _rej_count = int((
            (_fr["Status"] == "Rejected") &
            (_fr["FLAG"].astype(str).str.contains(r"\(Prefetched\)", na=False, case=False))
        ).sum())
    else:
        _rej_count = 0

    _audit_label = (
        f"Targeted Audit  ({_rej_count:,} rejected)"
        if _rej_count
        else "Targeted Audit"
    )

    has_zip_in_fr = not _fr.empty and "Is_Zip" in _fr.columns and _fr["Is_Zip"].any()
    has_zip = has_zip_in_fr or st.session_state.get("no_computation_zip", False) or not st.session_state.get("zip_qc_results", pd.DataFrame()).empty

    st.markdown("---")

    if has_zip:
        c1, c2, c3 = st.columns([2, 1, 1], gap="medium")
        with c1:
            st.header(_t("manual_review"), anchor=False)
            st.caption(
                "Use **Start Visual Review** to manually flip through products one by one, "
                "or **Targeted Audit** to review all rejections grouped by flag type."
            )
        with c2:
            if st.button(
                "Start Visual Review",
                type="primary",
                width='stretch',
                icon=":material/pageview:",
                key="btn_visual_review_main",
            ):
                st.session_state.show_review_modal = True
                st.session_state.show_targeted_audit_modal = False
                # Opening the modal always builds a NEW iframe, whose
                # CARDS start empty. The resend guard keys off the grid
                # document's hash, and reopening produces byte-identical
                # HTML — so it concluded the old iframe was still alive
                # and sent nothing, leaving a blank grid until some
                # control changed the markup enough to force a resend.
                # That is what "empty until I change cards per row" was.
                st.session_state.pop("_grid_last_html_sig", None)
                st.session_state.pop("_grid_last_sent_sig", None)
        with c3:
            if st.button(
                _audit_label,
                type="secondary",
                width='stretch',
                icon=":material/ads_click:",
                key="btn_targeted_audit_main",
            ):
                st.session_state.show_targeted_audit_modal = True
                st.session_state.show_review_modal = False
    else:
        c1, c2 = st.columns([3, 1], gap="medium")
        with c1:
            st.header(_t("manual_review"), anchor=False)
            st.caption(
                "Use **Start Visual Review** to manually flip through products one by one."
            )
        with c2:
            if st.button(
                "Start Visual Review",
                type="primary",
                width='stretch',
                icon=":material/pageview:",
                key="btn_visual_review_main",
            ):
                st.session_state.show_review_modal = True
                st.session_state.show_targeted_audit_modal = False
                # Opening the modal always builds a NEW iframe, whose
                # CARDS start empty. The resend guard keys off the grid
                # document's hash, and reopening produces byte-identical
                # HTML — so it concluded the old iframe was still alive
                # and sent nothing, leaving a blank grid until some
                # control changed the markup enough to force a resend.
                # That is what "empty until I change cards per row" was.
                st.session_state.pop("_grid_last_html_sig", None)
                st.session_state.pop("_grid_last_sent_sig", None)

    if st.session_state.get("show_targeted_audit_modal", False):
        targeted_audit_modal(support_files)
    elif st.session_state.get("show_review_modal", False):
        visual_review_modal(support_files)


@st.fragment
def render_grid_closing_overlay():
    """Cover the page while a closing visual review is written back.

    Closing the grid flips a flag and reruns, and that rerun redraws the whole
    page — the KPI strip, every flag expander, the exports. On a large batch
    that is several seconds during which the modal has gone and nothing has
    replaced it, so the decisions just made look lost.

    Emitted from the TOP of the script so it paints before that work starts,
    and cleared by end_grid_closing_overlay() at the very bottom, once the
    page behind it is whole. Pure CSS: Streamlit strips <script> from
    st.markdown, and a component would render inside an iframe that cannot
    cover the page.
    """
    st.markdown(
        f"""
        <div id="grid-closing-overlay">
          <div class="gco-card">
            <div class="gco-ring"><span></span><span></span><span></span></div>
            <div class="gco-title">Saving your review…</div>
            <div class="gco-sub">Applying your decisions and rebuilding the
              report. This takes a moment on a large batch — please don't
              refresh.</div>
            <div class="gco-bar"><i></i></div>
          </div>
        </div>
        <style>
          #grid-closing-overlay {{
            position: fixed; inset: 0; z-index: 2147483000;
            display: flex; align-items: center; justify-content: center;
            background: rgba(17, 24, 39, .55);
            backdrop-filter: blur(3px);
            animation: gcoIn .18s ease-out;
          }}
          @keyframes gcoIn {{ from {{ opacity: 0 }} to {{ opacity: 1 }} }}
          .gco-card {{
            width: min(420px, 86vw); padding: 30px 32px 26px;
            border-radius: 16px; text-align: center;
            background: {DT['surface']}; color: {DT['ink']};
            box-shadow: 0 24px 60px rgba(0,0,0,.32);
            border: 1px solid {DT['hairline']};
            animation: gcoPop .28s cubic-bezier(.34,1.56,.64,1);
          }}
          @keyframes gcoPop {{
            from {{ transform: translateY(10px) scale(.97); opacity: 0 }}
            to   {{ transform: none; opacity: 1 }}
          }}
          /* Three arcs at different speeds — reads as motion even on a frame
             the browser drops while the script is busy. */
          .gco-ring {{ position: relative; width: 54px; height: 54px; margin: 0 auto 16px; }}
          .gco-ring span {{
            position: absolute; inset: 0; border-radius: 50%;
            border: 3px solid transparent; border-top-color: {DT['accent']};
            animation: gcoSpin 1.1s linear infinite;
          }}
          .gco-ring span:nth-child(2) {{
            inset: 8px; border-top-color: {DT['accent']}; opacity: .55;
            animation-duration: 1.6s; animation-direction: reverse;
          }}
          .gco-ring span:nth-child(3) {{
            inset: 16px; border-top-color: {DT['ink']}; opacity: .28;
            animation-duration: 2.1s;
          }}
          @keyframes gcoSpin {{ to {{ transform: rotate(360deg) }} }}
          .gco-title {{ font-size: 1.05rem; font-weight: 700; letter-spacing: -.01em; }}
          .gco-sub {{
            margin-top: 7px; font-size: .82rem; line-height: 1.45;
            color: {DT['ink_muted']};
          }}
          .gco-bar {{
            margin-top: 18px; height: 4px; border-radius: 99px; overflow: hidden;
            background: {DT['hairline_soft']};
          }}
          /* Indeterminate: the work has no measurable progress, so a bar that
             claimed a percentage would be inventing one. */
          .gco-bar i {{
            display: block; width: 38%; height: 100%; border-radius: 99px;
            background: {DT['accent']};
            animation: gcoSlide 1.15s ease-in-out infinite;
          }}
          @keyframes gcoSlide {{
            0%   {{ transform: translateX(-100%) }}
            100% {{ transform: translateX(320%) }}
          }}
          @media (prefers-reduced-motion: reduce) {{
            #grid-closing-overlay, .gco-card {{ animation: none }}
            .gco-ring span, .gco-bar i {{ animation-duration: 3s }}
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def end_grid_closing_overlay():
    """Hide the overlay once the page behind it has finished rendering.

    A later stylesheet of equal specificity wins, so this needs no JS and no
    second rerun — the overlay simply stops applying the moment this element
    is emitted at the end of the script.
    """
    st.markdown(
        "<style>#grid-closing-overlay{display:none !important}</style>",
        unsafe_allow_html=True,
    )


def render_image_grid(support_files):
    if (
        st.session_state.get("final_report", pd.DataFrame()).empty
        or st.session_state.get("file_mode") == "post_qc"
    ):
        return

    _warm_urls = st.session_state.get("_grid_warm_urls", [])
    if _warm_urls:
        _preload_tags = "\n".join(
            f'<link rel="preload" as="image" href="{url}" referrerpolicy="no-referrer">'
            for url in _warm_urls[:100]
        )
        st.markdown(
            f"<div style='display:none'>{_preload_tags}</div>", unsafe_allow_html=True
        )


def _render_export_card(title, df, desc, func, exports_config):
    with st.container(border=True):
        st.markdown(f"**{title}**")
        st.markdown(
            f'<div style="height: 65px; overflow: hidden; font-size: 0.85rem; color: #6b7a8d; margin-bottom: 10px;">{desc}</div>',
            unsafe_allow_html=True,
        )
        st.metric(label="Rows", value=f"{len(df):,}")
        if title not in st.session_state.exports_cache:
            if st.button(
                "Generate",
                key=f"gen_{title}",
                type="primary",
                width='stretch',
                icon=":material/download:",
                icon_position="left",
            ):
                with st.spinner("Generating all reports…"):
                    for t2, d2, _, f2 in exports_config:
                        if t2 not in st.session_state.exports_cache:
                            res, fname, mime = f2(d2)
                            st.session_state.exports_cache[t2] = {
                                "data": res.getvalue(),
                                "fname": fname,
                                "mime": mime,
                            }
                st.rerun()
        else:
            cache = st.session_state.exports_cache[title]
            st.download_button(
                "Download",
                data=cache["data"],
                file_name=cache["fname"],
                mime=cache["mime"],
                width='stretch',
                type="primary",
                icon=":material/file_download:",
                key=f"dl_{title}",
            )
            if st.button("Clear", key=f"clr_{title}", width='stretch'):
                del st.session_state.exports_cache[title]
                st.rerun()


@st.fragment
def render_exports_section(support_files, country_validator):
    if (
        st.session_state.final_report.empty
        or st.session_state.get("file_mode") == "post_qc"
    ):
        return

    from datetime import datetime

    fr = st.session_state.final_report
    data = st.session_state.all_data_map

    if st.session_state.get("all_data_rows") is None:
        if "_data_filtered_ref" in st.session_state:
            st.session_state.all_data_rows = st.session_state._data_filtered_ref
        elif "current_sig_hash" in st.session_state:
            _fname = f"{st.session_state.current_sig_hash}_data_rows.parquet"
            st.session_state.all_data_rows = load_df_parquet(_fname)

    all_rows = st.session_state.get("all_data_rows", data)
    app_df = fr[fr["Status"] == "Approved"]
    rej_df = fr[fr["Status"] == "Rejected"]
    c_code = country_validator.code
    date_str = datetime.now().strftime("%Y-%m-%d")
    reasons_df = support_files.get("reasons", pd.DataFrame())

    st.markdown("---")
    st.header(_t("download_reports"), anchor=False)
    st.caption("Export QC results in Excel or ZIP format")

    def _gen_override_report(log):
        import io
        df = pd.DataFrame(log) if log else pd.DataFrame(
            columns=["Timestamp", "SID", "Product Name", "Rejection Flag",
                     "Seller", "AI Reason", "Action"]
        )
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return buf, f"{c_code}_Audit_Overrides_{date_str}.csv", "text/csv"

    exports_config = [
        (
            "QC Export",
            fr,
            "Complete QC report with all statuses",
            lambda df: generate_smart_export(
                df, f"{c_code}_QC_Export_{date_str}", "simple", reasons_df
            ),
        ),
        (
            "Rejected Only",
            rej_df,
            "Products that failed QC validation",
            lambda df: generate_smart_export(
                df, f"{c_code}_Rejected_{date_str}", "simple", reasons_df
            ),
        ),
        (
            "Approved Only",
            app_df,
            "Products that passed QC validation",
            lambda df: generate_smart_export(
                df, f"{c_code}_Approved_{date_str}", "simple", reasons_df
            ),
        ),
        (
            "Full Data",
            data,
            "Complete dataset with QC flags for every processed row",
            lambda df: generate_smart_export(
                prepare_full_data_merged(df, fr), f"{c_code}_Full_{date_str}", "full"
            ),
        ),
    ]

    all_cached = all(
        t in st.session_state.exports_cache for t, _, _, _ in exports_config
    )
    if all_cached:
        st.success("All reports generated and ready to download.")
    else:
        if st.button("Generate All Reports", type="primary", width='stretch'):
            with st.spinner("Generating all reports…"):
                for t2, d2, _, f2 in exports_config:
                    if t2 not in st.session_state.exports_cache:
                        res, fname, mime = f2(d2)
                        st.session_state.exports_cache[t2] = {
                            "data": res.getvalue(),
                            "fname": fname,
                            "mime": mime,
                        }
            st.rerun()

    cols_count = 4 if st.session_state.get("layout_mode") == "wide" else 2
    for i in range(0, len(exports_config), cols_count):
        cols = st.columns(cols_count)
        for j, col in enumerate(cols):
            if i + j < len(exports_config):
                title, df, desc, func = exports_config[i + j]
                with col:
                    _render_export_card(title, df, desc, func, exports_config)