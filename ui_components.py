"""
ui_components.py - All Streamlit UI rendering components, dialogs, and the image grid
"""

import base64
import concurrent.futures
import json
import logging
import re
import zipfile
from io import BytesIO

import orjson
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image

from constants import GRID_COLS, JUMIA_COLORS
from data_utils import (
    _get_image_from_zip,
    clean_category_code,
    df_hash,
    format_local_price,
    load_df_parquet,
)
from export_utils import generate_smart_export, prepare_full_data_merged

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
        "Product name_Brand name_rejection reason",
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
}


def flag_pill_header(flag_name: str, count: int, is_zip: bool = False) -> str:
    color_map = {
        "Wrong Category": ("#fef3c7", "#d97706"),
        "Restricted brands": ("#fee2e2", "#dc2626"),
        "Suspected Fake product": ("#fee2e2", "#b91c1c"),
        "BRAND name repeated in NAME": ("#ede9fe", "#7c3aed"),
        "Duplicate product": ("#dcfce7", "#15803d"),
    }
    bg, fg = color_map.get(flag_name, ("#f3f4f6", "#374151"))
    
    zip_badge = (
        ' <span style="background:linear-gradient(135deg, #3b82f6, #1d4ed8);color:white;border-radius:6px;padding:2px 8px;font-size:10px;font-weight:900;box-shadow:0 2px 4px rgba(0,0,0,0.1);margin-left:8px;">ZIP</span>'
        if is_zip
        else ""
    )
    
    return (
        f'<div style="display:flex;align-items:center;padding:10px 0;">'
        f'<span style="background:{fg};color:white;border-radius:8px;'
        f'padding:4px 12px;font-size:14px;font-weight:900;box-shadow:0 4px 12px {bg};">{count}</span>'
        f'<span style="font-size:16px;font-weight:700;margin-left:12px;color:#1f2937;">{flag_name}</span>'
        f'{zip_badge}</div>'
    )


def render_kpi_bar(final_report: pd.DataFrame):
    total = len(final_report)
    approved = int((final_report["Status"] == "Approved").sum())
    rejected = int((final_report["Status"] == "Rejected").sum())
    zip_rej = (
        int((final_report["Is_Zip"] == True).sum())
        if "Is_Zip" in final_report.columns
        else 0
    )
    pct = round(approved / total * 100, 1) if total else 0
    trend_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 40 else "#ef4444"

    st.html(
        f"""
    <style>
      .kpi-strip {{
        display:grid; grid-template-columns:repeat(4,1fr);
        gap:12px; margin-bottom:8px;
      }}
      .kpi-card {{
        background:#fff; border:1px solid #e5e7eb;
        border-radius:12px; padding:16px 20px;
        box-shadow:0 1px 3px rgba(0,0,0,.06);
        transition:transform .15s,box-shadow .15s;
      }}
      .kpi-card:hover {{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,.10);}}
      .kpi-label {{font-size:11px;font-weight:600;color:#6b7280;letter-spacing:.06em;text-transform:uppercase;}}
      .kpi-value {{font-size:28px;font-weight:800;margin:4px 0;}}
      .kpi-sub   {{font-size:11px;color:#9ca3af;}}
      .kpi-bar   {{height:4px;border-radius:99px;margin-top:10px;background:#f3f4f6;}}
      .kpi-fill  {{height:4px;border-radius:99px;background:{trend_color};
                   width:{pct}%;transition:width .8s cubic-bezier(.4,0,.2,1);}}
    </style>
    <div class="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-label">Total SKUs</div>
        <div class="kpi-value" style="color:#111827">{total:,}</div>
        <div class="kpi-sub">Unique product sets</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Approved</div>
        <div class="kpi-value" style="color:#16a34a">{approved:,}</div>
        <div class="kpi-sub">{pct}% approval rate</div>
        <div class="kpi-bar"><div class="kpi-fill"></div></div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Rejected</div>
        <div class="kpi-value" style="color:#dc2626">{rejected:,}</div>
        <div class="kpi-sub">{zip_rej} from ZIP/prefetch</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Approval Rate</div>
        <div class="kpi-value" style="color:{trend_color}">{pct}%</div>
        <div class="kpi-sub">{"Good" if pct>=70 else "Review needed" if pct>=40 else "High rejection"}</div>
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
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _base_prefetched_title(title: str) -> str:
    return str(title).replace("(Prefetched)", "").strip()


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


def _clear_result_caches() -> None:
    st.session_state.exports_cache.clear()
    st.session_state.display_df_cache.clear()
    st.session_state.pop("_grid_review_data_cache", None)
    st.session_state.pop("_grid_warm_urls", None)


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
        mask = df["PRODUCT_SET_SID"].astype(str).str.strip().isin(sid_set)
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
        data["PRODUCT_SET_SID"].astype(str).str.strip().isin(sid_set)
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
) -> int:
    sid_set = _normalize_sid_set(sids)
    
    # ── Global Similar Image Rejection ──
    # If this is an image-related rejection, apply it to ALL products sharing the same image globally.
    is_image_rej = status == "Rejected" and any(x in str(flag).lower() for x in ["image", "stretched", "blurry", "poor", "mismatch"])
    if is_image_rej:
        all_data = st.session_state.get("all_data_map")
        if all_data is not None and "PRODUCT_SET_SID" in all_data.columns and "IMAGE1" in all_data.columns:
            # 1. Identify the unique images associated with the input SIDs
            _input_sids_clean = list(sid_set)
            _target_images = set(
                all_data[all_data["PRODUCT_SET_SID"].astype(str).str.strip().isin(_input_sids_clean)]["IMAGE1"]
                .dropna().unique()
            )
            # 2. Find every other SID in the entire dataset that uses these images
            if _target_images:
                _global_similar_sids = set(
                    all_data[all_data["IMAGE1"].isin(_target_images)]["PRODUCT_SET_SID"]
                    .astype(str).str.strip().unique()
                )
                sid_set.update(_global_similar_sids)

    fr = st.session_state.get("final_report", pd.DataFrame())
    if (
        not sid_set
        or not isinstance(fr, pd.DataFrame)
        or fr.empty
        or "ProductSetSid" not in fr.columns
    ):
        return 0

    mask = fr["ProductSetSid"].astype(str).str.strip().isin(sid_set)
    if not mask.any():
        return 0

    from datetime import datetime

    # 🚀 Store snapshot for Undo functionality
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
        for sid in sid_set:
            if status == "Rejected":
                st.session_state[f"quick_rej_{sid}"] = True
                st.session_state[f"quick_rej_reason_{sid}"] = flag or comment or reason
            else:
                st.session_state.pop(f"quick_rej_{sid}", None)
                st.session_state.pop(f"quick_rej_reason_{sid}", None)

    _clear_result_caches()

    # 🚀 Trigger Undo Toast for bulk actions
    if len(sid_set) > 1:
        st.session_state["show_undo_toast"] = {
            "count": len(sid_set),
            "status": status,
            "time": datetime.now(),
        }

    return int(mask.sum())


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
        st.dataframe(_preview_df, hide_index=True, use_container_width=True)
    if st.button(_t("approve_btn"), type="primary", use_container_width=True):
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
                _future.result()
            _progress.progress(1.0, text="Done!")
            _progress.empty()
            msg_moved, msg_approved = {}, 0
            for sid in sids_to_process:
                sid_str = str(sid).strip()
                if apply_status_change(
                    [sid_str],
                    status="Approved",
                    reason="",
                    comment="",
                    flag="Approved by User",
                    is_manual=True,
                    is_zip=False,
                ):
                    msg_approved += 1

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

    # 🚀 Pre-detect image column to ensure it's kept in the merge
    possible_img_cols = [
        "image1",
        "MAIN_IMAGE_URL",
        "MAIN_IMAGE",
        "IMAGE_URL",
        "IMAGE1_ZIP",
    ]
    img_col = next((c for c in possible_img_cols if c in data.columns), None)
    if img_col and img_col not in current_display_cols:
        current_display_cols.append(img_col)

    if cache_key not in st.session_state.display_df_cache:
        _extra_cols = [c for c in current_display_cols if c in data.columns]
        if "CATEGORY_CODE" in data.columns and "CATEGORY_CODE" not in _extra_cols:
            _extra_cols.append("CATEGORY_CODE")

        if "PRODUCT_SET_SID" not in _extra_cols:
            _extra_cols.append("PRODUCT_SET_SID")

        # Ensure Is_Zip exists (may be absent on first render before any
        # apply_status_change call, or when loaded from a cached parquet).
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
        df_display = pd.merge(
            df_flagged_sids[["ProductSetSid", "Is_Zip"]],
            data,
            left_on="ProductSetSid",   # FIX #3: was "ProjectSetSid" (typo), dead ternary removed
            right_on="PRODUCT_SET_SID",
            how="left",
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
            df_display["CATEGORY"] = df_display["CATEGORY_CODE"].apply(
                lambda c: _code_to_path.get(str(c).strip(), "") if pd.notna(c) else ""
            )
            df_display = df_display.drop(columns=["CATEGORY_CODE"])
        _final_cols = list(
            dict.fromkeys(
                [c for c in current_display_cols if c in df_display.columns]
                + ["Is_Zip"]
            )
        )
        df_display = df_display[_final_cols]
        st.session_state.display_df_cache[cache_key] = df_display
    else:
        df_display = st.session_state.display_df_cache[cache_key]

    # 🚀 Add "Show Images" toggle here for the detailed view
    show_table_images = st.toggle(
        "Show Image Previews",
        value=st.session_state.get("show_table_images", False),
        key=f"tg_img_{title}",
    )
    st.session_state.show_table_images = show_table_images

    c1, c2 = st.columns([1, 1], gap="large")
    with c1:
        search_term = st.text_input(
            _t("search_grid"),
            placeholder="Name, Brand...",
            icon=":material/search:",
            key=f"s_{title}",
        )
    with c2:
        _seller_key = f"f_{title}"
        seller_filter = st.multiselect(
            "Filter by Seller",
            sorted(df_display["SELLER_NAME"].astype(str).unique()),
            default=st.session_state.get(f"_sf_{title}", []),
            key=_seller_key,
        )
        st.session_state[f"_sf_{title}"] = seller_filter

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
    if seller_filter:
        df_view = df_view[df_view["SELLER_NAME"].isin(seller_filter)]
    df_view = df_view.reset_index(drop=True)

    if "NAME" in df_view.columns:
        df_view["NAME"] = df_view["NAME"].apply(
            lambda t: re.sub("<[^<]+?>", "", t) if isinstance(t, str) else t
        )

    # Image column is already in img_col from above
    if img_col and img_col in df_view.columns:

        def get_img(row):
            sid = row.get("PRODUCT_SET_SID")
            name = row.get("NAME", "")
            brand = row.get("BRAND", "")
            img_val = row.get(img_col, "")
            if pd.isna(img_val):
                img_val = ""
            zip_img = _get_image_from_zip(name, brand, img_val)
            if zip_img:
                return zip_img
            if (
                "IMAGE1_ZIP" in row
                and pd.notna(row["IMAGE1_ZIP"])
                and str(row["IMAGE1_ZIP"]).startswith("http")
            ):
                return str(row["IMAGE1_ZIP"])
            if str(img_val).startswith("http"):
                return str(img_val).replace("http://", "https://", 1)
            return None

        if show_table_images:
            df_view["Image Preview"] = df_view.apply(get_img, axis=1)
    if "GLOBAL_PRICE" in df_view.columns and "GLOBAL_SALE_PRICE" in df_view.columns:

        def _local_p(row):
            sp, rp = row.get("GLOBAL_SALE_PRICE"), row.get("GLOBAL_PRICE")
            val = sp if pd.notna(sp) and str(sp).strip() != "" else rp
            return format_local_price(val, country_validator.country)

        df_view.insert(
            df_view.columns.get_loc("GLOBAL_PRICE") + 1
            if "GLOBAL_PRICE" in df_view.columns
            else len(df_view.columns),
            "Local Price",
            df_view.apply(_local_p, axis=1),
        )

    def style_rows(row):
        if row.get("Is_Zip"):
            return ["color: #ff4b4b; font-weight: 900;"] * len(row)
        return [""] * len(row)

    df_styled = df_view.style.apply(style_rows, axis=1)



    event = st.dataframe(
        df_styled,
        hide_index=True,
        use_container_width=True,
        selection_mode="multi-row",
        on_select="rerun",
        column_config={
            "PRODUCT_SET_SID": st.column_config.TextColumn(pinned=True),
            "NAME": st.column_config.TextColumn(pinned=True),
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
            "Is_Zip": None,  # Hide the helper column
            "Image Preview": None,  # Don't show in table
        },
        key=f"df_{title}_{st.session_state.get(f'df_ver_{title}', 0)}",
    )

    # 🚀 NEW: Grid View Implementation (BELOW the table)
    if show_table_images and not df_view.empty:
        fr = st.session_state.get("final_report", pd.DataFrame())
        # Filter final_report for these specific SIDs to get their comments/flags
        sids_in_view = df_view["PRODUCT_SET_SID"].astype(str).tolist()
        fr_subset = fr[fr["ProductSetSid"].astype(str).isin(sids_in_view)].set_index(
            "ProductSetSid"
        )

        # 🚀 Use Streamlit containers for robust rendering + ZIP support (Max 50)
        grid_data = df_view.head(50)
        if len(df_view) > 50:
            st.info(f"Showing first 50 of {len(df_view)} items in grid view.")

        st.markdown(
            """
        <style>
            [data-testid="stVerticalBlockBorderWrapper"] {
                height: 520px !important;
                display: flex;
                flex-direction: column;
                margin-bottom: 20px;
                position: relative;
            }
            .grid-price-badge {
                position: absolute;
                top: 80px;
                left: 10px;
                background: rgba(246, 139, 30, 0.95);
                color: white;
                padding: 4px 10px;
                border-radius: 4px;
                font-weight: 800;
                font-size: 13px;
                z-index: 100;
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            }
            .grid-reason {
                background: #fff5f5;
                border-bottom: 1px solid #ffe3e3;
                padding: 12px;
                color: #d63031;
                font-size: 14px;
                border-left: 5px solid #ff7675;
                height: 70px;
                overflow-y: auto;
                font-weight: 700;
            }
            .grid-name {
                font-size: 15px;
                font-weight: 700;
                color: #2d3436;
                margin-bottom: 8px;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
                height: 44px;
                line-height: 1.4;
            }
            .grid-reason-bot {
                background: #fffafa;
                border-top: 1px solid #ffe3e3;
                padding: 12px;
                color: #e17055;
                font-size: 14px;
                border-left: 5px solid #fab1a0;
                height: 100px;
                overflow-y: auto;
                font-style: italic;
                font-weight: 500;
            }
        </style>
        """,
            unsafe_allow_html=True,
        )

        cols = st.columns(4)
        for i, (_, row) in enumerate(grid_data.iterrows()):
            sid = str(row["PRODUCT_SET_SID"])
            name = str(row.get("NAME", ""))
            img_url = row.get("Image Preview")
            if (
                pd.isna(img_url)
                or not str(img_url).strip()
                or str(img_url).lower() == "none"
            ):
                img_url = _NO_IMAGE_SVG

            # Get flag info from final_report
            fr_row = fr_subset.loc[sid] if sid in fr_subset.index else None
            ai_reason = _prefetched_reason_for_row(title, row)

            reason_top = ""
            reason_bot = ""
            if fr_row is not None:
                display_comment = (
                    ai_reason
                    if (row.get("Is_Zip") and ai_reason != "No reason provided")
                    else fr_row["Comment"]
                )
                # 🚀 Remove prefix, just show the actual reason
                reason_bot = display_comment
                if "Brand" in fr_row["FLAG"] or "Restricted" in fr_row["FLAG"]:
                    reason_top = reason_bot

            with cols[i % 4]:
                with st.container(border=True):
                    # Top Label
                    if reason_top:
                        st.markdown(
                            f'<div class="grid-reason">{reason_top}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            '<div style="height: 70px;"></div>', unsafe_allow_html=True
                        )  # Spacer

                    # Price Badge
                    local_price = row.get("Local Price", "")
                    if local_price:
                        st.markdown(f'<div class="grid-price-badge">{local_price}</div>', unsafe_allow_html=True)

                    # Image (Streamlit handles ZIP extraction here)
                    st.image(img_url, use_container_width=True)

                    # Details
                    st.markdown(
                        f"""
                        <div style="flex-grow: 1; padding: 10px 0;">
                            <div class="grid-name">{name}</div>
                            <div style="font-size: 11px; color: #666; margin-bottom: 4px; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 1.2;" title="{row.get('CATEGORY', '')}">Category: {row.get('CATEGORY', 'N/A')}</div>
                            <div style="font-size: 12px; color: #b2bec3; font-weight: 500;">SID: {sid}</div>
                        </div>
                        <div class="grid-reason-bot">{reason_bot}</div>
                    """,
                        unsafe_allow_html=True,
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
        "Wrong Category",
        "Restricted brands",
        "Suspected Fake product",
        "Seller Not approved to sell Refurb",
        "Product Warranty",
        "Seller Approve to sell books",
        "Seller Approved to Sell Perfume",
        "Counterfeit Sneakers",
        "Suspected counterfeit Jerseys",
        "Prohibited products",
        "Unnecessary words in NAME",
        "Single-word NAME",
        "Generic BRAND Issues",
        "Fashion brand issues",
        "BRAND name repeated in NAME",
        "Wrong Variation",
        "Generic branded products with genuine brands",
        "Missing COLOR",
        "Missing Weight/Volume",
        "Incomplete Smartphone Name",
        "Duplicate product",
        "Poor images",
        "Image Stretched",
        "Image Blurry",
        "Image Mismatch",
        "Image Infringing",
        "Image Too Many things displayed",
        "Perfume Tester",
        "NG - Gift Card Seller",
        "NG - Books Seller",
        "NG - TV Brand Seller",
        "NG - HP Toners Seller",
        "NG - Apple Seller",
        "NG - Xmas Tree Seller",
        "NG - Rice Brand Seller",
        "NG - Powerbank Capacity",
        "Discount too high",
        "Category Max Price Exceeded",
        "Suspicious Discount",
        "Color Mismatch",
        "Other Reason (Custom)",
    ]

    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button(
            _t("approve_btn"),
            key=f"approve_sel_{title}",
            type="primary",
            use_container_width=True,
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
            use_container_width=True,
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
                    use_container_width=True,
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
                    use_container_width=True,
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
    seller_trust=None,
    support_files=None,
):
    if seller_trust is None: seller_trust = {}
    if support_files is None: support_files = {}
    
    from translations import get_translation
    lang = "fr" if country == "Morocco" else "en"

    def _t(key): return get_translation(lang, key)

    O = JUMIA_COLORS["primary_orange"]
    G = JUMIA_COLORS["success_green"]
    R = JUMIA_COLORS["jumia_red"]
    committed_json = json.dumps(rejected_state)
    poor_img_sids_json = json.dumps(list(poor_img_sids or []))
    prefetch_json = json.dumps(prefetch_urls or [])
    html_dir = "rtl" if st.session_state.get("ui_lang") == "ar" else "ltr"

    labels_dict = {
        "poor_img": _t("poor_img"),
        "wrong_cat": _t("wrong_cat"),
        "fake_prod": _t("fake_prod"),
        "restr_brand": _t("restr_brand"),
        "wrong_brand": _t("wrong_brand"),
        "prohibited": _t("prohibited"),
        "missing_color": _t("missing_color"),
        "more_options": _t("more_options"),
        "undo": _t("undo"),
        "approve": _t("approve_btn"),
        "clear_sel": _t("clear_sel"),
        "items_pending": _t("items_pending"),
        "batch_reject": _t("batch_reject"),
        "select_all": _t("select_all"),
        "deselect_all": _t("deselect_all"),
        "rejected": str(_t("rejected") or "REJECTED").upper(),
    }
    labels_json = json.dumps(labels_dict)

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

    cards_data = []
    for _, row in page_data.iterrows():
        sid = str(row["PRODUCT_SET_SID"])
        img_url = str(row.get("MAIN_IMAGE", "")).strip()
        if img_url.startswith("http"):
            img_url = img_url.replace("http://", "https://", 1)
        elif img_url:
            name = str(row.get("NAME", "")).strip()
            brand = str(row.get("BRAND", "")).strip()
            img_data = _get_image_from_zip(name, brand, img_url)
            if img_data:
                img_url = img_data
            else:
                img_url = ""

        # 🚀 Fallback to IMAGE1_ZIP
        if (not img_url or img_url == "") and "IMAGE1_ZIP" in row:
            _fallback = str(row.get("IMAGE1_ZIP", "")).strip()
            if _fallback.startswith("http"):
                img_url = _fallback.replace("http://", "https://", 1)

        sale_p = row.get("GLOBAL_SALE_PRICE")
        reg_p = row.get("GLOBAL_PRICE")
        usd_val = sale_p if pd.notna(sale_p) and str(sale_p).strip() != "" else reg_p
        price_str = (
            format_local_price(
                usd_val, st.session_state.get("selected_country", "Kenya")
            )
            if pd.notna(usd_val)
            else ""
        )

        color_val = str(row.get("COLOR", "")).strip()
        if color_val.lower() in ("nan", "none", "null"):
            color_val = ""

        cards_data.append(
            {
                "sid": sid,
                "img": img_url if show_images else _PLACEHOLDER_SVG,
                "name": str(row.get("NAME", "")),
                "brand": str(row.get("BRAND", "Unknown Brand")),
                "cat": str(row.get("CATEGORY", "Unknown Category")),
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
            }
        )

    cards_json = orjson.dumps(cards_data).decode("utf-8")

    scroll_js = ""
    if scroll_to_top:
        scroll_js = "sessionStorage.removeItem('__inner_iframe_scroll__'); window.scrollTo(0, 0);"
    else:
        scroll_js = """
        var savedInnerScroll = sessionStorage.getItem('__inner_iframe_scroll__');
        if (savedInnerScroll) {
            setTimeout(function() {
                window.scrollTo({top: parseInt(savedInnerScroll, 10), behavior: 'instant'});
            }, 50);
        }
        """

    return f"""<!DOCTYPE html>
<html dir="{html_dir}">
<head>
<meta charset="utf-8">
<meta name="referrer" content="no-referrer">
<style>
  :root {{
    --bg: #f9fafb;
    --card: #ffffff;
    --text: #111827;
    --border: #e5e7eb;
    --accent: {O};
  }}
  *{{box-sizing:border-box;margin:0;padding:0;font-family:sans-serif;}}
  body{{background:var(--bg);color:var(--text);padding:8px;overflow-x:hidden;width:100%;transition:background .2s, color .2s;}}

  .ctrl-bar{{position:-webkit-sticky;position:sticky;top:0;z-index:99999;display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 12px;background:var(--card);backdrop-filter:blur(8px);border-bottom:2px solid var(--accent);border-radius:4px;margin-bottom:12px;box-shadow:0 4px 16px rgba(0,0,0,0.15);}}
  
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
  #dark-toggle {{
    padding: 6px 12px;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--card);
    color: var(--text);
    cursor: pointer;
    font-size: 13px;
    font-weight: 600;
  }}
  #dark-toggle:hover {{ background: #f3f4f6; }}

  .bottom-bar {{position: relative; border-bottom: none; border-top: 2px solid {O}; margin-top: 16px; margin-bottom: 0; z-index: 10; box-shadow: 0 -4px 16px rgba(0,0,0,0.05);}}

  .sel-count{{font-weight:700;color:{O};font-size:13px;min-width:80px;}}
  .reason-sel{{flex:1;min-width:160px;padding:6px 10px;border:1px solid #ccc;border-radius:4px;font-size:12px;background:#fff;cursor:pointer;}}
  .batch-btn{{padding:7px 14px;background:{O};color:#fff;border:none;border-radius:4px;font-weight:700;font-size:12px;cursor:pointer;}}
  .batch-btn:hover{{opacity:.88;}}
  .desel-btn{{padding:7px 12px;background:#fff;color:#555;border:1px solid #ccc;border-radius:4px;font-size:12px;cursor:pointer;}}
  .desel-btn:hover{{background:#f5f5f5;}}
  .top-btn {{margin-left: auto; background: #313133; color: white; border-color: #313133; font-weight: bold;}}
  .top-btn:hover {{background: #000; color: white;}}

  .grid{{display:grid;grid-template-columns:repeat({cols_per_row},minmax(0,1fr));gap:12px;width:100%;}}
  .card{{border:2px solid var(--border);border-radius:8px;padding:10px;background:var(--card);position:relative;transition:border-color .15s,box-shadow .15s,transform .2s;z-index:1;min-width:0;word-wrap:break-word;display:flex;flex-direction:column;min-height:360px;outline:none;}}
  .card:focus {{ border-color: var(--accent); box-shadow: 0 0 0 3px rgba(246,139,30,0.3); transform: translateY(-4px); }}

  .card.selected{{border-color:{O};box-shadow:0 0 0 5px rgba(255,136,0,.35);background:rgba(255,136,0,.04);}}
  .card.staged-rej{{border-color:{R};box-shadow:0 0 0 4px rgba(231,60,23,.3);background:rgba(231,60,23,.04);}}
  .card.committed-rej{{border-color:#bbb;opacity:.6;}}

  .card-img-wrap{{position:relative;cursor:pointer;border-radius:8px;background:#fff;display:flex;align-items:center;justify-content:center;height:180px;overflow:hidden; border:1px solid #111;flex-shrink:0;}}
  .card-img-wrap::before{{content:'';position:absolute;inset:0;background:linear-gradient(90deg,#FFF8F2 25%,#FFEFE5 50%,#FFF8F2 75%);background-size:200% 100%;animation:shimmer 1.4s infinite;z-index:1;}}
  .card-img-wrap.img-loaded::before{{display:none;}}
  @keyframes shimmer{{0%{{background-position:200% 0}}100%{{background-position:-200% 0}}}}
  .card-img-placeholder{{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:1;}}
  .card-img{{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:2;opacity:0;transition:opacity .4s ease;}}
  .card-img.img-loaded{{opacity:1;}}
  .card.committed-rej .card-img{{filter:grayscale(80%);}}

  .warn-wrap{{position:absolute;top:8px;right:8px;display:flex;flex-direction:column;gap:4px;z-index:10;pointer-events:none;}}
  .warn-badge{{background:linear-gradient(90deg,#FFC107,#FF9800);color:#313133;font-size:9px;font-weight:800;padding:3px 8px;border-radius:9999px;box-shadow:0 2px 6px rgba(255,152,0,.3);animation:pulse 2s infinite;}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:0.85}}}}
  .price-badge{{position:absolute;top:8px;left:8px;background:rgba(246,139,30,0.95);color:#fff;font-size:10px;font-weight:800;padding:3px 8px;border-radius:9999px;z-index:10;pointer-events:none;box-shadow:0 2px 6px rgba(0,0,0,.2);}}

  .meta{{font-size:11px;margin-top:8px;line-height:1.4;flex-grow:1;display:flex;flex-direction:column;}}
  .meta .nm{{font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:help;}}
  .meta .br{{color:{O};font-weight:700;margin:2px 0;}}
  .meta .ct{{color:#666;font-size:10px;word-break:break-word;}}
  .meta .sl{{color:#999;font-size:9px;margin-top:4px;border-top:1px dashed #eee;padding-top:4px;cursor:help;}}
  .meta .co{{color:#555;font-size:10px;margin-top:4px;background:#f0f0f0;padding:3px 5px;border-radius:4px;display:inline-block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%;font-weight:600;}}

  .acts{{display:flex;gap:4px;margin-top:auto;padding-top:8px;}}
  .act-btn{{flex:1;padding:6px;font-size:11px;border:none;border-radius:4px;cursor:pointer;font-weight:700;color:#fff;background:{O};}}
  .act-more{{flex:1;font-size:11px;border:1px solid #ccc;border-radius:4px;outline:none;cursor:pointer;background:#fff;}}

  .zoom-btn{{position:absolute;bottom:6px;right:6px;width:22px;height:22px;background:rgba(0,0,0,0.4);color:#fff;border-radius:4px;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:25;border:none;transition:background .2s;}}
  .zoom-btn:hover{{background:rgba(0,0,0,0.7);}}
  .zoom-btn svg{{width:12px;height:12px;flex-shrink:0;}}

  .tick{{position:absolute;bottom:6px;left:6px;width:22px;height:22px;border-radius:50%;background:rgba(0,0,0,.18);display:flex;align-items:center;justify-content:center;color:transparent;font-size:13px;font-weight:900;pointer-events:none;z-index:10;}}
  .card.selected .tick{{background:{O};color:#fff;}}

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

  /* 🧠 Highlights & Trust Badges */
  .hlt {{ background: #fee2e2; color: #b91c1c; font-weight: 800; border-radius: 2px; padding: 0 2px; }}
  .trust-badge {{
    position: absolute; top: 10px; left: 10px;
    background: #ef4444; color: #fff; font-size: 10px; font-weight: 800;
    padding: 4px 8px; border-radius: 6px; z-index: 100;
    box-shadow: 0 4px 12px rgba(239, 68, 68, 0.4);
    cursor: pointer; transition: transform 0.2s;
  }}
  .trust-badge:hover {{ transform: scale(1.1); background: #dc2626; }}
  
  /* Per-card undo shimmer — only the card being processed gets this */
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

  /* Floating Tooltip */
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
  .bottom-bar {{
    top: auto;
    bottom: 0;
    border-bottom: none;
    border-top: 1px solid rgba(246, 139, 30, 0.2);
  }}

  /* 🚀 Floating Bulk Action Bar */
  #floating-action-bar {{
    position: fixed;
    bottom: 40px;
    left: 50%;
    transform: translateX(-50%) translateY(100px);
    background: rgba(16, 20, 26, 0.95);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    padding: 24px 48px;
    border-radius: 80px;
    display: flex;
    align-items: center;
    gap: 32px;
    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
    border: 1px solid rgba(255,255,255,0.15);
    z-index: 99999;
    transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
    opacity: 0;
    pointer-events: none;
  }}
  #floating-action-bar.visible {{
    transform: translateX(-50%) translateY(0);
    opacity: 1;
    pointer-events: auto;
  }}
  .fab-count {{ color: {O}; font-weight: 800; font-size: 18px; border-right: 1px solid rgba(255,255,255,0.2); padding-right: 25px; }}

  /* 🚀 Skeleton Shimmer */
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

  /* Custom reason inline panel */
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
</style>
</head>
<body>

<div id="custom-reason-panel">
  <h4>Enter custom rejection reason</h4>
  <input id="custom-reason-input" type="text" placeholder="Type your reason here…" maxlength="200">
  <div class="custom-panel-btns">
    <button class="custom-panel-confirm" onclick="confirmCustomReason()">Apply</button>
    <button class="custom-panel-cancel" onclick="cancelCustomReason()">Cancel</button>
  </div>
</div>

<div class="ctrl-bar">
  <input id="grid-search" type="search" placeholder="Search by name, brand, SID or category.">
  <div id="grid-count" style="font-size:11px; color:var(--text); opacity:0.7; margin-right:10px;">{len(page_data)} products</div>
  <button id="dark-toggle" onclick="toggleDark()">Dark</button>
  <span class="sel-count-text" style="font-weight:700; color:var(--accent); font-size:13px;">0 {labels_dict["items_pending"]}</span>
  <select class="reason-sel" id="batch-reason-top">
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
    <option value="OTHER_CUSTOM">Other Reason (Custom)</option>
  </select>
  <button class="batch-btn" onclick="doBatchReject('top')">{labels_dict["batch_reject"]}</button>
  <button class="desel-btn" onclick="doBatchUndo()">{labels_dict["undo"]}</button>
  <button class="desel-btn" onclick="window.doSelectAll()">{labels_dict["select_all"]}</button>
  <button class="desel-btn" onclick="doDeselAll()">{labels_dict["deselect_all"]}</button>
  <button class="batch-btn top-btn" onclick="window.scrollTo(0, document.body.scrollHeight)">{_t("go_bottom")}</button>
</div>

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
  <select class="reason-sel sort-sel" id="sort-sel-top" onchange="applySort(this.value)" style="max-width:170px;" title="Sort by image issue">
    <option value="">Sort by issue</option>
    <option value="low_res">Low Resolution</option>
    <option value="tall">Tall (Screenshot?)</option>
    <option value="wide">Wide Aspect</option>
    <option value="broken">Broken Image</option>
    <option disabled>── Prefetched ──</option>
    <option value="Wrong Category">Wrong Category</option>
    <option value="Restricted brands">Restricted brands</option>
    <option value="Suspected Fake product">Suspected Fake</option>
    <option value="Missing COLOR">Missing Color</option>
    <option value="Product Warranty">Warranty Issues</option>
    <option value="Duplicate product">Duplicates</option>
    <option value="no_issue">No Issues First</option>
  </select>
</div>

<div class="grid" id="card-grid"></div>

<div class="ctrl-bar bottom-bar">
  <span class="sel-count-text" style="font-weight:700; color:var(--accent); font-size:13px;">0 {labels_dict["items_pending"]}</span>
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
    <option value="OTHER_CUSTOM">Other Reason (Custom)</option>
  </select>
  <button class="batch-btn" onclick="doBatchReject('bottom')">{labels_dict["batch_reject"]}</button>
  <button class="desel-btn" onclick="doBatchUndo()">{labels_dict["undo"]}</button>
  <button class="desel-btn" onclick="window.doSelectAll()">{labels_dict["select_all"]}</button>
  <button class="desel-btn" onclick="doDeselAll()">{labels_dict["deselect_all"]}</button>
  <button class="desel-btn top-btn" onclick="window.scrollTo(0, 0)">{labels_dict["undo"]}</button>
</div>

<div id="zoom-tooltip">
  <img id="tooltip-img" alt="Zoomed product" referrerpolicy="no-referrer">
  <button class="tooltip-close" onclick="closeZoom()" title="Close">×</button>
</div>

<div id="prefetch-status"></div>

<div id="floating-action-bar">
  <div class="fab-count" id="fab-count-txt">0 {labels_dict["items_pending"].upper()}</div>
  <button class="batch-btn" onclick="window.batchApprove()" style="border-radius:24px; padding:10px 24px; background:#16a34a; font-size:15px; font-weight:600;">Approve All</button>
  <button class="batch-btn" onclick="doBatchReject('bottom')" style="border-radius:24px; padding:10px 24px; font-size:15px; font-weight:600;">Reject All</button>
  <button class="desel-btn" onclick="doBatchUndo()" style="border-radius:24px; padding:10px 24px; color:#fff; background:#4b4b4b; border:1px solid #777; font-size:15px; font-weight:600;">{labels_dict["undo"]}</button>
  <button class="desel-btn" onclick="doDeselAll()" style="border-radius:24px; padding:10px 24px; color:#fff; background:#e73c17; border:1px solid #e73c17; font-size:15px; font-weight:600;">{labels_dict["clear_sel"]}</button>
</div>

<script>
// ── Pin this iframe so Streamlit's rerun can't blank it ──────────────────────
(function pinIframe() {{
  try {{
    var par = window.parent;
    var STYLE_ID = '__cuf_iframe_pin__';
    if (!par.document.getElementById(STYLE_ID)) {{
      var s = par.document.createElement('style');
      s.id = STYLE_ID;
      s.textContent = [
        'iframe[title="st.iframe"], iframe[title="streamlit.components.v1.html"] {{',
        '  visibility: visible !important;',
        '  opacity: 1 !important;',
        '  transition: opacity 0.2s ease-in-out;',
        '}}'
      ].join('\\n');
      par.document.head.appendChild(s);
    }}
    var OBS_KEY = '__cuf_obs__';
    if (!par.window[OBS_KEY]) {{
      var obs = new par.MutationObserver(function(mutations) {{
        mutations.forEach(function(m) {{
          if (m.type !== 'attributes' || m.attributeName !== 'style') return;
          var el = m.target;
          if (el.tagName !== 'IFRAME') return;
          if (el.style.visibility === 'hidden') {{
            el.style.setProperty('visibility', 'visible', 'important');
            el.style.setProperty('opacity', '1', 'important');
          }}
        }});
      }});
      obs.observe(par.document.body, {{
        subtree: true, attributes: true, attributeFilter: ['style']
      }});
      par.window[OBS_KEY] = obs;
    }}
  }} catch(e) {{ /* cross-origin guard */ }}
}})();

// INSTANT CLOSE DIALOG LOCK
try {{
  var par = window.parent.document;
  if (!par.window.__stModalLocked) {{
    par.window.__stModalLocked = true;
    function blockOutsideClicks(e) {{
      var dialog = par.querySelector('[data-testid="stDialog"]');
      if (dialog && !dialog.contains(e.target)) {{
        e.stopPropagation();
        e.preventDefault();
      }}
    }}
    par.addEventListener('mousedown', blockOutsideClicks, true);
    par.addEventListener('mouseup', blockOutsideClicks, true);
    par.addEventListener('click', blockOutsideClicks, true);
  }}
}} catch(e) {{ console.error("Could not lock dialog", e); }}

function escapeHtml(u){{return(u||"").toString().replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#039;");}}
var CARDS = {cards_json};
var COMMITTED = {committed_json};
var POOR_IMG_SIDS = new Set({poor_img_sids_json});
var PREFETCH_URLS = {prefetch_json};
var PLACEHOLDER = "{_PLACEHOLDER_SVG}";
var LABELS = {labels_json};

window._gridSelected = window._gridSelected || {{}};
window._stagedRejections = window._stagedRejections || {{}};
window.currentZoomSid = null;
window._imageIssues = window._imageIssues || {{}};
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

function showGhostOverlay(msgText) {{
  var ghost = document.createElement('div');
  ghost.id = '__grid_ghost__';
  ghost.style.cssText = 'position:fixed;z-index:99999;inset:0;background:rgba(255,255,255,0.85);display:flex;align-items:center;justify-content:center;font-family:sans-serif;color:#FF8800;transition:opacity 0.4s ease;';
  ghost.innerHTML = '<div style="font-size:22px;font-weight:bold;">' + msgText + '</div>';
  var existing = document.getElementById('__grid_ghost__');
  if (existing) existing.remove();
  document.body.appendChild(ghost);
  setTimeout(function() {{
    var g = document.getElementById('__grid_ghost__');
    if (g) {{ g.style.opacity = '0'; setTimeout(function() {{ if(g && g.parentNode) g.remove(); }}, 400); }}
  }}, 4000);
}}

function sendMsg(type, payload) {{
  try {{
    var par = window.parent;
    var inputs = par.document.querySelectorAll('input[type="text"]');
    var bridge = null;
    for (var i = 0; i < inputs.length; i++) {{
      if (inputs[i].getAttribute('aria-label') === 'jtbridge' || inputs[i].placeholder === 'JTBRIDGE_UNIQUE_DO_NOT_USE') {{
        bridge = inputs[i]; break;
      }}
    }}
    if (!bridge) return;
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
  window.scrollTo({{top: 0, behavior: 'smooth'}});
}}

function updateParentPagination() {{
  var pending = Object.keys(selected).length + Object.keys(staged).length;
  try {{
    var par = window.parent.document;
    var buttons = par.querySelectorAll('button');
    buttons.forEach(b => {{
      var txt = b.innerText || "";
      if (txt.includes('Close') && !b.dataset.fastCloseBound) {{
        b.dataset.fastCloseBound = "true";
        b.addEventListener('click', function() {{
          var modalContainer = par.querySelector('div[data-testid="stModal"]');
          if (modalContainer) {{
            modalContainer.style.transition = 'opacity 0.15s ease-out';
            modalContainer.style.opacity = '0';
            setTimeout(() => modalContainer.style.display = 'none', 150);
          }}
        }});
      }}
      if (txt.includes('Prev Page') || txt.includes('Next Page') || txt.includes('Close')) {{
        if (pending > 0 && !txt.includes('Close')) {{
          b.style.pointerEvents = 'none';
          b.style.opacity = '0.3';
          b.title = "Confirm or clear your selections before navigating.";
        }} else {{
          b.style.pointerEvents = 'auto';
          b.style.opacity = '1';
          b.title = "";
        }}
      }}
    }});
    var inputs = par.querySelectorAll('input[type="number"]');
    inputs.forEach(inp => {{
      var wrapper = inp.closest('div[data-testid="stNumberInput"]');
      if (wrapper && wrapper.innerText.includes('Jump to Page')) {{
        if (pending > 0) {{
          wrapper.style.pointerEvents = 'none';
          wrapper.style.opacity = '0.3';
          wrapper.title = "Confirm or clear your selections before navigating.";
        }} else {{
          wrapper.style.pointerEvents = 'auto';
          wrapper.style.opacity = '1';
          wrapper.title = "";
        }}
      }}
    }});
  }} catch(e) {{}}
}}

function onImgLoad(img, sid) {{
  img.classList.remove('skeleton');
  img.classList.add('img-loaded');
  var wrap = img.closest('.card-img-wrap');
  if (wrap) wrap.classList.add('img-loaded');
  var w = img.naturalWidth, h = img.naturalHeight;
  var warns = [];
  if (w > 0 && h > 0) {{
    if (w < 200 || h < 200) warns.push('Low Resolution');
    var ratio = h / w;
    if (ratio > 1.5) warns.push('Tall (Screenshot?)');
    else if (ratio < 0.6) warns.push('Wide Aspect');
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
  }}, {{rootMargin: '200px 0px', threshold: 0.01}});
  return _lazyObserver;
}}

function activateLazyImages() {{
  var observer = getLazyObserver();
  if (!observer) return;
  document.querySelectorAll('img.card-img[data-lazy-src]').forEach(function(img) {{
    observer.observe(img);
  }});
}}

function onImgError(img, sid) {{
  var card = CARDS.find(c => c.sid === sid);
  var realSrc = img.dataset.lazySrc || (card ? card.img : '');
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

function buildCardActionsHtml(safeSid) {{
  var opts = [
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
    ['OTHER_CUSTOM',         'Other Reason (Custom)'],
  ];
  var optionsHtml = opts.map(function(o) {{
    return `<option value="${{o[0]}}">${{o[1]}}</option>`;
  }}).join('');
  return (
    `<div class="acts">` +
      `<button class="act-btn" onclick="event.stopPropagation();window.stageReject('${{safeSid}}','REJECT_POOR_IMAGE')">${{escapeHtml(LABELS.poor_img)}}</button>` +
      `<select class="act-more" onchange="if(this.value){{event.stopPropagation();window.stageReject('${{safeSid}}',this.value);this.value=''}}">` +
        `<option value="">${{escapeHtml(LABELS.more_options)}}</option>` +
        optionsHtml +
      `</select>` +
    `</div>`
  );
}}

// ── Smart Features Utility ──
var UNNECESSARY_WORDS = {json.dumps(support_files.get("unnecessary_words", []))};
var PROHIBITED_WORDS = {json.dumps(support_files.get("prohibited_words", []))};
var SELLER_TRUST = {json.dumps(seller_trust)};

function getHighlightedName(card) {{
  var name = card.name;
  var warns = card.warnings || [];
  var words = [];
  if (warns.includes("Unnecessary words")) words = words.concat(UNNECESSARY_WORDS);
  if (warns.includes("Prohibited Words")) words = words.concat(PROHIBITED_WORDS);
  
  // Also highlight specific trigger flags
  if (warns.includes("BRAND name repeated in NAME")) words.push(card.brand);
  
  if (words.length === 0) return card.name.length > 38 ? escapeHtml(card.name.slice(0,38)) + '\u2026' : escapeHtml(card.name);
  
  // Sort by length descending to avoid partial matches
  words.sort((a,b) => b.length - a.length);
  var regex = new RegExp('(' + words.map(w => w.replace(/[.*+?^${{}}()|[\\]\\]/g, '\\\\$&')).join('|') + ')', 'gi');
  var hName = name.replace(regex, '<span class="hlt">$1</span>');
  
  // Truncate if still too long (preserving HTML tags is tricky, so we limit characters but skip tags)
  return hName;
}}

window.rejectAllFromSeller = function(seller) {{
  var sids = CARDS.filter(c => c.seller === seller).map(c => c.sid);
  sids.forEach(sid => {{
    if (!(sid in staged)) {{
      if (sid in selected) delete selected[sid];
      staged[sid] = "Bulk Seller Reject (High Risk)";
      replaceCard(sid);
    }}
  }});
  updateSelCount();
}};

function renderCard(card) {{
  var sid = card.sid;
  var safeSid = sid.replace(/'/g, "\\\\'");
  var isCommitted = sid in COMMITTED;
  var isStaged = sid in staged;
  var isSelected = sid in selected;
  var isPoorImgRej = isCommitted && POOR_IMG_SIDS.has(sid);
  var isBrandImgRej = isCommitted && (String(COMMITTED[sid]).includes('Brand Image Check'));
  var cls = 'card' + (isCommitted ? ' committed-rej' + (isPoorImgRej ? ' poor-img-rej' : '') + (isBrandImgRej ? ' brand-image-rej' : '') : isStaged ? ' staged-rej' : '') + (isSelected ? ' selected' : '');

  var safeImgSrcForHtml = card.img ? card.img.replace(/'/g, "%27").replace(/"/g, "%22") : PLACEHOLDER;
  var shortName = card.name.length > 38 ? escapeHtml(card.name.slice(0,38)) + '\u2026' : escapeHtml(card.name);
  var warnHtml = (card.warnings || []).map(w => `<span class="warn-badge">${{escapeHtml(w)}}</span>`).join('');
  var priceHtml = card.price ? `<div class="price-badge">${{escapeHtml(card.price)}}</div>` : '';
  var colorHtml = card.color ? `<div class="co" title="Color: ${{escapeHtml(card.color)}}">Color: ${{escapeHtml(card.color)}}</div>` : '';
  var brandDetectedHtml = (isBrandImgRej && card.brand_detected) ? `<div class="co" style="background:#E8F5E9;color:#2E7D32;border:1px solid #C8E6C9;" title="Brand Detected: ${{escapeHtml(card.brand_detected)}}">Detected Brand: ${{escapeHtml(card.brand_detected)}}</div>` : '';

  var zoomHtml = `<button class="zoom-btn" onclick="event.stopPropagation();showZoom('${{safeSid}}', event)" title="Preview">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
      <line x1="11" y1="8" x2="11" y2="14"/><line x1="8" y1="11" x2="14" y2="11"/>
    </svg></button>`;

  var imgIdx = CARDS.indexOf(card);
  var isEager = imgIdx < {cols_per_row * 2};
  var loadingAttr = isEager ? 'eager' : 'lazy';
  var priorityAttr = isEager ? 'fetchpriority="high"' : 'fetchpriority="low"';
  var imgSrcAttr = isEager
    ? `src="${{safeImgSrcForHtml}}"`
    : `src="${{PLACEHOLDER}}" data-lazy-src="${{safeImgSrcForHtml}}"`;

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
    actHtml = buildCardActionsHtml(safeSid);
  }}

    var trustBadge = '';
    var score = SELLER_TRUST[card.seller] || 0;
    if (score > 80) {{
      trustBadge = `<div class="trust-badge" onclick="event.stopPropagation();window.rejectAllFromSeller('${{card.seller.replace(/'/g,"\\\\'")}}')" title="Seller has ${{score}}% rejection rate. Click to reject all from this seller.">High Risk Seller</div>`;
    }}

  var dataAttrs = 'data-sid="' + escapeHtml(String(card.data_sid||'')) + '" data-name="' + escapeHtml(String(card.data_name||'')) + '" data-brand="' + escapeHtml(String(card.data_brand||'')) + '" data-cat="' + escapeHtml(String(card.data_cat||'')) + '"';
  return `<div class="${{cls}}" id="card-${{escapeHtml(sid)}}" ${{dataAttrs}} tabindex="0" onclick="window.toggleSelect('${{safeSid}}',event)">
    <div class="card-img-wrap">
      ${{trustBadge}}
      ${{priceHtml}}
      <div class="warn-wrap">${{warnHtml}}</div>
      <div id="debug-${{escapeHtml(sid)}}" class="debug-hud"></div>
      <img class="card-img-placeholder" src="${{PLACEHOLDER}}" alt="">
      <img class="card-img" ${{imgSrcAttr}} decoding="async" loading="${{loadingAttr}}" ${{priorityAttr}} referrerpolicy="no-referrer"
            onload="onImgLoad(this,'${{safeSid}}')" onerror="onImgError(this,'${{safeSid}}')">
      ${{zoomHtml}}
      ${{overlayHtml}}
      <div class="tick">\u2714</div>
    </div>
    <div class="meta">
      <div class="nm" title="${{escapeHtml(card.name)}}">${{getHighlightedName(card)}}</div>
      <div class="br" title="${{escapeHtml(card.brand)}}">Brand: ${{escapeHtml(card.brand)}}</div>
      <div class="ct" title="${{escapeHtml(card.cat)}}">Category: ${{escapeHtml(card.cat)}}</div>
      <div class="sl" title="${{escapeHtml(card.seller)}}">Seller: ${{escapeHtml(card.seller)}}</div>
      ${{colorHtml}}
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
  img.src = card.img || PLACEHOLDER;
  img.onerror = function() {{ img.src = PLACEHOLDER; img.onerror = null; }};
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
  window.currentZoomSid = null;
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

  var fab = document.getElementById('floating-action-bar');
  if (fab) {{
    if (pendingCount > 0) fab.classList.add('visible');
    else fab.classList.remove('visible');
    var fabTxt = document.getElementById('fab-count-txt');
    if (fabTxt) fabTxt.textContent = pendingCount + ' ' + LABELS.items_pending.toUpperCase();
  }}
  updateParentPagination();
}}

function getSortedCards() {{
  var sort = window._currentSort;
  if (!sort) return CARDS;
  var ISSUE_MAP = {{ 'low_res':'Low Resolution','tall':'Tall (Screenshot?)','wide':'Wide Aspect','broken':'Broken Image' }};
  var sorted = CARDS.slice();
  if (sort === 'no_issue') {{
    sorted.sort(function(a,b) {{ return ((window._imageIssues[a.sid]||[]).length>0?1:0) - ((window._imageIssues[b.sid]||[]).length>0?1:0); }});
  }} else {{
    var target = ISSUE_MAP[sort] || sort;
    sorted.sort(function(a,b) {{ return ((window._imageIssues[a.sid]||[]).includes(target)?0:1) - ((window._imageIssues[b.sid]||[]).includes(target)?0:1); }});
  }}
  return sorted;
}}

window.applySort = function(val) {{
  window._currentSort = val;
  ['sort-sel-top','sort-sel-bottom'].forEach(function(id) {{ var el=document.getElementById(id); if(el) el.value=val; }});
  renderAll();
}};

function renderAll() {{ document.getElementById('card-grid').innerHTML = getSortedCards().map(renderCard).join(''); updateSelCount(); activateLazyImages(); }}

function replaceCard(sid) {{
  var el = document.getElementById('card-' + escapeHtml(sid));
  if (!el) return;
  var card = CARDS.find(c => c.sid === sid);
  if (card) {{ var t = document.createElement('div'); t.innerHTML = renderCard(card); el.replaceWith(t.firstElementChild); activateLazyImages(); }}
}}

window.doSelectAll = function() {{
  CARDS.forEach(c => {{ if (!(c.sid in staged)) selected[c.sid] = true; }});
  renderAll();
  updateSelCount();
}};

window.toggleSelect = function(sid, e) {{
  if (sid in staged) delete staged[sid];
  else if (sid in selected) delete selected[sid];
  else selected[sid] = true;
  replaceCard(sid); updateSelCount();
}};

window.stageReject = function(sid, r) {{
  var currentCard = CARDS.find(c => c.sid === sid);
  var toStage = [sid];

  // Intelligent Similar Image Detection
  if (currentCard && (r === 'REJECT_POOR_IMAGE' || r.startsWith('REJECT_IMG_'))) {{
      CARDS.forEach(c => {{
          if (c.sid !== sid && (c.img === currentCard.img || (c.hash && c.hash === currentCard.hash))) {{
              toStage.push(c.sid);
          }}
      }});
  }}

  // 🧠 Smart Feature: Linguistic Similarity Pre-flagging for Wrong Category
  if (r === 'REJECT_WRONG_CAT' && currentCard) {{
      var nameTokens = currentCard.name.toLowerCase().split(/[^\w]+/).filter(w => w.length > 4);
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

  // 1. Target exactly what to change so the image is NEVER re-drawn or flashed
  var safeSid = sid.replace(/'/g, "\\\\'");
  var cardEl = document.getElementById('card-' + escapeHtml(sid));

  if (cardEl) {{
      cardEl.classList.remove('committed-rej', 'poor-img-rej');

      var overlay = cardEl.querySelector('.rej-overlay');
      if (overlay) overlay.remove();

      var acts = cardEl.querySelector('.acts');
      if (acts) acts.remove();
      cardEl.insertAdjacentHTML('beforeend', buildCardActionsHtml(safeSid));

      // Add a slight shimmer to the card without blocking interaction
      cardEl.classList.add('undo-processing');
  }}

  updateSelCount();

  // 2. Pin iframe AND wrapper height in parent to absolutely stop scroll jumpiness
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

  // 3. Ultra-fast debounce: 400ms. Still allows multiple swift clicks to process together.
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
        }}, 1000); // Shorter cleanup window
      }});
    }});
  }}, 400);
}};

window.doBatchReject = function(pos) {{
  var selectId = pos === 'top' ? 'batch-reason-top' : 'batch-reason-bottom';
  var sel = document.getElementById(selectId);
  var br = sel.value;
  if (br === 'OTHER_CUSTOM') {{
    showCustomReasonPanel(function(cmt) {{
      if (!cmt) {{ sel.value = "REJECT_POOR_IMAGE"; return; }}
      _applyBatchReject("Other Reason (Custom): " + cmt);
      sel.value = "REJECT_POOR_IMAGE";
    }});
    return;
  }}
  _applyBatchReject(br);
}};

function _applyBatchReject(br) {{
  var payload = {{}}, count = 0;
  for (var s in staged) {{ payload[s] = staged[s]; count++; }}
  for (var s in selected) {{
    if (!(s in COMMITTED)) {{ payload[s] = br; count++; }}
  }}
  if (count === 0) {{
    // Even if no new rejections, clear the current selection to avoid "phantom" pending counts
    for (var s in selected) delete selected[s];
    for (var s in staged) delete staged[s];
    updateSelCount();
    return;
  }}
  // Clear EVERYTHING that was selected or staged
  var allSids = Object.assign({{}}, selected, staged);
  for (var s in payload) {{ COMMITTED[s] = payload[s]; }}
  for (var s in allSids) {{ delete selected[s]; delete staged[s]; }}
  showGhostOverlay('Applying rejections...');
  renderAll();
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

  // No Ghost Overlay here either, keeping batch undo fluid
  renderAll();
  updateSelCount();
  sendMsg('undo', payload);
}};

window.doDeselAll = function() {{ for (var k in selected) delete selected[k]; for (var k in staged) delete staged[k]; renderAll(); updateSelCount(); }};

(function() {{
  if (!PREFETCH_URLS || !PREFETCH_URLS.length) return;
  var statusEl = document.getElementById('prefetch-status');
  var POOL_SIZE = 8;
  var pool = [];
  for (var p = 0; p < POOL_SIZE; p++) {{
    var pi = new Image();
    pi.referrerPolicy = "no-referrer";
    pi.style.cssText = 'width:1px;height:1px;opacity:0;position:absolute;pointer-events:none;';
    document.body.appendChild(pi);
    pool.push(pi);
  }}
  var i = 0, done = 0, total = PREFETCH_URLS.length, slot = 0;
  var runner = window.requestIdleCallback || function(fn){{setTimeout(fn,300);}};
  function prefetchBatch() {{
    var limit = POOL_SIZE, processed = 0;
    while (i < total && processed < limit) {{
      var url = PREFETCH_URLS[i++]; processed++;
      var img = pool[slot % POOL_SIZE]; slot++;
      img.onload = (function(u) {{ return function() {{
        done++;
        if (statusEl) statusEl.textContent = 'Prefetched ' + done + '/' + total;
      }}; }})(url);
      img.onerror = img.onload;
      img.src = url;
    }}
    if (i < total) runner(prefetchBatch);
  }}
  setTimeout(prefetchBatch, 800);
}})();

window.addEventListener("scroll", function() {{
  sessionStorage.setItem("__inner_iframe_scroll__", window.scrollY);
}});

{scroll_js}

// ── Keyboard shortcuts ────────────────────────────────────────────────────
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
  document.querySelectorAll('.card').forEach(function(c) {{ c.style.outline = ''; }});
  var el = document.getElementById('card-' + escapeHtml(_focusedSid));
  if (el) {{
    el.style.outline = '3px solid #2196F3';
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

['batch-reason-top','batch-reason-bottom'].forEach(function(id) {{
  var el = document.getElementById(id);
  if (el) el.addEventListener('change', function() {{ _lastReason = this.value; }});
}});

// 🚀 Live Search
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

// 🚀 Dark Mode
var _dark = false;
try {{ _dark = localStorage.getItem('gridDark') === '1'; }} catch(e) {{}}
window.applyDark = function(on) {{
  document.documentElement.style.setProperty('--bg',    on ? '#18181b' : '#f9fafb');
  document.documentElement.style.setProperty('--card',  on ? '#27272a' : '#ffffff');
  document.documentElement.style.setProperty('--text',  on ? '#f4f4f5' : '#111827');
  document.documentElement.style.setProperty('--border',on ? '#3f3f46' : '#e5e7eb');
  var dtEl = document.getElementById('dark-toggle');
  if (dtEl) dtEl.textContent = on ? 'Light' : 'Dark';
  try {{ localStorage.setItem('gridDark', on ? '1' : '0'); }} catch(e) {{}}
}}
window.toggleDark = function() {{ _dark = !_dark; applyDark(_dark); }}
try {{ applyDark(_dark); }} catch(e) {{}}

// 🚀 Keyboard Navigation
var _focused = -1;
var _cards = [];
document.addEventListener('keydown', function(e) {{
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  
  _cards = Array.from(document.querySelectorAll('.card:not([style*="none"])'));
  
  if (e.key === 'j' || e.key === 'ArrowRight') {{
    e.preventDefault();
    _focused = Math.min(_focused + 1, _cards.length - 1);
    _cards[_focused]?.scrollIntoView({{block:'center', behavior:'smooth'}});
    _cards[_focused]?.focus();
  }} else if (e.key === 'k' || e.key === 'ArrowLeft') {{
    e.preventDefault();
    _focused = Math.max(_focused - 1, 0);
    _cards[_focused]?.scrollIntoView({{block:'center', behavior:'smooth'}});
    _cards[_focused]?.focus();
  }} else if (e.key === 'a' && _focused >= 0) {{
    var sid = _cards[_focused].id.replace('card-', '');
    window.batchApproveSingle(sid);
  }} else if (e.key === 'r' && _focused >= 0) {{
    var sid = _cards[_focused].id.replace('card-', '');
    if (!(sid in selected)) window.toggleSelect(sid);
    document.getElementById('batch-reason-top').focus();
  }} else if (e.key === '?') {{
    var h = document.getElementById('shortcut-help');
    h.style.display = h.style.display === 'flex' ? 'none' : 'flex';
  }}
}});

window.batchApproveSingle = function(sid) {{
  window.parent.postMessage({{type:'staged_reject', sid:sid, reason:'Approved'}}, '*');
}}

window.batchApprove = function() {{
  var sids = Object.keys(selected);
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
</body>
</html>"""


@st.dialog(
    "Visual Review Mode", width="large", icon=":material/pageview:", dismissible=False
)
def visual_review_modal(support_files):

    scroll_top_flag = st.session_state.get("do_scroll_top", False)
    st.session_state.do_scroll_top = False

    fr = st.session_state.final_report
    data = st.session_state.all_data_map
    all_rows = st.session_state.get("all_data_rows", data)
    committed_rej_sids = {
        k.replace("quick_rej_", "")
        for k in st.session_state.keys()
        if k.startswith("quick_rej_") and "reason" not in k
    }

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

    c1, c2, c3, c4 = st.columns(
        [1.5, 1.5, 1.5, 0.8], gap="large", vertical_alignment="bottom"
    )
    with c1:
        search_n = st.text_input(
            "Search by Name", placeholder="Product name…", icon=":material/search:"
        )
    with c2:
        search_sc = st.text_input(
            "Search by Seller/Category",
            placeholder="Seller or Category…",
            icon=":material/store:",
        )
    with c3:
        st.session_state.grid_items_per_page = st.select_slider(
            "Items per page",
            options=[20, 50, 100, 200],
            value=st.session_state.get("grid_items_per_page", 50),
        )
    with c4:
        if st.button("Close", use_container_width=True, type="secondary"):
            st.session_state.show_review_modal = False
            st.rerun()

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
            review_data["CATEGORY"] = review_data["CATEGORY_CODE"].apply(
                lambda c: (
                    _code_to_path.get(str(c).strip(), str(c)) if pd.notna(c) else ""
                )
            )

    if search_n:
        review_data = review_data[
            review_data["NAME"].astype(str).str.contains(search_n, case=False, na=False)
        ]
    if search_sc:
        mc = (
            review_data["CATEGORY"]
            .astype(str)
            .str.contains(search_sc, case=False, na=False)
            if "CATEGORY" in review_data.columns
            else pd.Series(False, index=review_data.index)
        )
        ms = (
            review_data["SELLER_NAME"]
            .astype(str)
            .str.contains(search_sc, case=False, na=False)
        )
        review_data = review_data[mc | ms]

    # ========== GROUP BY SELLER ==========
    review_data = review_data.sort_values(
        by=["SELLER_NAME", "NAME"], na_position="last"
    ).reset_index(drop=True)
    # =====================================

    ipp = st.session_state.get("grid_items_per_page", 50)
    total_pages = max(1, (len(review_data) + ipp - 1) // ipp)
    if st.session_state.get("grid_page", 0) >= total_pages:
        st.session_state.grid_page = 0

    pg_cols = st.columns([1, 2, 1], vertical_alignment="center", gap="small")
    with pg_cols[0]:
        if st.button(
            "Prev Page",
            key="prev_top",
            icon=":material/arrow_back:",
            icon_position="left",
            use_container_width=True,
            disabled=st.session_state.get("grid_page", 0) == 0,
        ):
            st.session_state.grid_page = max(
                0, st.session_state.get("grid_page", 0) - 1
            )
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")
    with pg_cols[1]:
        new_page = st.number_input(
            f"Jump to Page (Total: {total_pages} | {len(review_data)} items)",
            min_value=1,
            max_value=max(1, total_pages),
            value=st.session_state.grid_page + 1,
            step=1,
            key="jump_top",
        )
        if new_page - 1 != st.session_state.grid_page:
            st.session_state.grid_page = new_page - 1
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")
    with pg_cols[2]:
        if st.button(
            "Next Page",
            key="next_top",
            icon=":material/arrow_forward:",
            icon_position="right",
            use_container_width=True,
            disabled=st.session_state.grid_page >= total_pages - 1,
        ):
            st.session_state.grid_page += 1
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")

    page_start = st.session_state.grid_page * ipp
    page_data = review_data.iloc[page_start : page_start + ipp]

    _poor_img_comments = (
        fr[fr["FLAG"].isin(["Image Stretched", "Image Blurry", "Poor images"])]
        .set_index("ProductSetSid")["Comment"]
        .to_dict()
    )
    page_warnings = {}
    for _sid in page_data["PRODUCT_SET_SID"].astype(str):
        _comment = _poor_img_comments.get(_sid, "")
        _warns = []
        if _comment:
            _cl = _comment.lower()
            if "stretched" in _cl or "tall" in _cl:
                _warns.append("Tall (Screenshot?)")
            if "stretched" in _cl or "wide" in _cl:
                _warns.append("Wide Aspect")
            if (
                "blurry" in _cl
                or "low res" in _cl
                or "resolution" in _cl
                or "small" in _cl
            ):
                _warns.append("Low Resolution")

        # 🚀 ADD ALL FLAGS FROM FINAL REPORT AS WARNINGS
        _row_fr = fr[fr["ProductSetSid"].astype(str) == _sid]
        if not _row_fr.empty:
            _flag = _row_fr.iloc[0]["FLAG"]
            if _flag and _flag not in ("Approved", "Manual review"):
                _warns.append(_flag)
            elif _flag == "Manual review":
                _warns.append("Manual review")

        if _warns:
            page_warnings[_sid] = list(dict.fromkeys(_warns)) # Remove duplicates

    # 🧠 Calculate Seller Trust Scoring
    seller_trust = {}
    if not fr.empty and "SELLER_NAME" in fr.columns:
        _stats = fr.groupby("SELLER_NAME")["Status"].value_counts(normalize=True).unstack().fillna(0)
        if "Rejected" in _stats.columns:
            seller_trust = (_stats["Rejected"] * 100).round(1).to_dict()

    _prefetch_cache_key = f"prefetch_{st.session_state.grid_page}_{len(review_data)}"
    if _prefetch_cache_key not in st.session_state:
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
        st.session_state[_prefetch_cache_key] = prefetch_urls
    else:
        prefetch_urls = st.session_state[_prefetch_cache_key]

    rejected_state = {
        sid: st.session_state[f"quick_rej_reason_{sid}"]
        for sid in page_data["PRODUCT_SET_SID"].astype(str)
        if st.session_state.get(f"quick_rej_{sid}")
    }

    # 🚀 Build rejected_state for JS with stripped SIDs
    for _sid_raw in page_data.get(
        "PRODUCT_SET_SID", page_data.get("ProductSetSid", pd.Series())
    ).astype(str):
        _sid = _sid_raw.strip()
        if _sid in poor_img_rej_sids and _sid not in rejected_state:
            _row_fr = fr[fr["ProductSetSid"].astype(str).str.strip() == _sid]
            if not _row_fr.empty:
                _flag = str(_row_fr.iloc[0]["FLAG"])
                _comment = str(_row_fr.iloc[0]["Comment"])
                if "Brand Image Check" in _flag or "Brand Image Check" in _comment:
                    rejected_state[_sid] = "Brand Image Check"
                else:
                    rejected_state[_sid] = "Poor images"
            else:
                rejected_state[_sid] = "Poor images"

    cols_per_row = 5
    skeleton_html = (
        """
<style>
  .sk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px}
  .sk-card{border-radius:12px;overflow:hidden;background:#f3f4f6;height:260px;
           animation:pulse 1.4s ease-in-out infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
</style>
<div class="sk-grid">
"""
        + "".join(['<div class="sk-card"></div>'] * 12)
        + "</div>"
    )

    placeholder = st.empty()
    placeholder.html(skeleton_html)

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
        seller_trust=seller_trust,
        support_files=support_files,
    )

    placeholder.empty()
    st.iframe(grid_html, height=750)

    st.markdown("---")

    pg_cols_bot = st.columns([1, 2, 1, 1], vertical_alignment="center", gap="small")
    with pg_cols_bot[0]:
        if st.button(
            "Prev Page",
            key="prev_bot",
            icon=":material/arrow_back:",
            icon_position="left",
            use_container_width=True,
            disabled=st.session_state.get("grid_page", 0) == 0,
        ):
            st.session_state.grid_page = max(
                0, st.session_state.get("grid_page", 0) - 1
            )
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")
    with pg_cols_bot[1]:
        new_page_bot = st.number_input(
            f"Jump to Page (Total: {total_pages} | {len(review_data)} items)",
            min_value=1,
            max_value=max(1, total_pages),
            value=st.session_state.grid_page + 1,
            step=1,
            key="jump_bot",
        )
        if new_page_bot - 1 != st.session_state.grid_page:
            st.session_state.grid_page = new_page_bot - 1
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")
    with pg_cols_bot[2]:
        if st.button(
            "Next Page",
            key="next_bot",
            icon=":material/arrow_forward:",
            icon_position="right",
            use_container_width=True,
            disabled=st.session_state.grid_page >= total_pages - 1,
        ):
            st.session_state.grid_page += 1
            st.session_state.do_scroll_top = True
            st.rerun(scope="fragment")
    with pg_cols_bot[3]:
        if st.button(
            "Close Review", key="close_bot", use_container_width=True, type="secondary"
        ):
            st.session_state.show_review_modal = False
            st.rerun()


@st.fragment
def render_image_grid(support_files):
    if (
        st.session_state.final_report.empty
        or st.session_state.get("file_mode") == "post_qc"
    ):
        return

    st.markdown("---")

    _warm_urls = st.session_state.get("_grid_warm_urls", [])
    if _warm_urls:
        _preload_tags = "\n".join(
            f'<link rel="preload" as="image" href="{url}" referrerpolicy="no-referrer">'
            for url in _warm_urls[:100]
        )
        st.markdown(
            f"<div style='display:none'>{_preload_tags}</div>", unsafe_allow_html=True
        )

    c1, c2 = st.columns([3, 1], gap="medium")
    with c1:
        st.header(_t("manual_review"), anchor=False)
        st.caption("Open Focus Mode to rapidly visually review and reject products.")
    with c2:
        if st.button("Start Visual Review", type="primary", use_container_width=True):
            st.session_state.show_review_modal = True

    if st.session_state.get("show_review_modal", False):
        visual_review_modal(support_files)


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
                use_container_width=True,
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
                use_container_width=True,
                type="primary",
                icon=":material/file_download:",
                key=f"dl_{title}",
            )
            if st.button("Clear", key=f"clr_{title}", use_container_width=True):
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

    # 🚀 Lazy Load All Data Rows if needed for exports
    if st.session_state.get("all_data_rows") is None:
        if "_data_filtered_ref" in st.session_state:
            # First try the memory reference (only exists for current session if not garbage collected)
            st.session_state.all_data_rows = st.session_state._data_filtered_ref
        elif "current_sig_hash" in st.session_state:
            # Fallback to loading from disk
            _fname = f"{st.session_state.current_sig_hash}_data_rows.parquet"
            st.session_state.all_data_rows = load_df_parquet(_fname)

    all_rows = st.session_state.get("all_data_rows", data)
    app_df = fr[fr["Status"] == "Approved"]
    rej_df = fr[fr["Status"] == "Rejected"]
    c_code = st.session_state.get("selected_country", "Kenya")[:2].upper()
    date_str = datetime.now().strftime("%Y-%m-%d")
    reasons_df = support_files.get("reasons", pd.DataFrame())

    st.markdown("---")
    st.header(_t("download_reports"), anchor=False)
    st.caption("Export QC results in Excel or ZIP format")

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
        if st.button("Generate All Reports", type="primary", use_container_width=True):
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
