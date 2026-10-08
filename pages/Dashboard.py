"""Import historical rejection reports into the learned image-rule catalog."""

import json
import hashlib
import os
import zipfile
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import r2_storage

from learned_rules import (
    LEARNABLE_FLAGS,
    delete_learned_image_rules,
    load_learned_image_rules,
    learn_image_rejections_bulk,
    normalize_learned_flag,
    restore_learned_image_rules,
    set_learned_image_rule_review,
    update_learned_image_rule_flag,
)

st.set_page_config(page_title="Dashboard", page_icon=":material/auto_awesome:", layout="wide")
st.title("Dashboard")
st.caption("Upload old rejection reports. Only rejected rows with an image and a supported flag are imported; duplicate URL/pHash/flag rules are skipped.")
_runtime_review_matches = st.session_state.get("_learned_review_matches", {})
if _runtime_review_matches:
    st.info(f":material/rate_review: {len(_runtime_review_matches):,} uncertain near-pHash match(es) were sent to review instead of automatic rejection. Open the validation review queue to inspect them.")

# ── Existing catalog ────────────────────────────────────────────────────────
# Dashboard widgets rerun this script on every keystroke, filter, and editor
# click. Parsing and enriching tens of thousands of JSON rules on every rerun
# made the admin page progressively heavier. Cache the complete derived frame
# by the JSON nanosecond mtime; edits from another tab still invalidate it.
_rules_path = Path(__file__).resolve().parents[1] / "learned_image_rules.json"
try:
    _rules_mtime_ns = _rules_path.stat().st_mtime_ns
except OSError:
    _rules_mtime_ns = 0


@st.cache_data(show_spinner=False, max_entries=4)
def _load_dashboard_catalog(mtime_ns: int, day_key: str) -> pd.DataFrame:
    del day_key  # cache key keeps the overdue lifecycle fresh each day
    _rules = load_learned_image_rules()
    frame = pd.DataFrame(_rules)
    if "brand_infringed" not in frame.columns:
        frame["brand_infringed"] = frame["brand"] if "brand" in frame.columns else ""

    if frame.empty:
        for _empty_col in ("review_state", "_cluster_key", "cluster_size", "sellers_affected", "confirmed_decisions", "review_due_at", "lifecycle", "confidence_score", "confidence"):
            frame[_empty_col] = pd.Series(dtype=str)
        return frame

    _review = frame.get("review_state", pd.Series("Unreviewed", index=frame.index)).fillna("Unreviewed").astype(str)
    _image = frame.get("image_url", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    _phash = frame.get("phash", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    _seller = frame.get("seller_name", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    frame["review_state"] = _review
    frame["_cluster_key"] = _image.mask(_image.eq(""), _phash)
    frame["cluster_size"] = frame.groupby("_cluster_key")["_cluster_key"].transform("size").astype(int)
    _seller_nonempty = _seller.replace("", pd.NA)
    frame["sellers_affected"] = _seller_nonempty.groupby(frame["_cluster_key"]).transform("nunique").fillna(0).astype(int)
    frame["confirmed_decisions"] = frame["review_state"].eq("Confirmed").groupby(frame["_cluster_key"]).transform("sum").astype(int)
    _created = pd.to_datetime(frame.get("created_at", ""), errors="coerce", utc=True)
    frame["review_due_at"] = pd.to_datetime(frame.get("review_due_at", _created + pd.Timedelta(days=30)), errors="coerce", utc=True)
    frame["lifecycle"] = "Active"
    frame.loc[frame["review_state"].eq("Corrected"), "lifecycle"] = "Correction queued"
    frame.loc[frame["review_due_at"].lt(pd.Timestamp.now(tz="UTC")) & frame["review_state"].eq("Unreviewed"), "lifecycle"] = "Review overdue"
    _confidence = pd.Series(0.55, index=frame.index)
    _confidence += _image.ne("").astype(float) * 0.15
    _confidence += _phash.ne("").astype(float) * 0.15
    _confidence += frame["cluster_size"].ge(3).astype(float) * 0.08
    _confidence += frame["sellers_affected"].ge(2).astype(float) * 0.07
    _confidence += frame["confirmed_decisions"].ge(1).astype(float) * 0.10
    _confidence -= frame["review_state"].eq("Corrected").astype(float) * 0.35
    frame["confidence_score"] = _confidence.clip(0, 1).round(2)
    frame["confidence"] = (frame["confidence_score"] * 100).round().astype(int).astype(str) + "%"
    return frame


_stored_df = _load_dashboard_catalog(_rules_mtime_ns, datetime.now(timezone.utc).date().isoformat())
_undo_history = st.session_state.get("_learned_rule_undo_history", [])
_undo_rules = _undo_history[-1] if _undo_history else st.session_state.get("_learned_rule_undo", [])
if _undo_rules and st.button(
    f"Undo last learned-rule removal ({len(_undo_rules):,}) · {len(_undo_history)} undo step(s) left",
    icon=":material/undo:", key="undo_learned_rule_removal",
):
    restore_learned_image_rules(_undo_rules)
    if _undo_history:
        _undo_history.pop()
        st.session_state["_learned_rule_undo_history"] = _undo_history
    st.session_state.pop("_learned_rule_undo", None)
    st.success("Learned rules restored.")
    st.rerun()
# Import actions rerun the page after writing JSON. Keep the result in session
# state so the acknowledgement survives that rerun instead of disappearing
# immediately after ``st.success(...)`` is called.
_import_notice = st.session_state.pop("_learned_import_notice", None)
if _import_notice:
    st.success(_import_notice, icon=":material/check_circle:")

# ── Admin module navigation ────────────────────────────────────────────────
# Keep the existing learned-image workspace as the default, but make the page
# an extensible admin backend rather than a single long catalog screen.
with st.sidebar:
    st.markdown("### :material/admin_panel_settings: Admin workspace")
    _admin_module = st.radio(
        "Module",
        [
            "Learned image rules",
            "Category learned rules",
            "Settings",
            "Diagnostics",
        ],
        key="dashboard_admin_module",
        label_visibility="collapsed",
    )
    st.caption("Manage what validation learns and how it is reviewed.")


def _render_category_admin_module():
    st.markdown("## :material/category: Category learned rules")
    st.caption("Review category corrections and negative category decisions used by the category matcher.")
    try:
        from category_matcher_engine import CategoryMatcherEngine
        _db_path = Path(__file__).resolve().parents[1] / "cat_learning.db"
        try:
            _db_mtime = _db_path.stat().st_mtime_ns
        except OSError:
            _db_mtime = 0

        @st.cache_resource(show_spinner=False, max_entries=2)
        def _cached_category_engine(_mtime_ns: int):
            del _mtime_ns
            return CategoryMatcherEngine(str(_db_path))

        _category_engine = _cached_category_engine(_db_mtime)

        @st.cache_data(show_spinner=False, max_entries=4)
        def _cached_category_learning(_mtime_ns: int):
            # The mtime is deliberately the cache key: category edits from a
            # validation run or another tab invalidate the dashboard view.
            _engine = _cached_category_engine(_mtime_ns)
            return _engine.list_corrections(limit=2000), _engine.list_negatives(limit=2000)

        _corrections, _negatives = _cached_category_learning(_db_mtime)
    except Exception as _category_error:
        st.error(f"Could not load category learning data: {_category_error}", icon=":material/error:")
        return

    _category_counts = st.columns(4, gap="medium")
    _category_counts[0].metric("Positive corrections", f"{len(_corrections):,}")
    _category_counts[1].metric("Negative decisions", f"{len(_negatives):,}")
    _category_counts[2].metric("Categories taught", f"{_corrections['category'].nunique():,}" if not _corrections.empty else "0")
    _category_counts[3].metric("Database", f"{Path(_category_engine.db_path).stat().st_size / 1024 / 1024:.1f} MB" if Path(_category_engine.db_path).exists() else "0 MB")

    _category_tabs = st.tabs(["Corrections", "Rejected category decisions", "How learning works"])
    with _category_tabs[0]:
        _category_search = st.text_input("Search product name or category", key="dashboard_category_search")
        _correction_view = _corrections.copy()
        if _category_search and not _correction_view.empty:
            _q = _category_search.casefold()
            _correction_view = _correction_view[
                _correction_view["name"].astype(str).str.casefold().str.contains(_q, na=False)
                | _correction_view["category"].astype(str).str.casefold().str.contains(_q, na=False)
            ]
        st.dataframe(_correction_view.head(500), hide_index=True, width="stretch")
        if not _correction_view.empty:
            _delete_category_ids = st.multiselect(
                "Select corrections to remove",
                options=_correction_view["id"].astype(int).tolist(),
                format_func=lambda value: f"{value} · {_correction_view.loc[_correction_view['id'].eq(value), 'name'].iloc[0][:70]}",
                key="dashboard_delete_category_corrections",
            )
            if st.button("Remove selected corrections", icon=":material/delete_sweep:", disabled=not _delete_category_ids, key="dashboard_remove_category_corrections"):
                _removed = _category_engine.delete_corrections(_delete_category_ids)
                st.success(f"Removed {_removed:,} category correction(s).")
                st.rerun()
    with _category_tabs[1]:
        st.dataframe(_negatives.head(500), hide_index=True, width="stretch")
        if not _negatives.empty:
            _delete_negative_ids = st.multiselect(
                "Select negative decisions to remove",
                options=_negatives["id"].astype(int).tolist(),
                format_func=lambda value: f"{value} · {_negatives.loc[_negatives['id'].eq(value), 'name'].iloc[0][:70]}",
                key="dashboard_delete_category_negatives",
            )
            if st.button("Remove selected negative decisions", icon=":material/delete_sweep:", disabled=not _delete_negative_ids, key="dashboard_remove_category_negatives"):
                _removed = _category_engine.delete_negatives(_delete_negative_ids)
                st.success(f"Removed {_removed:,} negative category decision(s).")
                st.rerun()
    with _category_tabs[2]:
        st.info("Positive corrections teach a product-name pattern to a category. Negative decisions prevent a known wrong category from being suggested again. Removing a row takes effect for new validations after the matcher reloads.", icon=":material/lightbulb:")


def _render_admin_settings_module():
    st.markdown("## :material/settings: Validation settings")
    st.caption("Session preferences for review and administration. Validation rules remain unchanged here.")
    _settings_cols = st.columns(2, gap="large")
    with _settings_cols[0]:
        st.markdown("### Review defaults")
        st.selectbox("Default cards per page", [50, 100, 200, 500], key="dashboard_default_page_size")
        st.toggle("Open image previews by default", key="dashboard_default_image_previews")
        st.toggle("Show technical diagnostics", key="dashboard_show_diagnostics")
    with _settings_cols[1]:
        st.markdown("### Data locations")
        st.code(str(Path(__file__).resolve().parents[1] / "learned_image_rules.json"), language="text")
        st.code(str(Path(__file__).resolve().parents[1] / "cat_learning.db"), language="text")
        if st.button("Reload learned catalog", icon=":material/refresh:", key="dashboard_reload_catalog"):
            st.cache_data.clear()
            st.success("Catalog caches cleared. The next validation will reload current rules.")


def _render_admin_diagnostics_module():
    st.markdown("## :material/monitoring: Diagnostics")
    st.caption("Runtime health and timing information for the current session.")
    _timings = st.session_state.get("validation_stage_timings", {})
    if _timings:
        st.dataframe(pd.DataFrame([{"Stage": key, "Seconds": value} for key, value in _timings.items()]).sort_values("Seconds", ascending=False), hide_index=True, width="stretch")
    else:
        st.info("No validation timings are available in this session yet.")
    _rules_path = Path(__file__).resolve().parents[1] / "learned_image_rules.json"
    _category_path = Path(__file__).resolve().parents[1] / "cat_learning.db"
    _health = pd.DataFrame([
        {"Component": "Learned image rules", "Status": "Available" if _rules_path.exists() else "Missing", "Size": f"{_rules_path.stat().st_size / 1024 / 1024:.1f} MB" if _rules_path.exists() else "—"},
        {"Component": "Category learning", "Status": "Available" if _category_path.exists() else "Missing", "Size": f"{_category_path.stat().st_size / 1024 / 1024:.1f} MB" if _category_path.exists() else "—"},
    ])
    st.dataframe(_health, hide_index=True, width="stretch")


if _admin_module == "Category learned rules":
    _render_category_admin_module()
    st.stop()
if _admin_module == "Settings":
    _render_admin_settings_module()
    st.stop()
if _admin_module == "Diagnostics":
    _render_admin_diagnostics_module()
    st.stop()

# ── Modern dashboard shell ─────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .lr-shell { max-width: 1480px; margin: 0 auto; }
    .lr-hero { display:flex; justify-content:space-between; align-items:flex-end; gap:24px;
        padding:24px 26px; margin:0 0 18px; border-radius:18px;
        background:linear-gradient(135deg,#172033 0%,#243a5a 58%,#315d83 100%);
        color:#f8fafc; box-shadow:0 14px 35px rgba(15,23,42,.16); }
    .lr-eyebrow { font-size:11px; text-transform:uppercase; letter-spacing:.12em; color:#93c5fd; font-weight:800; }
    .lr-hero h2 { margin:5px 0 5px; font-size:25px; letter-spacing:-.03em; color:#fff; }
    .lr-hero p { margin:0; color:#cbd5e1; font-size:13px; max-width:700px; }
    .lr-hero-stat { text-align:right; min-width:130px; }
    .lr-hero-stat strong { display:block; font-size:30px; line-height:1; color:#fff; }
    .lr-hero-stat span { display:block; margin-top:6px; color:#bfdbfe; font-size:11px; font-weight:700; }
    .lr-card { min-height:112px; padding:17px 18px; border:1px solid #e2e8f0; border-radius:14px;
        background:#fff; box-shadow:0 5px 18px rgba(15,23,42,.06); }
    .lr-card-label { color:#64748b; font-size:11px; font-weight:800; text-transform:uppercase; letter-spacing:.06em; }
    .lr-card-value { color:#0f172a; font-size:26px; font-weight:800; margin-top:7px; letter-spacing:-.03em; }
    .lr-card-note { color:#64748b; font-size:11px; margin-top:5px; }
    .lr-section { margin:22px 0 8px; color:#0f172a; font-size:16px; font-weight:800; letter-spacing:-.01em; }
    @media (max-width: 760px) { .lr-hero { display:block; } .lr-hero-stat { text-align:left; margin-top:18px; } }
    </style>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    f'''<div class="lr-shell"><div class="lr-hero"><div><div class="lr-eyebrow">Rule intelligence</div>
    <h2>Learned rules control center</h2><p>Review, correct, and monitor image rules that protect future validations from repeated mistakes.</p>
    </div><div class="lr-hero-stat"><strong>{len(_stored_df):,}</strong><span>stored rules</span></div></div></div>''',
    unsafe_allow_html=True,
)

# Keep importing visible without making reviewers scroll past the catalog.
with st.container(border=True):
    st.markdown("### :material/upload_file: Import reports")
    st.caption("Upload multiple Excel, CSV, or ZIP reports. ZIP files are expanded automatically and duplicate image rules are skipped.")
    with st.expander(":material/help: Accepted upload columns", expanded=False):
        st.markdown(
            "Use **FLAG** to identify the learned reason. A brand column is optional and can be used with any learned reason, including counterfeit, prohibited product, or restricted-brand rules. FDA remains category-validator owned."
        )
        st.dataframe(
            pd.DataFrame([
                {"PRODUCT_SET_SID": "optional", "IMAGE_URL": "required", "FLAG": "required", "REASON": "optional detail", "BRAND_INFRINGED": "optional", "SELLER_NAME": "optional", "CATEGORY_CODE": "optional", "STATUS": "optional"},
                {"PRODUCT_SET_SID": "SKU-001", "IMAGE_URL": "https://example.com/image.jpg", "FLAG": "Counterfeit Sneakers", "REASON": "Brand logo appears unauthorized", "BRAND_INFRINGED": "Example Brand", "SELLER_NAME": "Seller A", "CATEGORY_CODE": "fashion", "STATUS": "rejected"},
            ]),
            hide_index=True,
            width="stretch",
        )
        st.download_button(
            "Download CSV template",
            pd.DataFrame(columns=["PRODUCT_SET_SID", "IMAGE_URL", "FLAG", "REASON", "BRAND_INFRINGED", "SELLER_NAME", "CATEGORY_CODE", "STATUS"]).to_csv(index=False).encode("utf-8"),
            file_name="learned_rules_import_template.csv",
            mime="text/csv",
            icon=":material/download:",
            key="download_learned_rules_template",
        )
    uploads = st.file_uploader(
        "Choose report files",
        type=["xlsx", "xls", "csv", "zip"],
        accept_multiple_files=True,
        key="dashboard_report_uploads",
        label_visibility="collapsed",
    )

_dashboard_review = _stored_df.get("review_state", pd.Series("Unreviewed", index=_stored_df.index)).fillna("Unreviewed").astype(str)
_dashboard_active = int(_stored_df.get("status", pd.Series("active", index=_stored_df.index)).astype(str).str.casefold().eq("active").sum())
_dashboard_confirmed = int(_dashboard_review.eq("Confirmed").sum())
_dashboard_corrected = int(_dashboard_review.eq("Corrected").sum())
_dashboard_unreviewed = int(_dashboard_review.eq("Unreviewed").sum())
_dashboard_reviewed = max(0, len(_stored_df) - _dashboard_unreviewed)
_dashboard_review_pct = _dashboard_reviewed / len(_stored_df) if len(_stored_df) else 0
_dashboard_active_pct = _dashboard_active / len(_stored_df) if len(_stored_df) else 0
st.markdown('<div class="lr-section">Rule health</div>', unsafe_allow_html=True)
_dashboard_cols = st.columns(4, gap="medium")
for _col, (_label, _value, _note) in zip(
    _dashboard_cols,
    (("Active rules", _dashboard_active, "Applied during validation"),
     ("Awaiting review", _dashboard_unreviewed, "Need a reviewer decision"),
     ("Confirmed", _dashboard_confirmed, "Validated as reliable"),
     ("Corrected", _dashboard_corrected, "Candidates for removal")),
):
    with _col:
        st.markdown(f'<div class="lr-card"><div class="lr-card-label">{_label}</div><div class="lr-card-value">{_value:,}</div><div class="lr-card-note">{_note}</div></div>', unsafe_allow_html=True)
_progress_cols = st.columns(2, gap="large")
with _progress_cols[0]:
    st.caption(f":material/task_alt: Review coverage · {_dashboard_reviewed:,} of {len(_stored_df):,}")
    st.progress(_dashboard_review_pct, text=f"{_dashboard_review_pct:.0%} reviewed")
with _progress_cols[1]:
    st.caption(f":material/shield: Active rule coverage · {_dashboard_active:,} of {len(_stored_df):,}")
    st.progress(_dashboard_active_pct, text=f"{_dashboard_active_pct:.0%} active")
_dashboard_flags = _stored_df.get("flag", pd.Series(dtype=str)).astype(str).value_counts().head(6)
if not _dashboard_flags.empty:
    with st.expander(":material/analytics: Rule distribution", expanded=False):
        _distribution_cols = st.columns(min(3, len(_dashboard_flags)), gap="small")
        for _distribution_index, (_distribution_flag, _distribution_count) in enumerate(_dashboard_flags.items()):
            with _distribution_cols[_distribution_index % len(_distribution_cols)]:
                st.metric(str(_distribution_flag), f"{int(_distribution_count):,}")

if not _stored_df.empty:
    with st.expander(":material/insights: Learned data insights", expanded=False):
        _insight_tabs = st.tabs(["Summary", "Duplicate clusters", "Suggestions"])
        with _insight_tabs[0]:
            _summary_cols = st.columns(4)
            _summary_cols[0].metric("Rules overdue for review", f"{int(_stored_df['lifecycle'].eq('Review overdue').sum()):,}")
            _summary_cols[1].metric("Multi-seller clusters", f"{int(_stored_df['sellers_affected'].ge(2).sum()):,}")
            _summary_cols[2].metric("High-confidence rules", f"{int(_stored_df['confidence_score'].ge(.8).sum()):,}")
            _summary_cols[3].metric("Correction rate", f"{(_stored_df['review_state'].eq('Corrected').mean() * 100):.1f}%")
            _left, _right = st.columns(2, gap="large")
            with _left:
                st.caption(":material/flag: Top rejection reasons")
                st.dataframe(_stored_df["flag"].value_counts().head(8).rename("Rules"), width="stretch")
            with _right:
                st.caption(":material/storefront: Sellers with the most learned matches")
                _seller_summary = _stored_df["seller_name"].replace("", pd.NA).dropna().value_counts().head(8).rename("Rules") if "seller_name" in _stored_df.columns else pd.Series(dtype=int)
                st.dataframe(_seller_summary, width="stretch")
            if "category_code" in _stored_df.columns:
                st.caption(":material/category: Categories with the most learned matches")
                st.dataframe(_stored_df["category_code"].replace("", pd.NA).dropna().value_counts().head(10).rename("Rules"), width="stretch")
        with _insight_tabs[1]:
            _cluster_table = (
                _stored_df[_stored_df["_cluster_key"].ne("")]
                .groupby("_cluster_key", as_index=False)
                .agg(Products=("_cluster_key", "size"), Sellers=("sellers_affected", "max"), Confirmed=("confirmed_decisions", "max"), Reason=("flag", "first"))
                .sort_values(["Products", "Sellers"], ascending=False)
            )
            if _cluster_table.empty:
                st.info("No image clusters are available yet.")
            else:
                st.caption("Clusters group exact image URLs or exact pHash values. Confirming a representative applies the review state to every rule in that cluster.")
                st.dataframe(_cluster_table.head(100), hide_index=True, width="stretch")
                _cluster_options = _cluster_table["_cluster_key"].head(100).tolist()
                _selected_cluster = st.selectbox("Cluster to review", _cluster_options, format_func=lambda value: f"{value[:42]} · {_cluster_table.loc[_cluster_table['_cluster_key'].eq(value), 'Products'].iloc[0]:,} products", key="learned_cluster_choice")
                _cluster_records = _stored_df[_stored_df["_cluster_key"].eq(_selected_cluster)]
                if st.button("Confirm entire cluster", icon=":material/done_all:", key="confirm_learned_cluster"):
                    for _, _cluster_rule in _cluster_records.iterrows():
                        set_learned_image_rule_review(_cluster_rule.to_dict(), "Confirmed")
                    st.success(f"Confirmed {_cluster_records.shape[0]:,} rules in the cluster.")
                    st.rerun()
        with _insight_tabs[2]:
            _suggestions = _stored_df[
                _stored_df["cluster_size"].ge(3) & _stored_df["review_state"].eq("Unreviewed")
            ].drop_duplicates("_cluster_key").sort_values(["cluster_size", "sellers_affected"], ascending=False)
            if _suggestions.empty:
                st.info("No repeated unreviewed image patterns need attention.")
            else:
                st.caption("These patterns appeared repeatedly and are good candidates for a confirmed learned rule or cluster review.")
                st.dataframe(_suggestions[[c for c in ("_cluster_key", "flag", "cluster_size", "sellers_affected", "confidence") if c in _suggestions.columns]].head(100), hide_index=True, width="stretch")

# Lightweight queues that turn the catalog and the current validation report
# into actionable maintenance work. They stay collapsed so opening Dashboard
# does not mount another large table unless the reviewer asks for it.
with st.expander(":material/cleaning_services: Rule conflicts and cleanup", expanded=False):
    _duplicate_rule_groups = pd.DataFrame()
    if {"image_url", "flag"}.issubset(_stored_df.columns):
        _rule_rows = _stored_df.copy()
        _rule_rows["_image_key"] = _rule_rows["image_url"].fillna("").astype(str).str.strip()
        if "phash" in _rule_rows.columns:
            _rule_rows.loc[_rule_rows["_image_key"].eq(""), "_image_key"] = _rule_rows["phash"].fillna("").astype(str).str.strip()
        _rule_rows = _rule_rows[_rule_rows["_image_key"].ne("")]
        if not _rule_rows.empty:
            _duplicate_rule_groups = (
                _rule_rows.groupby("_image_key", as_index=False)
                .agg(Rules=("_image_key", "size"), Reasons=("flag", "nunique"), Brands=("brand_infringed", "nunique"))
                .query("Rules > 1 and (Reasons > 1 or Brands > 1)")
                .sort_values(["Reasons", "Brands", "Rules"], ascending=False)
            )
    _conflict_rows = []
    _current_report = st.session_state.get("final_report", pd.DataFrame())
    if isinstance(_current_report, pd.DataFrame) and not _current_report.empty:
        _learned_col = _current_report.get("Learned Match", pd.Series(False, index=_current_report.index)).astype(str).str.casefold().isin({"true", "1", "yes", "y"})
        _review_col = _current_report.get("Learned Review Match", pd.Series(False, index=_current_report.index)).astype(str).str.casefold().isin({"true", "1", "yes", "y"})
        _approved = _current_report.get("Status", pd.Series("", index=_current_report.index)).astype(str).str.casefold().eq("approved")
        _brand_conflict = _current_report.get("FLAG", pd.Series("", index=_current_report.index)).astype(str).str.casefold().eq("brand image mismatch")
        _conflict_mask = (_learned_col & _approved) | _brand_conflict | _review_col
        if _conflict_mask.any():
            _conflict_rows = _current_report.loc[_conflict_mask].copy()
    _c1, _c2, _c3 = st.columns(3)
    _c1.metric("Conflicting current matches", f"{len(_conflict_rows):,}")
    _c2.metric("Duplicate rule groups", f"{len(_duplicate_rule_groups):,}")
    _overturn_rows = _current_report if isinstance(_current_report, pd.DataFrame) else pd.DataFrame()
    if not _overturn_rows.empty and "overturn_direction" in _overturn_rows.columns:
        _overturn_counts = _overturn_rows[_overturn_rows["overturn_direction"].astype(str).ne("")].get("FLAG", pd.Series(dtype=str)).value_counts()
    else:
        _overturn_counts = pd.Series(dtype=int)
    _c3.metric("Overturned decisions", f"{int(_overturn_counts.sum()):,}")
    if not _duplicate_rule_groups.empty:
        st.caption("Same image or pHash appears under different reasons or brands. Review these before merging or deleting rules.")
        st.dataframe(_duplicate_rule_groups.head(100), hide_index=True, width="stretch")
    if _overturn_counts.any():
        st.caption("Rules with the most overturned decisions")
        st.dataframe(_overturn_counts.head(10).rename("Overturns"), width="stretch")
        _top_overturn = str(_overturn_counts.index[0])
        if int(_overturn_counts.iloc[0]) >= 5:
            st.warning(f"{_top_overturn} has been overturned {int(_overturn_counts.iloc[0]):,} times. Consider downgrading or reviewing its learned rules.", icon=":material/priority_high:")
    if isinstance(_conflict_rows, pd.DataFrame) and not _conflict_rows.empty:
        st.caption("Current validation conflict queue")
        _conflict_cols = [c for c in ("ProductSetSid", "Status", "FLAG", "Comment", "Learned Match Method", "Learned Match Distance") if c in _conflict_rows.columns]
        st.dataframe(_conflict_rows[_conflict_cols].head(200), hide_index=True, width="stretch")

with st.expander(f"View stored rules ({len(_stored_df):,})", expanded=False):
    if _stored_df.empty:
        st.info("No learned rules have been stored yet.")
    else:
        _m1, _m2, _m3, _m4 = st.columns(4)
        _m1.metric("Total rules", f"{len(_stored_df):,}")
        _active_count = int(
            _stored_df["status"].astype(str).str.casefold().eq("active").sum()
        ) if "status" in _stored_df.columns else len(_stored_df)
        _m2.metric("Active", f"{_active_count:,}")
        _m3.metric("Image flags", f"{_stored_df.get('flag', pd.Series(dtype=str)).nunique():,}")
        _m4.metric("Sources", f"{_stored_df.get('source', pd.Series(dtype=str)).nunique():,}")
        _filter_col, _search_col = st.columns([1, 2])
        _flag_options = ["All"] + sorted(_stored_df.get("flag", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        _view_flag = _filter_col.selectbox("Filter by flag", _flag_options, key="learned_rules_view_flag")
        _view_search = _search_col.text_input("Search URL, brand, seller, or source", key="learned_rules_view_search")
        _brand_series = _stored_df["brand_infringed"].fillna("").astype(str).str.strip()
        _brand_options = ["All"] + sorted([_brand for _brand in _brand_series.unique().tolist() if _brand])
        _view_brand = st.selectbox("Filter by infringed/claimed brand", _brand_options, key="learned_rules_view_brand")
        _source_options = ["All"] + sorted(_stored_df.get("source", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        _view_source = st.selectbox("Filter by source", _source_options, key="learned_rules_view_source")
        if "review_state" not in _stored_df.columns:
            _stored_df["review_state"] = "Unreviewed"
        _review_options = ["All", "Unreviewed", "Confirmed", "Corrected", "Removed"]
        _view_review = st.selectbox("Review state", _review_options, key="learned_rules_view_review")
        _sort_option = st.selectbox(
            "Audit order",
            ["Priority", "Most reused", "Most sellers", "Brand", "Newest", "Oldest"],
            key="learned_rules_sort_order",
            help="Prioritizes multi-seller, repeated, overdue, and unresolved rules first.",
        )
        _correction_queue = _stored_df[
            _stored_df.get("review_state", pd.Series("Unreviewed", index=_stored_df.index)).fillna("Unreviewed").astype(str).eq("Corrected")
        ]
        if not _correction_queue.empty:
            with st.expander(f"False-positive correction queue ({len(_correction_queue):,})", expanded=False):
                st.caption("These learned rules were marked Corrected during review. Remove them after confirming the correction so they stop rejecting future listings.")
                st.dataframe(
                    _correction_queue[[c for c in ("image_url", "phash", "flag", "brand", "seller_name", "review_state") if c in _correction_queue.columns]].head(200),
                    hide_index=True, width="stretch",
                )
                if st.button("Remove corrected rules", icon=":material/delete_sweep:", key="remove_corrected_rules", type="secondary"):
                    _removed_corrected = delete_learned_image_rules(_correction_queue.to_dict("records"))
                    st.success(f"Removed {_removed_corrected:,} corrected rule(s).")
                    st.rerun()
        _view_df = _stored_df.copy()
        if _view_flag != "All" and "flag" in _view_df.columns:
            _view_df = _view_df[_view_df["flag"].astype(str).eq(_view_flag)]
        if _view_search:
            _needle = _view_search.casefold()
            _search_cols = [c for c in ("image_url", "brand", "seller_name", "source", "reason") if c in _view_df.columns]
            _mask = _view_df[_search_cols].astype(str).apply(lambda col: col.str.casefold().str.contains(_needle, na=False)).any(axis=1)
            _view_df = _view_df[_mask]
        if _view_source != "All" and "source" in _view_df.columns:
            _view_df = _view_df[_view_df["source"].astype(str).eq(_view_source)]
        if _view_brand != "All" and "brand_infringed" in _view_df.columns:
            _view_df = _view_df[_view_df["brand_infringed"].fillna("").astype(str).str.strip().eq(_view_brand)]
        if _view_review != "All":
            _view_df = _view_df[_view_df["review_state"].fillna("Unreviewed").astype(str).eq(_view_review)]
        if _sort_option == "Priority":
            _view_df = _view_df.sort_values(["confidence_score", "sellers_affected", "cluster_size"], ascending=[False, False, False])
        elif _sort_option == "Most reused":
            _view_df = _view_df.sort_values("cluster_size", ascending=False)
        elif _sort_option == "Most sellers":
            _view_df = _view_df.sort_values("sellers_affected", ascending=False)
        elif _sort_option == "Brand":
            _view_df = _view_df.assign(_brand_sort=_view_df["brand_infringed"].fillna("").astype(str).str.casefold()).sort_values("_brand_sort").drop(columns=["_brand_sort"])
        elif _sort_option == "Newest":
            _view_df = _view_df.sort_values("created_at", ascending=False)
        else:
            _view_df = _view_df.sort_values("created_at", ascending=True)
        _display_cols = [c for c in ("image_url", "phash", "flag", "reason", "brand_infringed", "brand", "seller_name", "source", "status", "created_at", "last_matched_at", "last_confirmed_at", "confidence", "cluster_size", "sellers_affected", "lifecycle", "review_due_at") if c in _view_df.columns]
        st.caption(f"Showing {_view_df.shape[0]:,} of {_stored_df.shape[0]:,} rules")
        _audit_mode = st.toggle(
            "Audit card view",
            value=False,
            key="learned_rules_audit_mode",
            help="Show larger images and actions for reviewing learned rules.",
        )
        if _audit_mode:
            _audit_limit = st.select_slider(
                "Audit cards to load",
                options=[24, 60, 100],
                value=60,
                key="learned_rules_audit_limit",
                help="Load more cards when reviewing a large filtered set.",
            )
            _audit_rows = _view_df[
                _view_df.get("image_url", pd.Series(index=_view_df.index, dtype=str)).astype(str).str.strip().ne("")
            ].head(_audit_limit)
            if _audit_rows.empty:
                st.info("No image rules match the current filters.")
            else:
                st.caption(f"Reviewing {_audit_rows.shape[0]:,} image rules. Actions update the JSON catalog immediately.")
                for _audit_start in range(0, len(_audit_rows), 4):
                    _audit_cols = st.columns(4, gap="small")
                    for _audit_offset, (_audit_col, (_audit_idx, _audit_row)) in enumerate(
                        zip(_audit_cols, _audit_rows.iloc[_audit_start:_audit_start + 4].iterrows())
                    ):
                        with _audit_col:
                            _audit_url = str(_audit_row.get("image_url", "")).strip()
                            try:
                                st.image(_audit_url, width=180)
                            except Exception:
                                st.caption("Image unavailable")
                            _audit_flag = str(_audit_row.get("flag", ""))
                            _audit_sid = str(_audit_row.get("product_set_sid", "")).strip()
                            _audit_seller = str(_audit_row.get("seller_name", "")).strip()
                            _audit_source = str(_audit_row.get("source", "")).strip()
                            _audit_reason_text = str(_audit_row.get("reason", "") or "").strip()
                            _audit_brand = str(_audit_row.get("brand_infringed", _audit_row.get("brand", "")) or "").strip()
                            _audit_category = str(_audit_row.get("category_code", "") or "").strip()
                            _audit_review = str(_audit_row.get("review_state", "Unreviewed") or "Unreviewed")
                            _audit_image_key = _audit_url or str(_audit_row.get("phash", "")).strip()
                            _audit_matches = _stored_df[
                                _stored_df.get("image_url", pd.Series(index=_stored_df.index, dtype=str)).astype(str).eq(_audit_image_key)
                            ] if _audit_url else _stored_df[
                                _stored_df.get("phash", pd.Series(index=_stored_df.index, dtype=str)).astype(str).eq(_audit_image_key)
                            ]
                            _audit_seller_count = _audit_matches.get("seller_name", pd.Series(dtype=str)).astype(str).replace({"": pd.NA}).nunique(dropna=True)
                            st.markdown(f"**{_audit_flag}**")
                            if _audit_reason_text:
                                st.markdown(f"**Rejection reason:** {_audit_reason_text}")
                            if _audit_brand:
                                st.markdown(f"**Brand infringed/claimed:** **{_audit_brand}**")
                            if _audit_category:
                                st.caption(f"Category: {_audit_category}")
                            st.caption(
                                f"SKU: {_audit_sid or '—'}\n"
                                f"Seller: {_audit_seller or '—'}\n"
                                f"Source: {_audit_source or '—'}\n"
                                f"Review: {_audit_review}\n"
                                f"Rules using image: {len(_audit_matches):,} · Sellers: {_audit_seller_count:,}"
                            )
                            # Timestamps are not guaranteed to be unique when a report
                            # imports several rules at once. Include the row identity so
                            # every Streamlit widget receives a stable unique key.
                            _audit_key = f"{_audit_start + _audit_offset}_{_audit_idx}_{str(_audit_row.get('created_at', '')).replace('-', '')[-16:]}"
                            _audit_new_flag = st.selectbox(
                                "Reason",
                                sorted(LEARNABLE_FLAGS),
                                index=(sorted(LEARNABLE_FLAGS).index(_audit_flag) if _audit_flag in LEARNABLE_FLAGS else 0),
                                key=f"audit_reason_{_audit_key}",
                            )
                            _a1, _a2, _a3 = st.columns(3)
                            with _a1:
                                if st.button("Confirm", icon=":material/check_circle:", key=f"audit_confirm_{_audit_key}", width="stretch"):
                                    if set_learned_image_rule_review(_audit_row.to_dict(), "Confirmed"):
                                        st.rerun()
                            with _a2:
                                if st.button("Corrected", icon=":material/edit_note:", key=f"audit_correct_{_audit_key}", width="stretch"):
                                    if set_learned_image_rule_review(_audit_row.to_dict(), "Corrected"):
                                        st.rerun()
                            with _a3:
                                if st.button("Save reason", icon=":material/save:", key=f"audit_save_{_audit_key}", width="stretch"):
                                    if update_learned_image_rule_flag(_audit_row.to_dict(), _audit_new_flag):
                                        st.success("Saved")
                                        st.rerun()
                            with _a1:
                                if st.button("Remove", icon=":material/delete:", key=f"audit_remove_{_audit_key}", width="stretch"):
                                    if delete_learned_image_rules([_audit_row.to_dict()]):
                                        st.success("Removed")
                                        st.rerun()
                            st.divider()
        # A small gallery makes it possible to verify the image before
        # deleting its rule. Keep it bounded so a large catalog stays fast.
        _preview_rows = _view_df[
            _view_df.get("image_url", pd.Series(index=_view_df.index, dtype=str)).astype(str).str.strip().ne("")
        ].head(40)
        _preview_enabled = st.toggle(
            "Load image previews",
            value=False,
            key="learned_rules_load_previews",
            help="Images are loaded only when requested because remote image previews are expensive for large catalogs.",
        )
        if _preview_enabled and not _preview_rows.empty:
            with st.expander(f"Image preview ({len(_preview_rows):,})", expanded=False):
                st.caption(":material/search: Gallery follows the flag, source, review-state, and text filters above. Each card can be edited or removed directly.")
                _select_all_preview = st.checkbox(
                    f"Select all visible images ({len(_preview_rows):,})",
                    key="learned_preview_select_all",
                )
                _preview_selected_records = []
                _preview_cols = st.columns(4)
                for _preview_index, (_, _preview_row) in enumerate(_preview_rows.iterrows()):
                    _preview_url = str(_preview_row.get("image_url", "")).strip()
                    _preview_flag = str(_preview_row.get("flag", ""))
                    _preview_sid = str(_preview_row.get("sid", _preview_row.get("product_set_sid", ""))).strip()
                    _preview_review = str(_preview_row.get("review_state", "Unreviewed") or "Unreviewed")
                    _preview_key = f"{_preview_index}_{str(_preview_row.get('created_at', '')).replace('-', '')[-16:]}"
                    with _preview_cols[_preview_index % 4]:
                        _preview_selected = _select_all_preview or st.checkbox(
                            "Select",
                            key=f"preview_select_{_preview_key}",
                            label_visibility="collapsed",
                        )
                        if _preview_selected:
                            _preview_selected_records.append(_preview_row.to_dict())
                        try:
                            st.image(_preview_url, width="stretch", output_format="auto")
                        except Exception:
                            st.caption("Image unavailable")
                        st.markdown(f"**{_preview_flag}**")
                        st.caption(f"{_preview_sid or 'SKU unavailable'} · {_preview_review}")
                        _preview_reason = st.selectbox(
                            "Reason",
                            sorted(LEARNABLE_FLAGS),
                            index=(sorted(LEARNABLE_FLAGS).index(_preview_flag) if _preview_flag in LEARNABLE_FLAGS else 0),
                            key=f"preview_reason_{_preview_key}",
                            label_visibility="collapsed",
                        )
                        _preview_action_cols = st.columns(3, gap="small")
                        with _preview_action_cols[0]:
                            if st.button("", icon=":material/check_circle:", help="Mark this rule confirmed", key=f"preview_confirm_{_preview_key}"):
                                if set_learned_image_rule_review(_preview_row.to_dict(), "Confirmed"):
                                    st.rerun()
                        with _preview_action_cols[1]:
                            if st.button("", icon=":material/save:", help="Save the selected reason", key=f"preview_save_{_preview_key}"):
                                if update_learned_image_rule_flag(_preview_row.to_dict(), _preview_reason):
                                    st.rerun()
                        with _preview_action_cols[2]:
                            if st.button("", icon=":material/delete:", help="Remove this learned rule", key=f"preview_remove_{_preview_key}"):
                                if delete_learned_image_rules([_preview_row.to_dict()]):
                                    st.rerun()
                if _preview_selected_records:
                    _bulk_remove_col, _bulk_count_col = st.columns([2, 3], gap="small")
                    with _bulk_count_col:
                        st.caption(f":material/checklist: {len(_preview_selected_records):,} image rule(s) selected")
                    with _bulk_remove_col:
                        if st.button(
                            "Remove selected images",
                            icon=":material/delete_sweep:",
                            type="secondary",
                            key="remove_selected_preview_rules",
                            width="stretch",
                        ):
                            _removed_preview = delete_learned_image_rules(_preview_selected_records)
                            st.success(f"Removed {_removed_preview:,} selected image rule(s).")
                            st.rerun()
        st.caption("Tick Remove for rules you want to delete, then save the catalog.")
        # Keep the editor responsive; use the filters above to work through
        # the full catalog in bounded pages instead of mounting 500 wide rows
        # on every Dashboard rerun.
        _edit_df = _view_df.head(100).copy()
        _edit_df.insert(0, "Remove", False)
        _editor_cols = ["Remove"] + [c for c in _display_cols if c != "Remove"]
        _edited = st.data_editor(
            _edit_df[_editor_cols],
            hide_index=True,
            width="stretch",
            disabled=[c for c in _editor_cols if c != "Remove"],
            column_config={"Remove": st.column_config.CheckboxColumn("Remove", help="Delete this rule", default=False)},
            key="learned_rules_editor",
        )
        _selected = _edited[_edited["Remove"].astype(bool)] if "Remove" in _edited.columns else pd.DataFrame()
        if st.button("Delete selected rules", icon=":material/delete_sweep:", type="secondary", disabled=_selected.empty, key="delete_selected_learned_rules"):
            _records = _selected.drop(columns=["Remove"], errors="ignore").to_dict("records")
            _deleted = delete_learned_image_rules(_records)
            st.success(f"Deleted {_deleted:,} learned rule(s).")
            st.rerun()
        st.download_button(
            "Download filtered rules (CSV)",
            _view_df[_display_cols].to_csv(index=False).encode("utf-8"),
            file_name="learned_image_rules.csv",
            mime="text/csv",
            key="download_learned_rules_csv",
        )


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [str(c).strip().upper().replace(" ", "_") for c in frame.columns]
    return frame


@st.cache_data(show_spinner=False, max_entries=24)
def _read_report_cached(source_name: str, source_bytes: bytes, _parser_version: str = "v3") -> pd.DataFrame:
    """Cache parsed report frames so reruns do not reopen the same workbook."""
    _required = {
        "PRODUCT_SET_SID", "PRODUCTSETSID", "PRODUCT_SET_SID_", "SID",
        "MAIN_IMAGE", "IMAGE1", "IMAGE", "IMAGE_URL", "MAIN_IMAGE_URL",
        "FLAG", "REJECTION_FLAG", "ISSUE", "REASON_FLAG", "STATUS", "LISTING_STATUS",
        "REASON", "COMMENT", "COMMENT_DETAIL", "BRAND", "BRAND_INFRINGED", "INFRINGED_BRAND", "BRAND_CLAIMED", "CLAIMED_BRAND", "CATEGORY_CODE", "SELLER_NAME",
    }
    _use_required = lambda name: str(name).strip().upper().replace(" ", "_") in _required
    frame = (
        pd.read_csv(BytesIO(source_bytes), dtype=str, usecols=_use_required)
        if source_name.lower().endswith(".csv")
        else pd.read_excel(BytesIO(source_bytes), dtype=str, usecols=_use_required)
    ).fillna("")
    return _normalise_columns(frame)


def _pick(columns, names):
    for name in names:
        if name in columns:
            return name
    return None


def _load_hash_cache() -> dict:
    path = Path(__file__).resolve().parents[1] / "Image_learn_cache.json"
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return {
            str(url).strip(): str(item.get("phash", "") or "").strip()
            for url, item in value.items()
            if isinstance(item, dict)
        }
    except (OSError, ValueError, AttributeError):
        return {}


def _flag_name(value):
    return normalize_learned_flag(value)


def _report_frames(uploaded):
    """Yield (display_name, bytes) for reports or reports inside ZIP files."""
    name = uploaded.name.lower()
    if not name.endswith(".zip"):
        yield uploaded.name, uploaded.getvalue()
        return
    try:
        with zipfile.ZipFile(BytesIO(uploaded.getvalue())) as archive:
            for info in archive.infolist():
                if info.is_dir() or not info.filename.lower().endswith((".xlsx", ".xls", ".csv")):
                    continue
                yield f"{uploaded.name} / {info.filename}", archive.read(info)
    except zipfile.BadZipFile:
        st.warning(f"{uploaded.name}: invalid ZIP archive")


if uploads:
    hash_cache = _load_hash_cache()
    prepared = []
    problems = []
    unsupported_flags = set()
    for uploaded in uploads:
        for source_name, source_bytes in _report_frames(uploaded):
            # Keep the original report in durable object storage when R2 is
            # configured. The local parser still handles the import exactly
            # as before, and the upload runs in the background.
            _report_key = hashlib.sha256(source_name.encode("utf-8") + source_bytes).hexdigest()[:24]
            r2_storage.upload_bytes_async(
                source_bytes,
                f"validation/historical-reports/{_report_key}-{Path(source_name).name}",
                "application/octet-stream",
            )
            try:
                frame = _read_report_cached(source_name, source_bytes, "v3")
            except Exception as exc:
                problems.append(f"{source_name}: could not read ({exc})")
                continue

            sid_col = _pick(frame.columns, ["PRODUCT_SET_SID", "PRODUCTSETSID", "PRODUCT_SET_SID_", "SID"])
            image_col = _pick(frame.columns, ["MAIN_IMAGE", "IMAGE1", "IMAGE", "IMAGE_URL", "MAIN_IMAGE_URL"])
            flag_col = _pick(frame.columns, ["FLAG", "REJECTION_FLAG", "ISSUE", "REASON_FLAG"])
            status_col = _pick(frame.columns, ["STATUS", "LISTING_STATUS"])
            reason_col = _pick(frame.columns, ["REASON", "COMMENT", "COMMENT_DETAIL"])
            brand_col = _pick(frame.columns, ["BRAND", "BRAND_INFRINGED", "INFRINGED_BRAND", "BRAND_CLAIMED", "CLAIMED_BRAND"])
            if not image_col:
                problems.append(f"{source_name}: needs an image URL column")
                continue

            # External image catalogs often contain only image, reason, and
            # infringed-brand columns. Create a stable SID for those rows and
            # infer the restricted-brand flag when no explicit flag exists.
            # A template may include PRODUCT_SET_SID as an optional column but
            # leave every cell blank. Treat that exactly like a missing column;
            # otherwise the import groups all rows under one empty SID and only
            # the first image can be learned.
            if not sid_col or frame[sid_col].astype(str).str.strip().eq("").all():
                frame["PRODUCT_SET_SID"] = [
                    "manual-image-" + hashlib.sha1(f"{source_name}|{idx}|{url}".encode("utf-8")).hexdigest()[:16]
                    for idx, url in enumerate(frame[image_col].astype(str))
                ]
                sid_col = "PRODUCT_SET_SID"
            if not flag_col:
                if reason_col:
                    frame["FLAG"] = frame[reason_col].map(_flag_name)
                    if frame["FLAG"].isin(LEARNABLE_FLAGS).all():
                        flag_col = "FLAG"
                    else:
                        problems.append(f"{source_name}: add a FLAG column such as 'Counterfeit Sneakers' or 'Restricted brands'; REASON is treated as detail text")
                        continue
                else:
                    problems.append(f"{source_name}: add a FLAG column; BRAND_INFRINGED may be used with any learned reason")
                    continue

            _rename_columns = {sid_col: "PRODUCT_SET_SID", image_col: "MAIN_IMAGE", flag_col: "FLAG"}
            if brand_col:
                _rename_columns[brand_col] = "BRAND_INFRINGED"
            work = frame.rename(columns=_rename_columns).copy()
            if status_col:
                # STATUS is optional in the manual image catalogue template.
                # Excel reads blank cells as empty strings, so filtering every
                # STATUS column to only "rejected" used to discard the entire
                # file when the column existed but was intentionally blank.
                _status = work[status_col].astype(str).str.strip().str.casefold()
                if _status.ne("").any():
                    work = work[_status.isin({"", "rejected"})]
            work["FLAG"] = work["FLAG"].map(_flag_name)
            unsupported_flags.update(
                str(value).strip() for value in work["FLAG"].unique()
                if str(value).strip() and value not in LEARNABLE_FLAGS
            )
            work["MAIN_IMAGE"] = work["MAIN_IMAGE"].astype(str).str.strip()
            work = work[work["MAIN_IMAGE"].ne("") & work["FLAG"].isin(LEARNABLE_FLAGS)]
            work["_SOURCE_FILE"] = source_name
            work["_REASON"] = work[reason_col].astype(str) if reason_col else "Imported historical rejection"
            prepared.append(work)

    if problems:
        for problem in problems:
            st.warning(problem)
    if unsupported_flags:
        st.warning(
            "Unsupported flags were skipped: "
            + ", ".join(sorted(unsupported_flags))
            + ". Add their canonical name and aliases to FLAG_ALIASES in learned_rules.py before importing them."
        )
    if prepared:
        preview = pd.concat(prepared, ignore_index=True)
        st.subheader("Import preview")
        c1, c2, c3 = st.columns(3)
        c1.metric("Eligible rows", len(preview))
        c2.metric("Unique image URLs", preview["MAIN_IMAGE"].nunique())
        c3.metric("Current learned rules", len(load_learned_image_rules()))
        st.dataframe(
            preview[["PRODUCT_SET_SID", "MAIN_IMAGE", "FLAG", "_SOURCE_FILE"]].head(100),
            hide_index=True,
            width="stretch",
        )

        if st.button("Import eligible rules", icon=":material/upload_file:", type="primary", width="stretch"):
            _progress = st.progress(0, text="Starting import…")
            _status = st.empty()
            _files = list(preview.groupby("_SOURCE_FILE"))
            _file_progress = {
                _source_name: st.progress(0, text=f"Queued · {_source_name}")
                for _source_name, _source_group in _files
            }
            _batches = []
            for _source_name, _source_group in _files:
                for flag, group in _source_group.groupby("FLAG"):
                    _batches.append({
                        "data": group,
                        "sids": group["PRODUCT_SET_SID"].astype(str).unique(),
                        "flag": flag,
                        "reason": str(group["_REASON"].iloc[0]).strip() or "Imported historical rejection",
                        "source": str(_source_name),
                        "hash_by_url": hash_cache,
                        "source_name": str(_source_name),
                    })

            def _report_import_progress(_batch_index, _batch_total, _batch):
                _source_name = _batch.get("source_name", "report")
                _source_batches = sum(1 for item in _batches if item.get("source_name") == _source_name)
                _source_done = sum(1 for item in _batches[:_batch_index] if item.get("source_name") == _source_name)
                _file_progress[_source_name].progress(
                    _source_done / max(_source_batches, 1),
                    text=f"{_source_name} · {_source_done}/{_source_batches} groups",
                )
                _progress.progress(
                    _batch_index / max(_batch_total, 1),
                    text=f"Prepared {_batch_index}/{_batch_total} groups",
                )
                _status.caption(f"Preparing {_source_name}; catalog will be written once after all files are processed.")

            try:
                added = learn_image_rejections_bulk(_batches, progress_callback=_report_import_progress)
            except Exception as _import_error:
                _progress.empty()
                _status.empty()
                st.error(f"Import failed: {_import_error}", icon=":material/error:")
                st.exception(_import_error)
                added = None
            if added is None:
                st.stop()
            for _source_name, _source_group in _files:
                _file_progress[_source_name].progress(1.0, text=f"Complete · {_source_name}")
            _progress.progress(1.0, text=f"Import complete · {added:,} new rules")
            _skipped = max(0, len(preview) - int(added))
            st.session_state["_learned_import_notice"] = (
                f"Import complete: {int(added):,} new learned rules added; "
                f"{_skipped:,} duplicate or ineligible rows skipped."
            )
            st.rerun()
    elif uploads:
        # Previously an upload with zero eligible rows simply reached the end
        # of the page with no result, which looked like the uploader ignored
        # the file. Always explain why nothing entered the catalog.
        st.error(
            "No eligible learned rules were found in the uploaded file(s). "
            "Check that IMAGE_URL and FLAG columns are present and that FLAG "
            "uses a supported learned reason.",
            icon=":material/info:",
        )
else:
    st.info("Upload one or more old Excel/CSV reports to begin.")
