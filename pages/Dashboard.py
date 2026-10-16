"""Import historical rejection reports into the learned image-rule catalog."""

import json
import hashlib
import gzip
import os
import re
import getpass
import shutil
import zipfile
from io import BytesIO, TextIOWrapper
from pathlib import Path
from datetime import datetime, timedelta, timezone
import tempfile

APP_ROOT = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

import pandas as pd
import streamlit as st
import r2_storage
import supabase_store
from processing_automation import clear_derived_image_cache
from operational_rule_lists import render_rule_lists_admin_module

from learned_rules import (
    DELETED_RULES_ARCHIVE,
    LEARNABLE_FLAGS,
    delete_learned_image_rules,
    deduplicate_learned_image_rules,
    get_cloud_sync_status,
    load_learned_image_rules,
    learn_image_rejections_bulk,
    normalize_learned_flag,
    normalize_rule_countries,
    normalize_rule_values,
    restore_learned_image_rules,
    replace_learned_image_rules,
    retry_pending_cloud_sync,
    set_learned_image_rule_review,
    update_learned_image_rule_flag,
    update_learned_image_rule_countries,
    update_learned_image_rule_scope,
    SUPPORTED_RULE_COUNTRIES,
)


def _configure_rar_backend(rarfile_module):
    """Configure rarfile with the local or Streamlit Cloud extractor."""
    configured = os.environ.get("UNRAR_TOOL", "").strip()
    candidates = [configured] if configured else []
    candidates.extend([
        shutil.which("7z"), shutil.which("7zz"), shutil.which("unrar"),
        shutil.which("unar"), shutil.which("bsdtar"),
        os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "WinRAR", "UnRAR.exe"),
        os.path.join(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"), "WinRAR", "UnRAR.exe"),
    ])
    for candidate in candidates:
        if candidate and (os.path.isfile(candidate) or shutil.which(candidate)):
            base = os.path.basename(candidate).lower()
            if base in {"7z", "7z.exe", "7zz", "7zz.exe"}:
                rarfile_module.SEVENZIP_TOOL = candidate
                rarfile_module.SEVENZIP2_TOOL = candidate
            elif base in {"unar", "unar.exe"}:
                rarfile_module.UNAR_TOOL = candidate
            else:
                rarfile_module.UNRAR_TOOL = candidate
            return candidate
    return None

st.set_page_config(page_title="Dashboard", page_icon=":material/auto_awesome:", layout="wide")


def _settings_profile():
    """Use authenticated identity when enabled; otherwise isolate local users."""
    try:
        user = st.user
        if bool(getattr(user, "is_logged_in", False)):
            identity = str(getattr(user, "email", "") or getattr(user, "sub", "")).strip().casefold()
            if identity:
                return "user-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24], True
    except Exception:
        pass
    identity = os.environ.get("USERNAME") or os.environ.get("USER") or getpass.getuser()
    if os.name == "nt" or identity not in {"", "adminuser", "appuser"}:
        return "local-" + hashlib.sha256(identity.casefold().encode("utf-8")).hexdigest()[:24], False
    return "cloud-shared", False


def _load_review_preferences(profile: str) -> dict:
    local_path = APP_ROOT / ".validation_automation" / "settings" / f"{profile}.json"
    try:
        if local_path.is_file():
            value = json.loads(local_path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        pass
    if r2_storage.enabled():
        try:
            raw = r2_storage.download_bytes(f"settings/review/{profile}.json")
            value = json.loads(raw.decode("utf-8")) if raw else {}
            return value if isinstance(value, dict) else {}
        except (ValueError, UnicodeDecodeError):
            pass
    return {}


def _save_review_preferences(profile: str, preferences: dict) -> tuple[bool, bool]:
    payload = json.dumps(preferences, ensure_ascii=False, sort_keys=True).encode("utf-8")
    local_path = APP_ROOT / ".validation_automation" / "settings" / f"{profile}.json"
    local_ok = False
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = local_path.with_suffix(".tmp")
        temp_path.write_bytes(payload)
        os.replace(temp_path, local_path)
        local_ok = True
    except OSError:
        pass
    cloud_ok = not r2_storage.enabled()
    if r2_storage.enabled():
        cloud_ok = r2_storage.upload_bytes(
            payload, f"settings/review/{profile}.json", "application/json"
        )
    return local_ok, cloud_ok


def _category_learning_payload(engine=None) -> dict:
    if engine is None:
        from category_matcher_engine import CategoryMatcherEngine
        db_path = APP_ROOT / "cat_learning.db"
        engine = CategoryMatcherEngine(str(db_path))
    corrections = engine.list_corrections(limit=200000).fillna("").to_dict("records")
    negatives = engine.list_negatives(limit=200000).fillna("").to_dict("records")
    payload = {
        "kind": "category_learning_backup",
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "corrections": corrections,
        "negatives": negatives,
    }
    payload["content_sha256"] = hashlib.sha256(json.dumps(
        {"corrections": corrections, "negatives": negatives},
        sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return payload


def _snapshot_category_learning(engine=None) -> tuple[Path, bool]:
    payload = json.dumps(_category_learning_payload(engine), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()[:16]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = APP_ROOT / ".learned_rule_snapshots"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"category-learning-{stamp}-{digest}.json.gz"
    with gzip.open(path, "wb", compresslevel=6) as handle:
        handle.write(payload)
    # Retain the newest 100 local backups in this dedicated recovery folder.
    for old in sorted(root.glob("category-learning-*.json.gz"), key=lambda item: item.stat().st_mtime, reverse=True)[100:]:
        try:
            old.unlink()
        except OSError:
            pass
    cloud_ok = not r2_storage.enabled()
    if r2_storage.enabled():
        cloud_ok = r2_storage.upload_file_verified(
            path, f"learned-rules/category-snapshots/{path.name}", "application/gzip"
        )
        if cloud_ok:
            r2_storage.prune_prefix("learned-rules/category-snapshots/", keep=100)
    return path, cloud_ok


st.title("Dashboard")
st.caption("Upload old rejection reports. Only rejected rows with an image and a supported flag are imported; duplicate URL/pHash/flag rules are skipped.")
_cloud_sync = get_cloud_sync_status()
if _cloud_sync["configured"]:
    if _cloud_sync["status"] == "failed":
        st.error(
            f"Learned-rule cloud sync failed; {_cloud_sync['pending_count']:,} change(s) are queued. "
            f"Last error: {_cloud_sync['last_error'] or 'unknown error'}",
            icon=":material/cloud_off:",
        )
        st.caption(
            f"Retry queue persisted to {_cloud_sync.get('queue_location')}." if _cloud_sync.get("queue_durable")
            else "Retry queue could not be persisted outside the current app process."
        )
        if st.button("Retry cloud sync", icon=":material/sync:", key="dashboard_retry_rule_sync"):
            _remaining = retry_pending_cloud_sync()
            if _remaining:
                st.warning(f"{_remaining:,} change(s) still need syncing.")
            else:
                st.success("Learned rules are synced to Supabase.")
            st.rerun()
    elif _cloud_sync["pending_count"]:
        st.warning(f"{_cloud_sync['pending_count']:,} learned-rule change(s) are waiting for cloud sync.", icon=":material/cloud_upload:")
        st.caption(
            f"Retry queue persisted to {_cloud_sync.get('queue_location')}." if _cloud_sync.get("queue_durable")
            else "Retry queue currently exists only on this app instance."
        )
        if st.button("Retry cloud sync", icon=":material/sync:", key="dashboard_retry_pending_rule_sync"):
            _remaining = retry_pending_cloud_sync()
            if _remaining:
                st.warning(f"{_remaining:,} change(s) still need syncing.")
            else:
                st.success("Learned rules are synced to Supabase.")
            st.rerun()
    else:
        _last_synced = _cloud_sync.get("last_synced_at", "")
        st.caption(f":material/cloud_done: Learned-rule sync · {_cloud_sync['status']}" + (f" · {_last_synced}" if _last_synced else ""))
else:
    st.caption(":material/storage: Learned rules are saved locally; Supabase is not configured.")
_runtime_review_matches = st.session_state.get("_learned_review_matches", {})
if _runtime_review_matches:
    st.info(f":material/rate_review: {len(_runtime_review_matches):,} uncertain near-pHash match(es) were sent to review instead of automatic rejection. Open the validation review queue to inspect them.")

# ── Existing catalog ────────────────────────────────────────────────────────
# Dashboard widgets rerun this script on every keystroke, filter, and editor
# click. Parsing and enriching tens of thousands of JSON rules on every rerun
# made the admin page progressively heavier. Cache the complete derived frame
# by the JSON nanosecond mtime; edits from another tab still invalidate it.
_rules_path = APP_ROOT / "learned_image_rules.json"
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
    if not frame.empty:
        frame["countries"] = frame.get("countries", pd.Series([None] * len(frame), index=frame.index)).map(
            lambda value: normalize_rule_countries(value, legacy_global=True)
        )

    if frame.empty:
        for _empty_col in ("review_state", "_cluster_key", "cluster_size", "sellers_affected", "confirmed_decisions", "review_due_at", "lifecycle", "confidence_score", "confidence", "_rule_id"):
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
    _matched_sids = frame.get("matched_sids", pd.Series([[] for _ in range(len(frame))], index=frame.index))
    _matched_sellers = frame.get("matched_sellers", pd.Series([[] for _ in range(len(frame))], index=frame.index))
    def _list_count(value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                value = [value] if value.strip() else []
        if not isinstance(value, (list, tuple, set)):
            return 0
        return len({str(item).strip() for item in value if str(item).strip()})
    _measured_sku_impact = pd.to_numeric(frame.get("match_count", pd.Series(0, index=frame.index)), errors="coerce").fillna(0).astype(int)
    _stored_sid_impact = _matched_sids.map(_list_count)
    frame["skus_affected"] = _measured_sku_impact.where(_measured_sku_impact.gt(0), _stored_sid_impact)
    _measured_seller_impact = _matched_sellers.map(_list_count)
    frame["sellers_affected"] = _measured_seller_impact.where(
        _measured_seller_impact.gt(0),
        _seller_nonempty.groupby(frame["_cluster_key"]).transform("nunique").fillna(0).astype(int),
    )
    frame["confirmed_decisions"] = frame["review_state"].eq("Confirmed").groupby(frame["_cluster_key"]).transform("sum").astype(int)
    _created_source = frame["created_at"] if "created_at" in frame.columns else pd.Series(pd.NaT, index=frame.index)
    _created = pd.to_datetime(_created_source, errors="coerce", utc=True)
    _default_due = _created + pd.to_timedelta(30, unit="D")
    _due_source = frame["review_due_at"] if "review_due_at" in frame.columns else _default_due
    frame["review_due_at"] = pd.to_datetime(_due_source, errors="coerce", utc=True)
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
    # Stable, human-friendly identity for search and deletion. It is derived
    # from the rule identity and does not alter the JSON schema or remote row.
    _identity = frame.reindex(columns=["image_url", "phash", "flag", "created_at"], fill_value="").fillna("").astype(str).agg("\x1f".join, axis=1)
    frame["_rule_id"] = "LR-" + _identity.map(lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()[:10].upper())
    return frame


def _make_compressed_csv_download(frame: pd.DataFrame, columns: list[str]):
    """Build a CSV.gz on demand, without an intermediate full-size CSV string."""
    output = tempfile.SpooledTemporaryFile(max_size=2 * 1024 * 1024, mode="w+b")
    compressed = gzip.GzipFile(fileobj=output, mode="wb", mtime=0)
    text = TextIOWrapper(compressed, encoding="utf-8", newline="")
    try:
        frame.to_csv(text, columns=columns, index=False)
        text.flush()
        text.detach()
        compressed.close()
        output.seek(0)
        return output
    except Exception:
        try:
            text.close()
        except Exception:
            pass
        output.close()
        raise


_stored_df = _load_dashboard_catalog(_rules_mtime_ns, datetime.now(timezone.utc).date().isoformat())


def _delete_rules_with_undo(records) -> int:
    """Delete selected rule records and keep the last ten sets restorable."""
    snapshots = [dict(row) for row in (records or []) if isinstance(row, dict)]
    if not snapshots:
        return 0
    removed = delete_learned_image_rules(snapshots)
    if removed:
        history = list(st.session_state.get("_learned_rule_undo_history", []))
        history.append(snapshots)
        st.session_state["_learned_rule_undo_history"] = history[-10:]
    return removed

# Exact pHash audit: CDN URLs can differ while the downloaded image is the
# same. Keep the flag in the grouping key so one image can still have separate
# review reasons. Near-pHash matches remain review candidates and are never
# deleted automatically here. Build the preview with vectorized pandas work;
# do not convert the entire catalog to Python dictionaries on every rerun.
_duplicate_rule_groups = []
if not _stored_df.empty:
    _dup_view = _stored_df.copy()
    _dup_phash = _dup_view["phash"] if "phash" in _dup_view.columns else pd.Series("", index=_dup_view.index)
    _dup_url = _dup_view["image_url"] if "image_url" in _dup_view.columns else pd.Series("", index=_dup_view.index)
    if "image_url" not in _dup_view.columns:
        _dup_view["image_url"] = ""
    _dup_view["_dedupe_key"] = _dup_phash.fillna("").astype(str).str.strip()
    _dup_view["_dedupe_key"] = _dup_view["_dedupe_key"].mask(
        _dup_view["_dedupe_key"].eq(""), _dup_url.fillna("").astype(str).str.strip()
    )
    _dup_view["_key_type"] = _dup_phash.fillna("").astype(str).str.strip().ne("").map({True: "Exact pHash", False: "Exact URL"})
    _dup_flag = _dup_view["flag"] if "flag" in _dup_view.columns else pd.Series("", index=_dup_view.index)
    _dup_view["_flag_key"] = _dup_flag.fillna("").astype(str)
    _dup_view["_dedupe_key"] = _dup_view["_dedupe_key"].replace({"": pd.NA, "nan": pd.NA})
    _dup_view = _dup_view.dropna(subset=["_dedupe_key"])
    if not _dup_view.empty:
        _dup_counts = (
            _dup_view.groupby(["_dedupe_key", "_flag_key", "_key_type"], dropna=False)
            .agg(Rules=("_dedupe_key", "size"), URLs=("image_url", "nunique"))
            .reset_index()
        )
        _dup_counts = _dup_counts[_dup_counts["Rules"] > 1]
        _duplicate_rule_groups = [
            {"match_type": str(row["_key_type"]), "flag": str(row["_flag_key"]), "count": int(row["Rules"]), "urls": [""] * int(row["URLs"]), "phash": str(row["_dedupe_key"]) if str(row["_key_type"]) == "Exact pHash" else "" }
            for _, row in _dup_counts.iterrows()
        ]
if _duplicate_rule_groups:
    with st.expander(
        f"Image duplicate audit · {sum(max(0, g['count'] - 1) for g in _duplicate_rule_groups):,} redundant rule(s)",
        expanded=False,
    ):
        _dup_preview = pd.DataFrame([
            {
                "Match": group["match_type"],
                "Flag": group["flag"],
                "Rules": group["count"],
                "URLs": len(group["urls"]),
                "pHash": group["phash"] or "—",
            }
            for group in _duplicate_rule_groups[:200]
        ])
        st.caption("These are exact URL or exact pHash duplicates. Different URLs with the same pHash are included.")
        st.dataframe(_dup_preview, hide_index=True, width="stretch")
        _duplicate_total = sum(max(0, group["count"] - 1) for group in _duplicate_rule_groups)
        if st.button(
            f"Remove {_duplicate_total:,} redundant exact duplicates",
            icon=":material/cleaning_services:",
            type="secondary",
            key="dedupe_learned_exact_images",
        ):
            _removed_duplicates = deduplicate_learned_image_rules()
            st.session_state["_learned_import_notice"] = (
                f"Removed {_removed_duplicates:,} redundant exact image rule(s)."
            )
            st.rerun()

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
            "Rule lists",
            "Category learned rules",
            "Settings",
            "Diagnostics",
            "Performance",
            "Backups and recovery",
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
        _db_path = APP_ROOT / "cat_learning.db"
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
                _snapshot_path, _snapshot_cloud_ok = _snapshot_category_learning(_category_engine)
                if r2_storage.enabled() and not _snapshot_cloud_ok:
                    st.error("Category-learning backup did not verify in R2. No corrections were removed.")
                    return
                _removed = _category_engine.delete_corrections(_delete_category_ids)
                st.success(f"Saved category backup {_snapshot_path.name}; removed {_removed:,} correction(s).")
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
                _snapshot_path, _snapshot_cloud_ok = _snapshot_category_learning(_category_engine)
                if r2_storage.enabled() and not _snapshot_cloud_ok:
                    st.error("Category-learning backup did not verify in R2. No decisions were removed.")
                    return
                _removed = _category_engine.delete_negatives(_delete_negative_ids)
                st.success(f"Saved category backup {_snapshot_path.name}; removed {_removed:,} negative decision(s).")
                st.rerun()
    with _category_tabs[2]:
        st.info("Positive corrections teach a product-name pattern to a category. Negative decisions prevent a known wrong category from being suggested again. Removing a row takes effect for new validations after the matcher reloads.", icon=":material/lightbulb:")


def _render_admin_settings_module():
    st.markdown("## :material/settings: Validation settings")
    st.caption("Persistent review preferences and safe cache controls. Learned rule files are never removed by cache actions.")
    st.info("Upload guardrails: 200 MB per file · 1 GB total uncompressed per batch. These limits protect validation from oversized archives.", icon=":material/tune:")
    base_profile, authenticated = _settings_profile()
    _profile_country_options = list(SUPPORTED_RULE_COUNTRIES)
    _profile_country_default = str(st.session_state.get("selected_country", "KE") or "KE").upper()
    _profile_country = st.selectbox(
        "Country profile",
        _profile_country_options,
        index=_profile_country_options.index(_profile_country_default) if _profile_country_default in _profile_country_options else 0,
        key="dashboard_settings_country_profile",
        help="Review preferences are saved separately for each country.",
    )
    profile = f"{base_profile}-{_profile_country}"
    if "_review_preferences_loaded_for" not in st.session_state or st.session_state["_review_preferences_loaded_for"] != profile:
        preferences = _load_review_preferences(profile) or _load_review_preferences(base_profile)
        st.session_state["dashboard_default_page_size"] = int(preferences.get("cards_per_page", 100))
        st.session_state["dashboard_default_image_previews"] = bool(preferences.get("image_previews", False))
        st.session_state["dashboard_show_diagnostics"] = bool(preferences.get("show_diagnostics", False))
        st.session_state["_review_preferences_loaded_for"] = profile
    if not authenticated and base_profile == "cloud-shared":
        st.info("Cloud sign-in is not configured, so these preferences are shared by this deployment. Configure Streamlit authentication for per-user settings.", icon=":material/info:")
        if not r2_storage.enabled():
            st.caption("R2 is not configured; Cloud deployment preferences may reset when the app restarts.")
    else:
        st.caption("Preferences are stored under a hashed reviewer profile; the raw identity is not written to storage.")

    with st.form("dashboard_review_preferences"):
        st.markdown("### Review defaults")
        st.selectbox("Default cards per page", [50, 100, 200, 500], key="dashboard_default_page_size")
        st.toggle("Open image previews by default", key="dashboard_default_image_previews")
        st.toggle("Expand validation diagnostics by default", key="dashboard_show_diagnostics")
        _save_preferences = st.form_submit_button("Save preferences", icon=":material/save:", type="primary")
    if _save_preferences:
        preferences = {
            "cards_per_page": int(st.session_state.dashboard_default_page_size),
            "image_previews": bool(st.session_state.dashboard_default_image_previews),
            "show_diagnostics": bool(st.session_state.dashboard_show_diagnostics),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        local_ok, cloud_ok = _save_review_preferences(profile, preferences)
        if local_ok or cloud_ok:
            st.session_state.grid_items_per_page = preferences["cards_per_page"]
            st.session_state.grid_ipp_slider = preferences["cards_per_page"]
            st.session_state.show_images = preferences["image_previews"]
            if local_ok and (cloud_ok or not r2_storage.enabled()):
                st.success("Preferences saved.")
            else:
                st.warning("Preferences were saved locally, but Cloudflare R2 did not confirm the cloud copy.")
        else:
            st.error("Preferences could not be saved. Check local write permissions and R2 configuration.")

    st.markdown("### Data locations")
    _data_cols = st.columns(2, gap="medium")
    _data_cols[0].caption("Learned image rules · authoritative JSON")
    _data_cols[0].code(str(APP_ROOT / "learned_image_rules.json"), language="text")
    _data_cols[1].caption("Category learning · SQLite locally, Supabase when configured")
    _data_cols[1].code(str(APP_ROOT / "cat_learning.db"), language="text")

    st.markdown("### Cache controls")
    st.caption("This clears only derived image dimensions, hashes, and thumbnails. Learned rules and category learning remain untouched.")
    if st.button("Clear derived image cache", icon=":material/delete_sweep:", key="dashboard_clear_derived_image_cache"):
        clear_derived_image_cache()
        st.success("Derived image cache cleared. Learned rules were not changed.")


def _render_admin_diagnostics_module():
    st.markdown("## :material/monitoring: Diagnostics")
    st.caption("Runtime health and timing information for the current session.")
    _timings = st.session_state.get("validation_stage_timings", {})
    if _timings:
        st.dataframe(pd.DataFrame([{"Stage": key, "Seconds": value} for key, value in _timings.items()]).sort_values("Seconds", ascending=False), hide_index=True, width="stretch")
    else:
        st.info("No validation timings are available in this session yet.")
    _rules_path = APP_ROOT / "learned_image_rules.json"
    _category_path = APP_ROOT / "cat_learning.db"
    _health = pd.DataFrame([
        {"Component": "Learned image rules", "Status": "Available" if _rules_path.exists() else "Missing", "Size": f"{_rules_path.stat().st_size / 1024 / 1024:.1f} MB" if _rules_path.exists() else "—"},
        {"Component": "Category learning", "Status": "Available" if _category_path.exists() else "Missing", "Size": f"{_category_path.stat().st_size / 1024 / 1024:.1f} MB" if _category_path.exists() else "—"},
    ])
    st.dataframe(_health, hide_index=True, width="stretch")


def _render_admin_recovery_module():
    st.markdown("## :material/backup: Backups and recovery")
    st.caption("Image-rule and category-learning snapshots include SHA-256 verification and retain the newest 100 copies locally and in R2 when configured.")
    st.caption("Export a point-in-time copy before bulk edits. Restoring is explicit and does not happen during ordinary validation.")
    _rules_path = APP_ROOT / "learned_image_rules.json"
    _rules_payload = _rules_path.read_bytes() if _rules_path.exists() else b"[]"
    _backup_cols = st.columns(3, gap="medium")
    _backup_cols[0].metric("Learned rules", f"{len(_stored_df):,}")
    _backup_cols[1].metric("File size", f"{len(_rules_payload) / 1024 / 1024:.1f} MB")
    _backup_cols[2].metric("Last modified", datetime.fromtimestamp(_rules_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M") if _rules_path.exists() else "Unavailable")
    st.download_button(
        "Download learned-rules backup",
        data=_rules_payload,
        file_name=f"learned_image_rules_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        mime="application/json",
        icon=":material/download:",
        type="primary",
        key="dashboard_download_rules_backup",
        disabled=not _rules_path.exists(),
    )
    st.markdown("### Category learning backup")
    try:
        from category_matcher_engine import CategoryMatcherEngine
        _category_db_path = APP_ROOT / "cat_learning.db"
        _category_engine = CategoryMatcherEngine(str(_category_db_path))
        _category_backup = _category_learning_payload(_category_engine)
        _category_backup_bytes = json.dumps(_category_backup, ensure_ascii=False, indent=2).encode("utf-8")
        _category_digest = hashlib.sha256(_category_backup_bytes).hexdigest()
        _category_backup_cols = st.columns(2, gap="medium")
        _category_backup_cols[0].download_button(
            "Download category-learning backup",
            data=_category_backup_bytes,
            file_name=f"category_learning_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            icon=":material/download:",
            key="dashboard_download_category_backup",
        )
        if _category_backup_cols[1].button("Create verified category snapshot", icon=":material/cloud_upload:", key="dashboard_create_category_snapshot"):
            _snapshot_path, _snapshot_cloud_ok = _snapshot_category_learning(_category_engine)
            if r2_storage.enabled() and not _snapshot_cloud_ok:
                st.error("The snapshot was created locally, but R2 verification failed.")
            elif r2_storage.enabled():
                st.success(f"Category snapshot saved and verified in R2: {_snapshot_path.name}")
            else:
                st.success(f"Category snapshot saved locally: {_snapshot_path.name}. Configure R2 for off-device recovery.")
        st.caption(
            f"{len(_category_backup['corrections']):,} corrections · {len(_category_backup['negatives']):,} negative decisions · "
            f"SHA-256 { _category_digest[:16] }…"
        )
        _category_snapshot_objects = r2_storage.list_objects("learned-rules/category-snapshots/", limit=100)
        _category_snapshot_dir = APP_ROOT / ".learned_rule_snapshots"
        _category_snapshot_choices = [f"r2::{item['key']}" for item in _category_snapshot_objects]
        if _category_snapshot_dir.exists():
            _category_snapshot_choices.extend(
                f"local::{path.name}"
                for path in sorted(_category_snapshot_dir.glob("category-learning-*.json.gz"), key=lambda item: item.stat().st_mtime, reverse=True)[:100]
            )
        if _category_snapshot_choices:
            _category_snapshot_choice = st.selectbox(
                "Category-learning snapshots",
                _category_snapshot_choices,
                format_func=lambda value: value.split("::", 1)[-1],
                key="dashboard_category_snapshot_choice",
            )
            if st.button("Preview category snapshot", icon=":material/visibility:", key="dashboard_preview_category_snapshot"):
                try:
                    _snapshot_compressed = (
                        r2_storage.download_bytes(_category_snapshot_choice.split("::", 1)[1])
                        if _category_snapshot_choice.startswith("r2::")
                        else (_category_snapshot_dir / _category_snapshot_choice.split("::", 1)[1]).read_bytes()
                    )
                    _snapshot_raw = gzip.decompress(_snapshot_compressed)
                    _snapshot_digest = hashlib.sha256(_snapshot_raw).hexdigest()
                    _snapshot_expected = Path(_category_snapshot_choice.split("::", 1)[-1]).name.rsplit("-", 1)[-1].replace(".json.gz", "")
                    if _snapshot_expected and not _snapshot_digest.startswith(_snapshot_expected):
                        raise ValueError("Snapshot SHA-256 does not match its filename.")
                    _snapshot_payload = json.loads(_snapshot_raw.decode("utf-8"))
                    if not isinstance(_snapshot_payload, dict) or _snapshot_payload.get("kind") != "category_learning_backup":
                        raise ValueError("This is not a category-learning snapshot.")
                    if not isinstance(_snapshot_payload.get("corrections"), list) or not isinstance(_snapshot_payload.get("negatives"), list):
                        raise ValueError("Snapshot correction and negative-decision rows are invalid.")
                    _snapshot_content_digest = hashlib.sha256(json.dumps(
                        {"corrections": _snapshot_payload["corrections"], "negatives": _snapshot_payload["negatives"]},
                        sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                    ).encode("utf-8")).hexdigest()
                    if _snapshot_payload.get("content_sha256") and _snapshot_payload["content_sha256"] != _snapshot_content_digest:
                        raise ValueError("Category snapshot content checksum does not match.")
                    st.session_state["_category_snapshot_preview"] = {
                        "choice": _category_snapshot_choice,
                        "compressed": _snapshot_compressed,
                        "sha256": _snapshot_digest,
                        "corrections": len(_snapshot_payload.get("corrections", [])),
                        "negatives": len(_snapshot_payload.get("negatives", [])),
                    }
                    del _snapshot_payload, _snapshot_raw
                    st.rerun()
                except Exception as _category_preview_error:
                    st.session_state.pop("_category_snapshot_preview", None)
                    st.error(f"Category snapshot preview failed: {_category_preview_error}")
            _category_snapshot_preview = st.session_state.get("_category_snapshot_preview", {})
            if _category_snapshot_preview.get("choice") == _category_snapshot_choice:
                st.info(
                    f"Verified snapshot · {_category_snapshot_preview['corrections']:,} corrections · "
                    f"{_category_snapshot_preview['negatives']:,} negative decisions · "
                    f"SHA-256 {_category_snapshot_preview['sha256'][:16]}…"
                )
                if st.button("Prepare this snapshot for merge", icon=":material/restore:", key="dashboard_prepare_category_snapshot_merge"):
                    st.session_state["_category_restore_bytes_override"] = gzip.decompress(_category_snapshot_preview["compressed"])
                    st.session_state.pop("_category_snapshot_preview", None)
                    st.rerun()
        _category_restore = st.file_uploader(
            "Merge a category-learning backup",
            type=["json"],
            key="dashboard_restore_category_backup",
        )
        _category_restore_override = st.session_state.pop("_category_restore_bytes_override", None)
        if _category_restore is not None or _category_restore_override is not None:
            try:
                _category_restore_bytes = _category_restore.getvalue() if _category_restore is not None else _category_restore_override
                _category_restore_payload = json.loads(_category_restore_bytes.decode("utf-8-sig"))
                if not isinstance(_category_restore_payload, dict) or _category_restore_payload.get("kind") != "category_learning_backup":
                    raise ValueError("Choose a category-learning backup created by this Dashboard.")
                _incoming_corrections = _category_restore_payload.get("corrections", [])
                _incoming_negatives = _category_restore_payload.get("negatives", [])
                if not isinstance(_incoming_corrections, list) or not isinstance(_incoming_negatives, list):
                    raise ValueError("The backup has invalid correction or negative rows.")
                _incoming_content_digest = hashlib.sha256(json.dumps(
                    {"corrections": _incoming_corrections, "negatives": _incoming_negatives},
                    sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                ).encode("utf-8")).hexdigest()
                if _category_restore_payload.get("content_sha256") and _category_restore_payload["content_sha256"] != _incoming_content_digest:
                    raise ValueError("Category backup content checksum does not match.")
                _existing_corrections_frame = _category_engine.list_corrections(limit=200000)
                _existing_corrections = set(zip(
                    _existing_corrections_frame.get("name", pd.Series(dtype=str)).astype(str).str.casefold(),
                    _existing_corrections_frame.get("category", pd.Series(dtype=str)).astype(str).str.casefold(),
                ))
                _existing_negatives_frame = _category_engine.list_negatives(limit=200000)
                _existing_negatives = set(zip(
                    _existing_negatives_frame.get("name", pd.Series(dtype=str)).astype(str).str.casefold(),
                    _existing_negatives_frame.get("category", pd.Series(dtype=str)).astype(str).str.casefold(),
                    _existing_negatives_frame.get("reason", pd.Series(dtype=str)).astype(str).str.casefold(),
                ))
                _new_corrections = [
                    row for row in _incoming_corrections
                    if isinstance(row, dict)
                    and str(row.get("name", "")).strip()
                    and str(row.get("category", "")).strip()
                    and (str(row.get("name", "")).strip().casefold(), str(row.get("category", "")).strip().casefold()) not in _existing_corrections
                ]
                _new_negatives = [
                    row for row in _incoming_negatives
                    if isinstance(row, dict)
                    and str(row.get("name", "")).strip()
                    and str(row.get("category", "")).strip()
                    and (str(row.get("name", "")).strip().casefold(), str(row.get("category", "")).strip().casefold(), str(row.get("reason", "")).strip().casefold()) not in _existing_negatives
                ]
                st.caption(f"Restore preview · {len(_new_corrections):,} new corrections · {len(_new_negatives):,} new negative decisions.")
                if st.button("Merge category-learning backup", icon=":material/restore:", key="dashboard_merge_category_backup"):
                    _snapshot_path, _snapshot_cloud_ok = _snapshot_category_learning(_category_engine)
                    if r2_storage.enabled() and not _snapshot_cloud_ok:
                        st.error("A verified R2 backup could not be made; no category rows were restored.")
                    else:
                        with _category_engine._connect() as _conn:
                            _conn.executemany(
                                "INSERT INTO category_corrections (name, category) VALUES (?, ?)",
                                [(str(row["name"]).strip(), str(row["category"]).strip()) for row in _new_corrections],
                            )
                            _conn.executemany(
                                "INSERT INTO category_negatives (name, category, reason) VALUES (?, ?, ?)",
                                [(str(row["name"]).strip(), str(row["category"]).strip(), str(row.get("reason", "")).strip()) for row in _new_negatives],
                            )
                            _conn.commit()
                        if supabase_store.enabled():
                            try:
                                if _new_corrections:
                                    supabase_store.insert("category_corrections", [
                                        {"name": str(row["name"]).strip(), "category": str(row["category"]).strip(), "learned_at": supabase_store.now_iso()}
                                        for row in _new_corrections
                                    ])
                                if _new_negatives:
                                    supabase_store.insert("category_negatives", [
                                        {"name": str(row["name"]).strip(), "category": str(row["category"]).strip(), "reason": str(row.get("reason", "")).strip(), "learned_at": supabase_store.now_iso()}
                                        for row in _new_negatives
                                    ])
                            except Exception as _category_sync_error:
                                st.warning(f"Rows were restored locally, but category data did not fully sync to Supabase: {_category_sync_error}")
                        _category_engine.load_learning_db()
                        st.success(f"Restored {len(_new_corrections):,} correction(s) and {len(_new_negatives):,} negative decision(s); existing rows were skipped.")
                        st.rerun()
            except Exception as _category_restore_error:
                st.error(f"Could not read that category backup: {_category_restore_error}")
    except Exception as _category_backup_error:
        st.warning(f"Category-learning backup is unavailable: {_category_backup_error}")

    _restore_upload = st.file_uploader(
        "Restore or merge from a learned-rules JSON backup",
        type=["json"],
        key="dashboard_restore_rules_json",
    )
    if _restore_upload is not None:
        if st.button("Merge backup into current catalog", icon=":material/restore:", key="dashboard_merge_rules_backup"):
            try:
                _backup_rows = json.loads(_restore_upload.getvalue().decode("utf-8-sig"))
                if not isinstance(_backup_rows, list) or any(not isinstance(row, dict) for row in _backup_rows):
                    raise ValueError("Expected a JSON array of learned-rule records.")
                _restored = restore_learned_image_rules(_backup_rows)
                st.success(f"Restored {_restored:,} rule(s); existing rules and other reasons were preserved.")
                st.rerun()
            except Exception as _restore_error:
                st.error(f"Could not restore that backup: {_restore_error}")
    _snapshot_objects = r2_storage.list_objects("learned-rules/snapshots/", limit=100)
    _snapshot_dir = APP_ROOT / ".learned_rule_snapshots"
    _snapshot_choices = [f"r2::{row['key']}" for row in _snapshot_objects]
    if _snapshot_dir.exists():
        _snapshot_choices.extend(
            f"local::{path.name}"
            for path in sorted(_snapshot_dir.glob("*.json.gz"), key=lambda path: path.stat().st_mtime, reverse=True)[:100]
        )
    if _snapshot_choices:
        _snapshot_choice = st.selectbox(
            "Catalog snapshots (created before imports)",
            _snapshot_choices,
            format_func=lambda value: value.split("::", 1)[-1],
            key="dashboard_r2_snapshot_choice",
        )
        _replace_ack = st.checkbox(
            "I understand this replaces the current catalog. The current version will be archived first.",
            key="dashboard_replace_snapshot_ack",
        )
        _preview_key = st.session_state.get("_dashboard_snapshot_preview", {})
        _snapshot_preview = _preview_key if _preview_key.get("choice") == _snapshot_choice else None
        if st.button("Preview selected snapshot", icon=":material/visibility:", key="dashboard_preview_r2_snapshot"):
            try:
                if _snapshot_choice.startswith("r2::"):
                    _preview_bytes = r2_storage.download_bytes(_snapshot_choice.split("::", 1)[1])
                else:
                    _preview_bytes = (_snapshot_dir / _snapshot_choice.split("::", 1)[1]).read_bytes()
                if not _preview_bytes:
                    raise RuntimeError("The selected snapshot could not be read.")
                _preview_raw = gzip.decompress(_preview_bytes)
                _preview_digest = hashlib.sha256(_preview_raw).hexdigest()
                _expected_digest = Path(_snapshot_choice.split("::", 1)[-1]).name.rsplit("-", 1)[-1].replace(".json.gz", "")
                if _expected_digest and len(_expected_digest) == 12 and not _preview_digest.startswith(_expected_digest):
                    raise ValueError("Snapshot checksum does not match its filename.")
                _preview_rows = json.loads(_preview_raw.decode("utf-8"))
                if not isinstance(_preview_rows, list) or any(not isinstance(row, dict) for row in _preview_rows):
                    raise ValueError("The snapshot is not a learned-rule JSON catalog.")
                st.session_state["_dashboard_snapshot_preview"] = {
                    "choice": _snapshot_choice,
                    "compressed_payload": _preview_bytes,
                    "row_count": len(_preview_rows),
                    "sha256": _preview_digest,
                    "compressed_bytes": len(_preview_bytes),
                }
                del _preview_rows, _preview_raw
                st.rerun()
            except Exception as _preview_error:
                st.session_state.pop("_dashboard_snapshot_preview", None)
                st.error(f"Snapshot preview failed: {_preview_error}")
        if _snapshot_preview:
            st.info(
                f"Preview verified · {_snapshot_preview['compressed_bytes'] / 1024 / 1024:.1f} MB compressed · "
                f"{_snapshot_preview['row_count']:,} rules · SHA-256 {_snapshot_preview['sha256'][:16]}…"
            )
        if st.button(
            "Restore selected snapshot as the active catalog",
            icon=":material/history:",
            key="dashboard_restore_r2_snapshot",
            disabled=not _replace_ack or not _snapshot_preview,
        ):
            try:
                _snapshot_raw = gzip.decompress(_snapshot_preview["compressed_payload"])
                if not hashlib.sha256(_snapshot_raw).hexdigest().startswith(_snapshot_preview["sha256"]):
                    raise ValueError("Snapshot checksum changed after preview. Preview it again before restoring.")
                _snapshot_rows = json.loads(_snapshot_raw.decode("utf-8"))
                _replaced_count = replace_learned_image_rules(_snapshot_rows, confirmed=True)
                st.session_state.pop("_dashboard_snapshot_preview", None)
                st.success(f"Restored {_replaced_count:,} rules. The catalog that was active before restore is in removal history.")
                st.rerun()
            except Exception as _snapshot_error:
                st.error(f"Could not restore snapshot: {_snapshot_error}")
    if DELETED_RULES_ARCHIVE.exists():
        _archived = []
        try:
            with DELETED_RULES_ARCHIVE.open("r", encoding="utf-8") as _archive_file:
                for _line in _archive_file:
                    try:
                        _entry = json.loads(_line)
                        _record = _entry.get("rule") if isinstance(_entry, dict) else None
                        if isinstance(_record, dict):
                            _record = dict(_record)
                            _record["_deleted_at"] = str(_entry.get("deleted_at", ""))
                            _record["_archive_id"] = hashlib.sha256(
                                "\x1f".join(str(_record.get(_key, "")) for _key in
                                    ("_deleted_at", "image_url", "phash", "flag", "created_at")).encode("utf-8")
                            ).hexdigest()[:16]
                            _archived.append(_record)
                    except (ValueError, TypeError):
                        continue
        except OSError as _archive_error:
            st.warning(f"Could not read the removal history: {_archive_error}")
        if _archived:
            with st.expander(f"Removal history and restore · {len(_archived):,} archived record(s)", expanded=False):
                _archive_frame = pd.DataFrame(_archived)
                _archive_frame.insert(0, "Restore", False)
                _archive_cols = [c for c in ("Restore", "_archive_id", "_deleted_at", "flag", "brand_infringed", "image_url", "phash", "countries", "product_set_sid", "seller_name") if c in _archive_frame.columns]
                _archive_selection = st.data_editor(
                    _archive_frame[_archive_cols].tail(500),
                    hide_index=True,
                    width="stretch",
                    disabled=[c for c in _archive_cols if c != "Restore"],
                    key="dashboard_archived_rule_restore_table",
                    column_config={"Restore": st.column_config.CheckboxColumn("Restore", help="Select records to restore")},
                )
                if st.button("Restore selected archived rules", icon=":material/restore:", key="dashboard_restore_archived_selection"):
                    _restore_mask = _archive_selection["Restore"].fillna(False).astype(bool)
                    _restore_ids = set(_archive_selection.loc[_restore_mask, "_archive_id"].astype(str))
                    _restore_records = [
                        {k: v for k, v in row.items() if k not in {"_deleted_at", "_archive_id"}}
                        for row in _archived if str(row.get("_archive_id", "")) in _restore_ids
                    ]
                    _restored_count = restore_learned_image_rules(_restore_records)
                    st.success(f"Restored {_restored_count:,} archived rule(s).")
                    st.rerun()
    st.info("Restores merge duplicate-safely. They never replace the live catalog; the last ten dashboard removals also have an Undo action.", icon=":material/info:")


if _admin_module == "Rule lists":
    render_rule_lists_admin_module()
    st.stop()
if _admin_module == "Category learned rules":
    _render_category_admin_module()
    st.stop()
if _admin_module == "Settings":
    _render_admin_settings_module()
    st.stop()
if _admin_module == "Diagnostics":
    _render_admin_diagnostics_module()
    st.stop()
if _admin_module == "Performance":
    _render_admin_diagnostics_module()
    st.stop()
if _admin_module == "Backups and recovery":
    _render_admin_recovery_module()
    st.stop()

# ── Modern dashboard shell ─────────────────────────────────────────────────
st.markdown(
    """
    <style>
    [data-testid="stAppViewContainer"] { background:#f6f8fb; }
    [data-testid="stSidebar"] { background:#101827; border-right:1px solid #243247; }
    [data-testid="stSidebar"] * { color:#dbe5f0; }
    [data-testid="stSidebar"] label { color:#94a9c2 !important; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.08em; }
    [data-testid="stSidebar"] [role="radio"] { padding:9px 10px; border-radius:9px; }
    [data-testid="stSidebar"] [role="radio"]:hover { background:#1e2b40; }
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
        background:#fff; box-shadow:0 5px 18px rgba(15,23,42,.06); transition:transform .18s,box-shadow .18s; }
    .lr-card:hover { transform:translateY(-2px); box-shadow:0 12px 28px rgba(15,23,42,.11); }
    .lr-card-head { display:flex; align-items:center; justify-content:space-between; gap:10px; }
    .lr-card-icon { width:32px; height:32px; display:grid; place-items:center; border-radius:10px; background:#eff6ff; color:#2563eb; font-size:18px; }
    .lr-card-label { color:#64748b; font-size:11px; font-weight:800; text-transform:uppercase; letter-spacing:.06em; }
    .lr-card-value { color:#0f172a; font-size:26px; font-weight:800; margin-top:7px; letter-spacing:-.03em; }
    .lr-card-note { color:#64748b; font-size:11px; margin-top:5px; }
    .lr-section { margin:24px 0 10px; color:#0f172a; font-size:16px; font-weight:800; letter-spacing:-.01em; display:flex; align-items:center; gap:8px; }
    .lr-section:before { content:''; width:4px; height:18px; border-radius:4px; background:#f28c28; }
    div[data-testid="stExpander"] { border-color:#e2e8f0; border-radius:12px; background:rgba(255,255,255,.72); }
    div[data-testid="stMetric"] { background:#fff; border:1px solid #e2e8f0; border-radius:12px; padding:12px 14px; }
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
            "Include an image-link column and **FLAG**. **BRAND_INFRINGED** is optional and is not needed for NSFW image rules. FDA remains category-validator owned."
        )
        st.dataframe(
            pd.DataFrame([
                {"PRODUCT_SET_SID": "optional", "IMAGE_URL": "required", "FLAG": "required", "REASON": "optional detail", "BRAND_INFRINGED": "optional", "SELLER_NAME": "optional", "CATEGORY_CODE": "optional", "STATUS": "optional"},
                {"PRODUCT_SET_SID": "not needed", "IMAGE_URL": "https://example.com/nsfw-image.jpg", "FLAG": "NSFW Images", "REASON": "optional", "BRAND_INFRINGED": "not needed", "SELLER_NAME": "optional", "CATEGORY_CODE": "optional", "STATUS": "optional"},
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
        type=["xlsx", "xls", "csv", "zip", "rar"],
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
for _col, (_label, _value, _note, _icon) in zip(
    _dashboard_cols,
    (("Active rules", _dashboard_active, "Applied during validation", "verified"),
     ("Awaiting review", _dashboard_unreviewed, "Need a reviewer decision", "rate_review"),
     ("Confirmed", _dashboard_confirmed, "Validated as reliable", "task_alt"),
     ("Corrected", _dashboard_corrected, "Candidates for removal", "edit_note")),
):
    with _col:
        st.markdown(f'<div class="lr-card"><div class="lr-card-head"><div class="lr-card-label">{_label}</div><div class="lr-card-icon"><span class="material-symbols-outlined">{_icon}</span></div></div><div class="lr-card-value">{_value:,}</div><div class="lr-card-note">{_note}</div></div>', unsafe_allow_html=True)
_progress_cols = st.columns(2, gap="large")
with _progress_cols[0]:
    st.caption(f":material/task_alt: Review coverage · {_dashboard_reviewed:,} of {len(_stored_df):,}")
    st.progress(_dashboard_review_pct, text=f"{_dashboard_review_pct:.0%} reviewed")
with _progress_cols[1]:
    st.caption(f":material/shield: Active rule coverage · {_dashboard_active:,} of {len(_stored_df):,}")
    st.progress(_dashboard_active_pct, text=f"{_dashboard_active_pct:.0%} active")
_dashboard_flags = _stored_df.get("flag", pd.Series(dtype=str)).astype(str).value_counts().head(6)
if not _dashboard_flags.empty and st.toggle(":material/analytics: Show rule distribution", key="dashboard_show_rule_distribution"):
    _distribution_cols = st.columns(min(3, len(_dashboard_flags)), gap="small")
    for _distribution_index, (_distribution_flag, _distribution_count) in enumerate(_dashboard_flags.items()):
        with _distribution_cols[_distribution_index % len(_distribution_cols)]:
            st.metric(str(_distribution_flag), f"{int(_distribution_count):,}")

if not _stored_df.empty and st.toggle(":material/insights: Show learned data insights", key="dashboard_show_learned_insights"):
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
        _view_search = _search_col.text_input(
            "Search rule ID, URL, pHash, SKU, reason, brand, seller, or source",
            key="learned_rules_view_search",
            placeholder="LR-0000000000, pHash, SKU, NIVEA, Poor images…",
        )
        _brand_series = _stored_df["brand_infringed"].fillna("").astype(str).str.strip()
        _brand_options = ["All"] + sorted([_brand for _brand in _brand_series.unique().tolist() if _brand])
        _view_brand = st.selectbox("Filter by infringed/claimed brand", _brand_options, key="learned_rules_view_brand")
        _source_options = ["All"] + sorted(_stored_df.get("source", pd.Series(dtype=str)).dropna().astype(str).unique().tolist())
        _view_source = st.selectbox("Filter by source", _source_options, key="learned_rules_view_source")
        if "review_state" not in _stored_df.columns:
            _stored_df["review_state"] = "Unreviewed"
        _review_options = ["All", "Unreviewed", "Confirmed", "Corrected", "Removed"]
        _view_review = st.selectbox("Review state", _review_options, key="learned_rules_view_review")
        _country_filter_options = ["All"] + list(SUPPORTED_RULE_COUNTRIES)
        _view_country = st.selectbox("Market scope", _country_filter_options, key="learned_rules_view_country")
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
                    _removed_corrected = _delete_rules_with_undo(_correction_queue.to_dict("records"))
                    st.success(f"Removed {_removed_corrected:,} corrected rule(s).")
                    st.rerun()
        _view_df = _stored_df.copy()
        if _view_flag != "All" and "flag" in _view_df.columns:
            _view_df = _view_df[_view_df["flag"].astype(str).eq(_view_flag)]
        if _view_search:
            _needle = _view_search.casefold()
            _search_cols = [c for c in ("_rule_id", "image_url", "phash", "product_set_sid", "brand", "brand_infringed", "seller_name", "source", "flag", "reason", "comment", "category_code") if c in _view_df.columns]
            _mask = _view_df[_search_cols].astype(str).apply(lambda col: col.str.casefold().str.contains(_needle, na=False)).any(axis=1)
            _view_df = _view_df[_mask]
        if _view_source != "All" and "source" in _view_df.columns:
            _view_df = _view_df[_view_df["source"].astype(str).eq(_view_source)]
        if _view_brand != "All" and "brand_infringed" in _view_df.columns:
            _view_df = _view_df[_view_df["brand_infringed"].fillna("").astype(str).str.strip().eq(_view_brand)]
        if _view_review != "All":
            _view_df = _view_df[_view_df["review_state"].fillna("Unreviewed").astype(str).eq(_view_review)]
        if _view_country != "All":
            _view_df = _view_df[_view_df["countries"].map(
                lambda values: "*" in normalize_rule_countries(values) or _view_country in normalize_rule_countries(values)
            )]
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
        _display_cols = [c for c in ("_rule_id", "image_url", "phash", "flag", "reason", "brand_infringed", "brand", "countries", "scope_categories", "scope_brands", "seller_name", "product_set_sid", "source", "status", "created_at", "last_matched_at", "last_confirmed_at", "confidence", "cluster_size", "skus_affected", "sellers_affected", "lifecycle", "review_due_at") if c in _view_df.columns]
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
                _audit_selected_records = []
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
                            _audit_rule_id = str(_audit_row.get("_rule_id", ""))
                            _audit_sid = str(_audit_row.get("product_set_sid", "")).strip()
                            _audit_seller = str(_audit_row.get("seller_name", "")).strip()
                            _audit_source = str(_audit_row.get("source", "")).strip()
                            _audit_reason_text = str(_audit_row.get("reason", "") or "").strip()
                            _audit_brand = str(_audit_row.get("brand_infringed", _audit_row.get("brand", "")) or "").strip()
                            _audit_category = str(_audit_row.get("category_code", "") or "").strip()
                            _audit_review = str(_audit_row.get("review_state", "Unreviewed") or "Unreviewed")
                            _audit_countries = normalize_rule_countries(_audit_row.get("countries"), legacy_global=True)
                            _audit_image_key = _audit_url or str(_audit_row.get("phash", "")).strip()
                            _audit_matches = _stored_df[
                                _stored_df.get("image_url", pd.Series(index=_stored_df.index, dtype=str)).astype(str).eq(_audit_image_key)
                            ] if _audit_url else _stored_df[
                                _stored_df.get("phash", pd.Series(index=_stored_df.index, dtype=str)).astype(str).eq(_audit_image_key)
                            ]
                            _audit_seller_count = _audit_matches.get("seller_name", pd.Series(dtype=str)).astype(str).replace({"": pd.NA}).nunique(dropna=True)
                            st.markdown(f"**{_audit_rule_id or 'Rule ID unavailable'}** · **{_audit_flag}**")
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
                                f"Markets: {'All countries' if '*' in _audit_countries else ', '.join(_audit_countries) or 'None'}\n"
                                f"Review: {_audit_review}\n"
                                f"Affected SKUs: {int(_audit_row.get('skus_affected', 0) or 0):,} · Sellers: {int(_audit_row.get('sellers_affected', _audit_seller_count) or 0):,}"
                            )
                            # Timestamps are not guaranteed to be unique when a report
                            # imports several rules at once. Include the row identity so
                            # every Streamlit widget receives a stable unique key.
                            _audit_key = f"{_audit_start + _audit_offset}_{_audit_idx}_{str(_audit_row.get('created_at', '')).replace('-', '')[-16:]}"
                            if st.checkbox("Select for removal", key=f"audit_select_{_audit_key}", label_visibility="collapsed"):
                                _audit_selected_records.append(_audit_row.to_dict())
                            _audit_new_flag = st.selectbox(
                                "Reason",
                                sorted(LEARNABLE_FLAGS),
                                index=(sorted(LEARNABLE_FLAGS).index(_audit_flag) if _audit_flag in LEARNABLE_FLAGS else 0),
                                key=f"audit_reason_{_audit_key}",
                            )
                            _scope_options = ["All countries", *SUPPORTED_RULE_COUNTRIES]
                            _scope_default = ["All countries"] if "*" in _audit_countries else _audit_countries
                            _scope_choice = st.multiselect(
                                "Applies in",
                                _scope_options,
                                default=_scope_default,
                                key=f"audit_countries_{_audit_key}",
                            )
                            _scope_category_values = normalize_rule_values(_audit_row.get("scope_categories"))
                            _scope_brand_values = normalize_rule_values(_audit_row.get("scope_brands"))
                            _scope_category_text = st.text_input(
                                "Category scope (optional; comma-separated codes or path terms)",
                                value=", ".join(_scope_category_values),
                                key=f"audit_category_scope_{_audit_key}",
                            )
                            _scope_brand_text = st.text_input(
                                "Brand scope (optional; comma-separated declared brands)",
                                value=", ".join(_scope_brand_values),
                                key=f"audit_brand_scope_{_audit_key}",
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
                            _scope_to_save = ["*"] if "All countries" in _scope_choice else _scope_choice
                            if st.button("Save market scope", icon=":material/public:", key=f"audit_scope_save_{_audit_key}", width="stretch"):
                                if update_learned_image_rule_countries(_audit_row.to_dict(), _scope_to_save):
                                    st.success("Market scope saved.")
                                    st.rerun()
                            if st.button("Save category/brand scope", icon=":material/tune:", key=f"audit_detail_scope_save_{_audit_key}", width="stretch"):
                                if update_learned_image_rule_scope(
                                    _audit_row.to_dict(),
                                    categories=_scope_category_text,
                                    brands=_scope_brand_text,
                                ):
                                    st.success("Category and brand scope saved.")
                                    st.rerun()
                            with _a1:
                                if st.button("Remove", icon=":material/delete:", key=f"audit_remove_{_audit_key}", width="stretch"):
                                    if _delete_rules_with_undo([_audit_row.to_dict()]):
                                        st.success("Removed")
                                        st.rerun()
                            st.divider()
                if _audit_selected_records:
                    _audit_bulk_a, _audit_bulk_b = st.columns([2, 3], gap="small")
                    with _audit_bulk_a:
                        if st.button("Remove selected audit rules", icon=":material/delete_sweep:", type="secondary", key="remove_selected_audit_rules", width="stretch"):
                            _audit_removed = _delete_rules_with_undo(_audit_selected_records)
                            st.success(f"Removed {_audit_removed:,} learned rule(s).")
                            st.rerun()
                    with _audit_bulk_b:
                        st.caption(f":material/checklist: {len(_audit_selected_records):,} audit card(s) selected")
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
                    _preview_rule_id = str(_preview_row.get("_rule_id", ""))
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
                        st.markdown(f"**{_preview_rule_id or 'Rule ID unavailable'}** · **{_preview_flag}**")
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
                                if _delete_rules_with_undo([_preview_row.to_dict()]):
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
                            _removed_preview = _delete_rules_with_undo(_preview_selected_records)
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
            column_config={
                "Remove": st.column_config.CheckboxColumn("Remove", help="Delete this rule", default=False),
                "_rule_id": st.column_config.TextColumn("Rule ID", help="Stable identifier used to search and remove this rule."),
            },
            key="learned_rules_editor",
        )
        _selected = _edited[_edited["Remove"].astype(bool)] if "Remove" in _edited.columns else pd.DataFrame()
        if st.button("Delete selected rules", icon=":material/delete_sweep:", type="secondary", disabled=_selected.empty, key="delete_selected_learned_rules"):
            _records = _selected.drop(columns=["Remove"], errors="ignore").to_dict("records")
            _deleted = _delete_rules_with_undo(_records)
            st.success(f"Deleted {_deleted:,} learned rule(s).")
            st.rerun()
        st.download_button(
            "Download filtered rules (compressed CSV)",
            data=lambda _frame=_view_df, _columns=_display_cols: _make_compressed_csv_download(_frame, _columns),
            file_name="learned_image_rules.csv.gz",
            mime="application/gzip",
            key="download_learned_rules_csv",
        )


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [str(c).strip().upper().replace(" ", "_") for c in frame.columns]
    return frame


@st.cache_data(show_spinner=False, max_entries=24)
def _read_report_cached(source_name: str, source_bytes: bytes, _parser_version: str = "v4") -> pd.DataFrame:
    """Cache parsed report frames so reruns do not reopen the same workbook."""
    _required = {
        "PRODUCT_SET_SID", "PRODUCTSETSID", "PRODUCT_SET_SID_", "SID",
        "MAIN_IMAGE", "IMAGE1", "IMAGE", "IMAGE_URL", "MAIN_IMAGE_URL",
        "IMAGE_LINK", "IMAGE_LINKS", "PRODUCT_IMAGE", "PRODUCT_IMAGE_URL", "URL", "LINK",
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
    path = APP_ROOT / "Image_learn_cache.json"
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


def _infer_source_country(source_name):
    """Infer KE/UG provenance from report/archive names when no column exists."""
    tokens = re.findall(r"(?<![A-Z0-9])(KENYA|UGANDA|KE|UG)(?![A-Z0-9])", str(source_name or "").upper())
    codes = ["KE" if token in {"KENYA", "KE"} else "UG" for token in tokens]
    # An archive and its member should normally repeat the same country. If
    # their names disagree, leave provenance blank instead of guessing.
    return codes[-1] if codes and len(set(codes)) == 1 else ""


def _report_frames(uploaded):
    """Yield (display_name, bytes) for reports or reports inside ZIP/RAR files."""
    name = uploaded.name.lower()
    if not name.endswith((".zip", ".rar")):
        yield uploaded.name, uploaded.getvalue()
        return
    try:
        if name.endswith(".rar"):
            try:
                import rarfile
            except ImportError as exc:
                raise RuntimeError("RAR support requires the rarfile package.") from exc
            if not _configure_rar_backend(rarfile):
                raise RuntimeError(
                    "RAR extraction backend not found. Install 7-Zip/WinRAR locally "
                    "or add p7zip-full to the deployment packages."
                )
            archive = rarfile.RarFile(BytesIO(uploaded.getvalue()))
        else:
            archive = zipfile.ZipFile(BytesIO(uploaded.getvalue()))
        with archive:
            for info in archive.infolist():
                is_dir = info.isdir() if hasattr(info, "isdir") else info.is_dir()
                if is_dir or not info.filename.lower().endswith((".xlsx", ".xls", ".csv")):
                    continue
                yield f"{uploaded.name} / {info.filename}", archive.read(info)
    except (zipfile.BadZipFile, RuntimeError) as exc:
        st.warning(f"{uploaded.name}: could not open archive ({exc})")
    except Exception as exc:
        st.warning(f"{uploaded.name}: invalid or unreadable archive ({exc})")


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
                frame = _read_report_cached(source_name, source_bytes, "v4")
            except Exception as exc:
                problems.append(f"{source_name}: could not read ({exc})")
                continue

            sid_col = _pick(frame.columns, ["PRODUCT_SET_SID", "PRODUCTSETSID", "PRODUCT_SET_SID_", "SID"])
            image_col = _pick(frame.columns, [
                "MAIN_IMAGE", "IMAGE1", "IMAGE", "IMAGE_URL", "MAIN_IMAGE_URL",
                "IMAGE_LINK", "IMAGE_LINKS", "PRODUCT_IMAGE", "PRODUCT_IMAGE_URL", "URL", "LINK",
            ])
            flag_col = _pick(frame.columns, ["FLAG", "REJECTION_FLAG", "ISSUE", "REASON_FLAG"])
            status_col = _pick(frame.columns, ["STATUS", "LISTING_STATUS"])
            reason_col = _pick(frame.columns, ["REASON", "COMMENT", "COMMENT_DETAIL"])
            brand_col = _pick(frame.columns, ["BRAND", "BRAND_INFRINGED", "INFRINGED_BRAND", "BRAND_CLAIMED", "CLAIMED_BRAND"])
            country_col = _pick(frame.columns, ["ACTIVE_STATUS_COUNTRY", "COUNTRY_CODE", "COUNTRY", "MARKET"])
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
            _country_aliases = {
                "KENYA": "KE", "UGANDA": "UG", "NIGERIA": "NG", "GHANA": "GH",
                "MOROCCO": "MA", "EGYPT": "EG", "SENEGAL": "SN", "IVORY COAST": "CI",
            }
            if country_col:
                work["_COUNTRY_CODE"] = work[country_col].fillna("").astype(str).str.strip().str.upper().map(
                    lambda value: _country_aliases.get(value, value)
                )
                work.loc[~work["_COUNTRY_CODE"].isin(SUPPORTED_RULE_COUNTRIES), "_COUNTRY_CODE"] = ""
            else:
                work["_COUNTRY_CODE"] = _infer_source_country(source_name)
            _source_country = _infer_source_country(source_name)
            if _source_country:
                work.loc[work["_COUNTRY_CODE"].eq(""), "_COUNTRY_CODE"] = _source_country
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
        _existing_rules = load_learned_image_rules()
        c3.metric("Current learned rules", len(_existing_rules))
        _existing_keys = {
            (str(_rule.get("image_url", "") or "").strip(), normalize_learned_flag(_rule.get("flag", "")))
            for _rule in _existing_rules if isinstance(_rule, dict)
        }
        _preview_keys = list(zip(
            preview["MAIN_IMAGE"].astype(str).str.strip(),
            preview["FLAG"].map(normalize_learned_flag),
        ))
        _duplicate_preview_count = sum(1 for _key in _preview_keys if _key in _existing_keys)
        _new_preview_count = max(0, len(preview) - _duplicate_preview_count)
        if _duplicate_preview_count:
            if _new_preview_count:
                st.warning(
                    f"This upload contains {_duplicate_preview_count:,} rule(s) already learned and {_new_preview_count:,} potentially new rule(s). "
                    "Duplicates will be skipped; only new rules will be written.",
                    icon=":material/content_copy:",
                )
            else:
                st.warning(
                    f"All {len(preview):,} eligible rows are already in the learned catalog. No new rules will be added.",
                    icon=":material/content_copy:",
                )
        preview["_APPLIES_IN"] = preview["FLAG"].map(
            lambda value: "Kenya, Uganda" if normalize_learned_flag(value) == "Poor images" else "Kenya"
        )
        st.caption("Source country is inferred from the file when possible. Applies in shows the learned-rule scope that will be saved.")
        preview_display = preview[[
            "PRODUCT_SET_SID", "MAIN_IMAGE", "FLAG", "_COUNTRY_CODE", "_APPLIES_IN", "_SOURCE_FILE"
        ]].rename(columns={
            "_COUNTRY_CODE": "SOURCE COUNTRY",
            "_APPLIES_IN": "APPLIES IN",
            "_SOURCE_FILE": "SOURCE FILE",
        })
        st.dataframe(
            preview_display.head(100),
            hide_index=True,
            width="stretch",
        )

        if st.button("Import eligible rules", icon=":material/upload_file:", type="primary", width="stretch", help="Duplicate URL/reason rules are skipped automatically."):
            _progress = st.progress(0, text="Starting import…")
            _status = st.empty()
            _files = list(preview.groupby("_SOURCE_FILE"))
            _file_progress = {
                _source_name: st.progress(0, text=f"Queued · {_source_name}")
                for _source_name, _source_group in _files
            }
            _batches = []
            for _source_name, _source_group in _files:
                for (flag, _country_code), group in _source_group.groupby(["FLAG", "_COUNTRY_CODE"], dropna=False):
                    _batches.append({
                        "data": group,
                        "sids": group["PRODUCT_SET_SID"].astype(str).unique(),
                        "flag": flag,
                        "reason": str(group["_REASON"].iloc[0]).strip() or "Imported historical rejection",
                        "source": str(_source_name),
                        "hash_by_url": hash_cache,
                        "country_code": str(_country_code or ""),
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
                # Snapshot the authoritative catalog before the bulk write.
                # Keep the compressed file locally and upload it to R2 so an
                # import can be rolled back even after a cloud restart.
                _catalog_path = APP_ROOT / "learned_image_rules.json"
                if _catalog_path.exists():
                    _snapshot_dir = _catalog_path.parent / ".learned_rule_snapshots"
                    _snapshot_dir.mkdir(parents=True, exist_ok=True)
                    _snapshot_hasher = hashlib.sha256()
                    with _catalog_path.open("rb") as _hash_source:
                        for _hash_chunk in iter(lambda: _hash_source.read(1024 * 1024), b""):
                            _snapshot_hasher.update(_hash_chunk)
                    _snapshot_digest = _snapshot_hasher.hexdigest()[:12]
                    _snapshot_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    _snapshot_path = _snapshot_dir / f"{_snapshot_stamp}-{_snapshot_digest}.json.gz"
                    with _catalog_path.open("rb") as _source_catalog, gzip.open(_snapshot_path, "wb", compresslevel=6) as _compressed_catalog:
                        shutil.copyfileobj(_source_catalog, _compressed_catalog, length=1024 * 1024)
                    for _old_snapshot in sorted(_snapshot_dir.glob("*.json.gz"), key=lambda path: path.stat().st_mtime, reverse=True)[100:]:
                        try:
                            _old_snapshot.unlink()
                        except OSError:
                            pass
                    if r2_storage.enabled():
                        _snapshot_key = f"learned-rules/snapshots/{_snapshot_path.name}"
                        if not r2_storage.upload_file_verified(_snapshot_path, _snapshot_key, "application/gzip"):
                            raise RuntimeError("The pre-import snapshot could not be verified in R2; import was stopped before changing the learned catalog.")
                        r2_storage.prune_prefix("learned-rules/snapshots/", keep=100)
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
