"""Fast, append-only learned image rules.

The JSON file is the authoritative catalog. Rules are indexed in memory and
written atomically so iframe rejection does not wait on a database or
spreadsheet write. The old SQLite mirror was removed because validation never
queried it and its snapshot audit table grew without bound.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import threading
import time
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import supabase_store
import r2_storage

try:
    import orjson as _fast_json
except ImportError:  # local environments may still use the stdlib fallback
    _fast_json = None

RULES_PATH = Path(__file__).resolve().parent / "learned_image_rules.json"
DELETED_RULES_ARCHIVE = RULES_PATH.with_name("learned_image_rules.deleted.jsonl")
REMOTE_SYNC_OUTBOX = RULES_PATH.with_name("learned_image_sync_pending.json")
_RULES_LOAD_ERROR = None


def _archive_deleted_rules(records) -> None:
    """Persist deliberately removed records before changing the live catalog.

    This append-only recovery log is separate from the active rules file, so an
    explicit Dashboard deletion can be undone even after a restart.
    """
    rows = [row for row in (records or []) if isinstance(row, dict)]
    if not rows:
        return
    deleted_at = datetime.now(timezone.utc).isoformat()
    DELETED_RULES_ARCHIVE.parent.mkdir(parents=True, exist_ok=True)
    with DELETED_RULES_ARCHIVE.open("a", encoding="utf-8") as archive:
        for row in rows:
            archive.write(json.dumps({"deleted_at": deleted_at, "rule": row}, ensure_ascii=False) + "\n")
        archive.flush()
        os.fsync(archive.fileno())


def _rule_identity_counts(records) -> Counter:
    """Count stable rule identities, excluding fields users can edit."""
    return Counter(
        (
            str(row.get("image_url", "") or "").strip(),
            str(row.get("phash", "") or "").strip().casefold(),
            str(row.get("created_at", "") or "").strip(),
            str(row.get("product_set_sid", "") or "").strip(),
            str(row.get("seller_name", "") or "").strip(),
            str(row.get("source", "") or "").strip(),
        )
        for row in records
        if isinstance(row, dict)
    )


def _assert_no_rules_lost(previous, proposed) -> None:
    """Reject any routine catalog write that drops an existing rule identity."""
    before = previous if isinstance(previous, Counter) else _rule_identity_counts(previous)
    after = _rule_identity_counts(proposed)
    missing = before - after
    if missing:
        raise RuntimeError(
            f"Refusing learned-rule write: {sum(missing.values()):,} existing rule(s) "
            "would disappear. Use an explicit archived removal action instead."
        )

def _atomic_replace_rules(temp_path: Path, target_path: Path, retries: int = 40, delay: float = 0.10) -> None:
    """Safely replace target_path with temp_path on Windows and POSIX.

    On Windows, active readers (Streamlit file watcher, antivirus, concurrent threads)
    can hold a temporary read lock on target_path, raising PermissionError [WinError 5]
    or OSError [WinError 32]. Retry with exponential backoff and fall back to copy if needed.
    """
    if target_path.resolve() == RULES_PATH.resolve() and _RULES_LOAD_ERROR:
        raise RuntimeError(
            "Learned rules could not be read safely; refusing to overwrite the catalog. "
            f"Repair or restore {RULES_PATH.name} first. Details: {_RULES_LOAD_ERROR}"
        )
    last_error = None
    for attempt in range(retries):
        try:
            os.replace(temp_path, target_path)
            return
        except (PermissionError, OSError) as e:
            last_error = e
            # Windows scanners and Streamlit's file watcher can hold the
            # destination for several seconds. Keep the complete temp file
            # intact while waiting; never truncate the live catalog.
            if attempt < retries - 1:
                time.sleep(min(delay * (1.35 ** attempt), 1.0))
                continue

    # A locked destination cannot be atomically replaced. As a last resort,
    # copy through a separate handle; this works for transient sharing locks
    # on many Windows installations and still leaves the original untouched if
    # the copy cannot be opened. The caller receives a clear error and the
    # .tmp file remains available for a later retry.
    try:
        shutil.copyfile(temp_path, target_path)
        temp_path.unlink(missing_ok=True)
        return
    except Exception as copy_error:
        raise PermissionError(
            f"Could not update {target_path.name} because Windows has the file "
            "locked. Close other Dashboard/Streamlit tabs or antivirus preview "
            "and retry; the staged import remains in the temporary file."
        ) from (last_error or copy_error)
LEARNABLE_FLAGS = frozenset({
    "Poor images", "Image Stretched", "Image Blurry", "Image Mismatch",
    "Image Infringing", "Image Too Many things displayed",
    "NSFW Images",
    "Restricted brands", "Suspected Fake product",
    "Brand Image Mismatch", "Counterfeit Sneakers",
    "Suspected counterfeit Jerseys",
    "Category Check – Replica Jersey / IP Violation",
})
SUPPORTED_RULE_COUNTRIES = ("KE", "UG", "NG", "GH", "MA", "EG", "SN", "CI")
FLAG_ALIASES = {
    "poor image": "Poor images", "poor images": "Poor images",
    "poor image quality": "Poor images",
    "nsfw image": "NSFW Images", "nsfw images": "NSFW Images",
    "adult / nsfw content": "NSFW Images", "adult nsfw content": "NSFW Images",
    "adult content image": "NSFW Images",
    "image stretched": "Image Stretched",
    "image blurry": "Image Blurry",
    "image mismatch": "Image Mismatch",
    "image infringing": "Image Infringing",
    "reject_img_infringing": "Image Infringing",
    "too many things": "Image Too Many things displayed",
    "too many things displayed": "Image Too Many things displayed",
    "image too many things": "Image Too Many things displayed",
    "image too many things displayed": "Image Too Many things displayed",
    "restricted brand": "Restricted brands", "restricted brands": "Restricted brands",
    "suspected counterfeit": "Suspected Fake product",
    "counterfeit product": "Suspected Fake product",
    "suspected fake": "Suspected Fake product",
    "suspected fake product": "Suspected Fake product",
    "suspected fake sneakers": "Counterfeit Sneakers",
    "counterfeit sneakers": "Counterfeit Sneakers",
    "suspected counterfeit sneakers": "Counterfeit Sneakers",
    "counterfeit jersey": "Suspected counterfeit Jerseys",
    "counterfeit jerseys": "Suspected counterfeit Jerseys",
    "suspected counterfeit jersey": "Suspected counterfeit Jerseys",
    "suspected counterfeit jerseys": "Suspected counterfeit Jerseys",
    "category check – replica jersey / ip violation": "Category Check – Replica Jersey / IP Violation",
    "category check - replica jersey / ip violation": "Category Check – Replica Jersey / IP Violation",
    "replica jersey / ip violation": "Category Check – Replica Jersey / IP Violation",
}
# When one image has several learned reasons, generic poor-image quality is
# the least specific explanation. Keep it as a fallback so a more specific
# learned reason remains visible and actionable after one reason is removed.
_LEARNED_FLAG_PRIORITY = {
    "Poor images": 10,
    "NSFW Images": 80,
    "Image Stretched": 30,
    "Image Blurry": 30,
    "Image Mismatch": 40,
    "Image Too Many things displayed": 40,
    "Image Infringing": 50,
    "Restricted brands": 60,
    "Brand Image Mismatch": 60,
    "Suspected Fake product": 70,
    "Counterfeit Sneakers": 70,
    "Suspected counterfeit Jerseys": 70,
    "Category Check – Replica Jersey / IP Violation": 75,
}

_LOCK = threading.RLock()
_RULES_MTIME = -1.0
_RULES = []
_WRITER = ThreadPoolExecutor(max_workers=1, thread_name_prefix="learned-rules")
_REMOTE_RULES = None
_REMOTE_RULES_FETCHED_AT = 0.0
_REMOTE_RULES_TTL = 60.0
_REMOTE_RULES_SUSPICIOUS = False
_PAGE_RULE_CACHE = OrderedDict()
_PAGE_RULE_CACHE_MAX = 64   # covers batches up to ~64k rows at 1k chunk size
_RUNTIME_INDEX = None
_RUNTIME_INDEX_MTIME = -1
_LOG = logging.getLogger(__name__)
_REMOTE_SYNC_STATUS = {
    "status": "not_configured",
    "pending": [],
    "last_synced_at": "",
    "last_error": "",
    "queue_durable": False,
    "queue_location": "",
}
_REMOTE_SYNC_OUTBOX_LOADED = False


def _save_remote_sync_outbox_locked() -> None:
    """Persist cloud write retries separately from the authoritative rule catalog."""
    temp = REMOTE_SYNC_OUTBOX.with_suffix(".json.tmp")
    try:
        REMOTE_SYNC_OUTBOX.parent.mkdir(parents=True, exist_ok=True)
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(_REMOTE_SYNC_STATUS.get("pending", [])[-5000:], handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, REMOTE_SYNC_OUTBOX)
        if r2_storage.enabled():
            _queue_uploaded = r2_storage.upload_bytes(
                REMOTE_SYNC_OUTBOX.read_bytes(),
                "learned-rules/sync-outbox.json",
                "application/json",
            )
            _REMOTE_SYNC_STATUS["queue_durable"] = bool(_queue_uploaded)
            _REMOTE_SYNC_STATUS["queue_location"] = "R2" if _queue_uploaded else "local only"
        else:
            _REMOTE_SYNC_STATUS["queue_durable"] = True
            _REMOTE_SYNC_STATUS["queue_location"] = "local"
    except Exception:
        _LOG.exception("Could not persist learned-rule cloud sync retry queue")
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _load_remote_sync_outbox_locked() -> None:
    global _REMOTE_SYNC_OUTBOX_LOADED
    if _REMOTE_SYNC_OUTBOX_LOADED:
        return
    _REMOTE_SYNC_OUTBOX_LOADED = True
    try:
        if REMOTE_SYNC_OUTBOX.exists():
            with REMOTE_SYNC_OUTBOX.open("r", encoding="utf-8") as handle:
                records = json.load(handle)
        elif r2_storage.enabled():
            _raw = r2_storage.download_bytes("learned-rules/sync-outbox.json")
            records = json.loads(_raw.decode("utf-8")) if _raw else []
            if records:
                REMOTE_SYNC_OUTBOX.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
                _REMOTE_SYNC_STATUS["queue_durable"] = True
                _REMOTE_SYNC_STATUS["queue_location"] = "R2"
        else:
            records = []
        if isinstance(records, list):
            _REMOTE_SYNC_STATUS["pending"] = [row for row in records if isinstance(row, dict)][-5000:]
            if _REMOTE_SYNC_STATUS["pending"]:
                _REMOTE_SYNC_STATUS["status"] = "pending"
                if not _REMOTE_SYNC_STATUS.get("queue_location"):
                    _REMOTE_SYNC_STATUS["queue_location"] = "local"
                _REMOTE_SYNC_STATUS["queue_durable"] = True
    except FileNotFoundError:
        pass
    except (OSError, ValueError, TypeError):
        _LOG.exception("Could not load learned-rule cloud sync retry queue")


def _clear_page_rule_cache() -> None:
    _PAGE_RULE_CACHE.clear()


def get_cloud_sync_status() -> dict:
    """Return the latest image-rule cloud sync state without exposing secrets."""
    with _LOCK:
        _load_remote_sync_outbox_locked()
        return {
            "configured": bool(supabase_store.enabled()),
            "status": _REMOTE_SYNC_STATUS.get("status", "not_configured"),
            "pending_count": len(_REMOTE_SYNC_STATUS.get("pending", [])),
            "last_synced_at": _REMOTE_SYNC_STATUS.get("last_synced_at", ""),
            "last_error": _REMOTE_SYNC_STATUS.get("last_error", ""),
            "queue_durable": bool(_REMOTE_SYNC_STATUS.get("queue_durable", False)),
            "queue_location": str(_REMOTE_SYNC_STATUS.get("queue_location", "")),
        }

def _sync_sqlite_locked(rules: list) -> None:
    """Compatibility no-op for callers from the pre-JSON-only implementation."""
    return None


def normalize_learned_flag(value: str) -> str:
    """Map report spelling variants to one exact canonical flag."""
    raw = re.sub(r"\s*\(prefetched\)\s*$", "", str(value or "").strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*-\s*blocked fingerprint\s*$", "", raw, flags=re.IGNORECASE)
    return FLAG_ALIASES.get(raw.casefold(), raw)


def _country_scope_for_learned_flag(flag: str) -> list[str]:
    """Keep learned image rules Kenya-only until another market is enabled."""
    return ["KE"]


def normalize_rule_countries(value, *, legacy_global: bool = True) -> list[str]:
    """Normalize learned-rule market scope; legacy records remain global."""
    if value is None:
        return ["*"] if legacy_global else []
    if isinstance(value, str):
        raw_values = re.split(r"[,;|]", value)
    else:
        try:
            raw_values = list(value)
        except TypeError:
            raw_values = [value]
    countries = sorted({str(item).strip().upper() for item in raw_values if str(item).strip()})
    if not countries:
        return ["*"] if legacy_global else []
    if "*" in countries or "ALL" in countries:
        return ["*"]
    return [code for code in countries if code in SUPPORTED_RULE_COUNTRIES]


def normalize_rule_values(value) -> list[str]:
    """Normalize optional category or brand scope values."""
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = re.split(r"[,;\n|]", value)
    else:
        try:
            raw_values = list(value)
        except TypeError:
            raw_values = [value]
    return sorted({str(item).strip() for item in raw_values
                   if str(item).strip() and str(item).strip().casefold() not in {"nan", "none"}}, key=str.casefold)


def _rule_applies_in_country(rule: dict, country_code: str) -> bool:
    countries = normalize_rule_countries(rule.get("countries"), legacy_global=True)
    return not country_code or "*" in countries or str(country_code).strip().upper() in countries


def _load_locked() -> list:
    global _RULES_MTIME, _RULES, _RULES_LOAD_ERROR
    try:
        # Nanosecond precision matters when two browser tabs write the same
        # catalog within one filesystem second. Second-resolution mtimes could
        # make the other tab keep serving its stale in-memory rule list.
        mtime = RULES_PATH.stat().st_mtime_ns
    except OSError:
        mtime = 0.0
    if mtime == _RULES_MTIME and _RULES is not None:
        return _RULES
    _clear_page_rule_cache()
    try:
        if _fast_json is not None:
            loaded = _fast_json.loads(RULES_PATH.read_bytes())
        else:
            with RULES_PATH.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
        if not isinstance(loaded, list):
            raise ValueError("catalog root must be a JSON array")
        _RULES = loaded
        _RULES_LOAD_ERROR = None
    except (OSError, ValueError, TypeError) as exc:
        _RULES_LOAD_ERROR = f"{type(exc).__name__}: {exc}"
        _LOG.error("Learned rules catalog is unreadable; preserving it and disabling writes: %s", _RULES_LOAD_ERROR)
        _RULES = []
    _RULES_MTIME = mtime
    return _RULES


def _local_rule_count() -> int:
    """Get local catalog size through the mtime-aware shared cache.

    Parsing the 50+ MB JSON again just to count it doubled peak memory during
    remote catalog checks. `_load_locked` already parses only when the file
    revision changes, so reuse that result here.
    """
    with _LOCK:
        return len(_load_locked())


def load_learned_image_rules() -> list:
    global _REMOTE_RULES, _REMOTE_RULES_FETCHED_AT, _REMOTE_RULES_SUSPICIOUS
    if supabase_store.enabled():
        now = time.monotonic()
        if _REMOTE_RULES_SUSPICIOUS and now - _REMOTE_RULES_FETCHED_AT < _REMOTE_RULES_TTL:
            # A previous fetch looked like a truncated remote catalog. Keep
            # using the known-good local snapshot until it is repaired.
            with _LOCK:
                return list(_load_locked())
        if _REMOTE_RULES is None or now - _REMOTE_RULES_FETCHED_AT >= _REMOTE_RULES_TTL:
            try:
                _fetched = supabase_store.fetch_all("learned_image_rules")
                _REMOTE_RULES_FETCHED_AT = now
                _active_fetched = [
                    row for row in _fetched
                    if str(row.get("status", "active") or "active").strip().casefold() not in {"removed", "deleted", "archived"}
                ]
                _local_count = _local_rule_count()
                # Never let a partial cloud migration replace a substantially
                # larger local catalog during an edit/import. A real remote
                # catalog may be larger or the local file may be absent; only
                # reject an implausibly small remote result.
                if _local_count >= 100 and len(_active_fetched) < max(25, int(_local_count * 0.5)):
                    _REMOTE_RULES = None
                    _REMOTE_RULES_SUSPICIOUS = True
                    _LOG.error(
                        "Refusing partial Supabase learned catalog (%s rules vs %s local); "
                        "repair the cloud catalog before saving learned data.",
                        len(_active_fetched), _local_count,
                    )
                else:
                    _REMOTE_RULES = _active_fetched
                    _REMOTE_RULES_SUSPICIOUS = False
            except Exception as exc:
                _LOG.warning("Could not read Supabase learned rules; using local fallback: %s", exc)
                _REMOTE_RULES = None
        if _REMOTE_RULES is not None:
            return list(_REMOTE_RULES)
    with _LOCK:
        return list(_load_locked())


def iter_learned_image_rules(country_code: str = ""):
    """Iterate the cached catalog for read-only work without copying its list.

    The returned dictionaries must be treated as read-only. Mutations should
    continue to use ``load_learned_image_rules`` and the explicit update APIs.
    Writers replace the cached list under ``_LOCK``; retaining this reference
    keeps an in-progress iteration stable while that happens.
    """
    if supabase_store.enabled():
        # The public loader provides a defensive list for remote callers; keep
        # that behavior here because Supabase data is refreshed asynchronously
        # and the local catalog is not authoritative when remote sync is on.
        rules = load_learned_image_rules()
    else:
        with _LOCK:
            rules = _load_locked()

    code = str(country_code or "").strip().upper()
    for rule in rules or ():
        if not code or _rule_applies_in_country(rule, code):
            yield rule

_REMOTE_RULE_COLUMNS = {
    "image_url", "phash", "flag", "reason", "comment", "brand_infringed",
    "category_code", "product_set_sid", "seller_name", "status", "review_state",
    "source", "created_at", "updated_at", "last_matched_at", "last_confirmed_at",
    "review_due_at", "countries", "match_count", "matched_sids", "matched_sellers", "impact_truncated",
    "brand_infringed_by_country", "scope_categories", "scope_brands",
}


def _remote_upsert_rules(records: list[dict]) -> None:
    if not records:
        return
    if not supabase_store.enabled():
        with _LOCK:
            _REMOTE_SYNC_STATUS.update(status="local_only", last_error="")
        return
    now = supabase_store.now_iso()
    payload = []
    for record in records:
        row = {key: record.get(key) for key in _REMOTE_RULE_COLUMNS}
        row["countries"] = normalize_rule_countries(record.get("countries"), legacy_global=True)
        row["match_count"] = int(record.get("match_count", 0) or 0)
        row["matched_sids"] = list(record.get("matched_sids", []) or [])
        row["matched_sellers"] = list(record.get("matched_sellers", []) or [])
        row["brand_infringed_by_country"] = record.get("brand_infringed_by_country", {}) or {}
        row["scope_categories"] = normalize_rule_values(record.get("scope_categories"))
        row["scope_brands"] = normalize_rule_values(record.get("scope_brands"))
        row["updated_at"] = row.get("updated_at") or now
        row["created_at"] = row.get("created_at") or now
        for field in ("last_matched_at", "last_confirmed_at", "review_due_at"):
            if row.get(field) == "":
                row[field] = None
        payload.append(row)
    try:
        # The production table is keyed by (phash, flag). Older deployments
        # used (image_url, flag), which makes that conflict target return a
        # PostgREST 400. Send hashed rows against the live constraint and send
        # rows without a hash as nullable inserts; a duplicate insert is safe
        # to ignore because the local JSON remains authoritative.
        _hashed = [row for row in payload if row.get("phash")]
        _unhashed = [row for row in payload if not row.get("phash")]
        if _hashed:
            try:
                supabase_store.upsert("learned_image_rules", _hashed, on_conflict="phash,flag")
            except Exception as _phash_conflict_exc:
                # Compatibility with projects that retained the original
                # image_url/flag unique constraint instead of the pHash one.
                _phash_error = str(_phash_conflict_exc).lower()
                _is_conflict_target_error = any(
                    marker in _phash_error
                    for marker in (
                        "on conflict", "conflict target", "unique constraint",
                        "no unique", "exclusion constraint",
                    )
                )
                if not _is_conflict_target_error:
                    raise
                supabase_store.upsert("learned_image_rules", _hashed, on_conflict="image_url,flag")
        if _unhashed:
            for row in _unhashed:
                row["phash"] = None
                try:
                    supabase_store.insert("learned_image_rules", [row])
                except Exception as _insert_exc:
                    # A pre-existing URL/flag row is already synchronized.
                    if "409" not in str(_insert_exc) and "duplicate" not in str(_insert_exc).lower():
                        raise
        global _REMOTE_RULES, _REMOTE_RULES_FETCHED_AT, _REMOTE_RULES_SUSPICIOUS
        _REMOTE_RULES = None
        _REMOTE_RULES_FETCHED_AT = 0.0
        _REMOTE_RULES_SUSPICIOUS = False
        _clear_page_rule_cache()
        with _LOCK:
            _load_remote_sync_outbox_locked()
            pending = _REMOTE_SYNC_STATUS.setdefault("pending", [])
            synced_keys = {
                (str(row.get("image_url", "") or ""), str(row.get("phash", "") or ""),
                 normalize_learned_flag(row.get("flag", "")), str(row.get("status", "active") or "active"))
                for row in payload
            }
            pending[:] = [
                item for item in pending
                if (str(item.get("image_url", "") or ""), str(item.get("phash", "") or ""),
                    normalize_learned_flag(item.get("flag", "")), str(item.get("status", "active") or "active"))
                not in synced_keys
            ]
            _REMOTE_SYNC_STATUS.update(
                status="synced" if not pending else "pending",
                last_synced_at=supabase_store.now_iso(),
                last_error="",
            )
            _save_remote_sync_outbox_locked()
    except Exception as exc:
        _LOG.warning("Could not write learned rules to Supabase: %s", exc)
        with _LOCK:
            _load_remote_sync_outbox_locked()
            pending = _REMOTE_SYNC_STATUS.setdefault("pending", [])
            known = {
                (str(row.get("image_url", "") or ""), str(row.get("phash", "") or ""),
                 normalize_learned_flag(row.get("flag", "")), str(row.get("status", "active") or "active"))
                for row in pending
            }
            for row in payload:
                key = (str(row.get("image_url", "") or ""), str(row.get("phash", "") or ""),
                       normalize_learned_flag(row.get("flag", "")), str(row.get("status", "active") or "active"))
                if key not in known:
                    pending.append(row)
                    known.add(key)
            del pending[:-5000]
            _REMOTE_SYNC_STATUS.update(status="failed", last_error=f"{type(exc).__name__}: {exc}")
            _save_remote_sync_outbox_locked()


def retry_pending_cloud_sync() -> int:
    """Retry queued rule writes. Returns the number of records still pending."""
    with _LOCK:
        pending = list(_REMOTE_SYNC_STATUS.get("pending", []))
    if pending and supabase_store.enabled():
        _remote_upsert_rules(pending)
    return int(get_cloud_sync_status()["pending_count"])


def _remote_delete_rules(records: list[dict]) -> None:
    if not records or not supabase_store.enabled():
        return
    try:
        # Keep cloud history too: user-requested removals are soft-deleted in
        # Supabase. The runtime loader filters these records from active rules.
        archived = []
        now = supabase_store.now_iso()
        for record in records:
            row = dict(record)
            row["status"] = "removed"
            row["updated_at"] = now
            archived.append(row)
        _remote_upsert_rules(archived)
        global _REMOTE_RULES, _REMOTE_RULES_FETCHED_AT
        _REMOTE_RULES = None
        _REMOTE_RULES_FETCHED_AT = 0.0
        _clear_page_rule_cache()
    except Exception as exc:
        _LOG.warning("Could not delete learned rules from Supabase: %s", exc)


def delete_learned_image_rules(rules) -> int:
    """Delete exact rule records from the JSON catalog and return the count."""
    targets = {
        (
            str(rule.get("image_url", "")).strip(),
            str(rule.get("phash", "")).strip(),
            normalize_learned_flag(rule.get("flag", "")),
            str(rule.get("created_at", "")).strip(),
        )
        for rule in (rules or [])
        if isinstance(rule, dict)
    }
    if not targets:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        kept = []
        removed_records = []
        removed = 0
        for rule in current:
            key = (
                str(rule.get("image_url", "")).strip(),
                str(rule.get("phash", "")).strip(),
                normalize_learned_flag(rule.get("flag", "")),
                str(rule.get("created_at", "")).strip(),
            )
            if key in targets:
                removed += 1
                removed_records.append(rule)
            else:
                kept.append(rule)
        if not removed:
            return 0
        _archive_deleted_rules(removed_records)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(removed_records)
        return removed


def remove_learned_rule_match(
    flag: str,
    *,
    rule_urls=None,
    rule_phashes=None,
    country_code: str = "",
    explicit_approval: bool = False,
) -> int:
    """Remove only the matched learned image rule for an explicit approval.

    This is intentionally separate from validation reconciliation: only a
    reviewer action may call it, and the reason must match exactly after flag
    normalization. Other reasons attached to the same image remain active.
    Removed records are archived and soft-deleted remotely by the normal
    explicit deletion path.
    """
    if not explicit_approval:
        return 0
    canonical_flag = normalize_learned_flag(flag)
    urls = {str(value or "").strip() for value in (rule_urls or [])}
    phashes = {str(value or "").strip().casefold() for value in (rule_phashes or [])}
    urls.discard("")
    phashes.discard("")
    if not canonical_flag or (not urls and not phashes):
        return 0

    country_code = str(country_code or "").strip().upper()
    matched_rules = [
        rule for rule in iter_learned_image_rules()
        if isinstance(rule, dict)
        and normalize_learned_flag(rule.get("flag", "")) == canonical_flag
        and _rule_applies_in_country(rule, country_code)
        and (
            (str(rule.get("image_url", "") or "").strip() in urls)
            or (str(rule.get("phash", "") or "").strip().casefold() in phashes)
        )
    ]
    if not country_code:
        return delete_learned_image_rules(matched_rules)

    # An explicit approval corrects the rule for this market. Preserve other
    # market decisions when a rule is shared globally or across countries.
    changed, removed = [], []
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        for rule in current:
            if rule not in matched_rules:
                continue
            old_countries = normalize_rule_countries(rule.get("countries"), legacy_global=True)
            next_countries = (
                [code for code in SUPPORTED_RULE_COUNTRIES if code != country_code]
                if "*" in old_countries
                else [code for code in old_countries if code != country_code]
            )
            if not next_countries:
                removed.append(rule)
            else:
                rule["countries"] = next_countries
                rule["updated_at"] = datetime.now(timezone.utc).isoformat()
                changed.append(rule)
        if not changed and not removed:
            return 0
        kept = [rule for rule in current if rule not in removed]
        _assert_no_rules_lost(existing_identities - _rule_identity_counts(removed), kept)
        if removed:
            _archive_deleted_rules(removed)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME, _RUNTIME_INDEX, _RUNTIME_INDEX_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _RUNTIME_INDEX = None
        _RUNTIME_INDEX_MTIME = -1
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(removed)
        _remote_upsert_rules(changed)
        return len(changed) + len(removed)


def delete_associated_learned_image_rules(rules) -> int:
    """Remove every rule associated with the supplied image identity.

    A single image can be learned more than once under different CDN URLs or
    flags.  Removing only the displayed URL leaves those aliases active and
    causes the same product to be rejected again.  Treat a non-empty URL or
    pHash from any selected rule as the identity and remove all matching
    records from JSON, the SQLite mirror, and the remote store.
    """
    records = [rule for rule in (rules or []) if isinstance(rule, dict)]
    if not records:
        return 0
    urls = {str(rule.get("image_url", "") or "").strip() for rule in records}
    phashes = {str(rule.get("phash", "") or "").strip().casefold() for rule in records}
    urls.discard("")
    phashes.discard("")
    if not urls and not phashes:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        removed_records = []
        kept = []
        for rule in current:
            url = str(rule.get("image_url", "") or "").strip()
            phash = str(rule.get("phash", "") or "").strip().casefold()
            if (url and url in urls) or (phash and phash in phashes):
                removed_records.append(rule)
            else:
                kept.append(rule)
        if not removed_records:
            return 0
        _archive_deleted_rules(removed_records)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(removed_records)
        return len(removed_records)


def audit_duplicate_learned_image_rules(rules=None) -> list[dict]:
    """Return exact-pHash duplicate groups without changing the catalog.

    A URL is not part of the grouping key: the same downloaded image can have
    different CDN URLs. Flags remain part of the key because one image may
    legitimately carry separate review reasons. Rules without a pHash are
    reported only when their URL is repeated exactly.
    """
    current = list(rules if rules is not None else load_learned_image_rules())
    groups = {}
    for index, rule in enumerate(current):
        if not isinstance(rule, dict):
            continue
        phash = str(rule.get("phash", "") or "").strip().casefold()
        url = str(rule.get("image_url", "") or "").strip()
        flag = normalize_learned_flag(rule.get("flag", ""))
        key = ("phash", phash, flag) if phash else ("url", url, flag) if url else None
        if key is not None:
            groups.setdefault(key, []).append((index, rule))
    duplicates = []
    for key, entries in groups.items():
        if len(entries) < 2:
            continue
        duplicates.append({
            "match_type": "Exact pHash" if key[0] == "phash" else "Exact URL",
            "phash": key[1] if key[0] == "phash" else "",
            "flag": key[2],
            "count": len(entries),
            "urls": sorted({str(row.get("image_url", "")).strip() for _, row in entries if row.get("image_url")}),
            "rules": [row for _, row in entries],
        })
    duplicates.sort(key=lambda item: item["count"], reverse=True)
    return duplicates


def deduplicate_learned_image_rules(rules=None) -> int:
    """Remove redundant exact URL/pHash rules, retaining one per group."""
    groups = audit_duplicate_learned_image_rules(rules)
    remove = []
    for group in groups:
        entries = list(group.get("rules", []))
        # Prefer a confirmed rule, then the most recently confirmed/matched,
        # then the most recently created record so useful audit metadata wins.
        entries.sort(key=lambda row: (
            str(row.get("review_state", "")).casefold() == "confirmed",
            str(row.get("last_confirmed_at", "") or ""),
            str(row.get("last_matched_at", "") or ""),
            str(row.get("created_at", "") or ""),
        ), reverse=True)
        remove.extend(entries[1:])
    return delete_learned_image_rules(remove)


def remove_learned_rules_by_flag(flag: str, *, confirmed: bool = False) -> int:
    """Explicitly remove every rule for a flag after caller confirmation.

    Routine validator/code changes must not prune the learned catalog. Callers
    must pass ``confirmed=True`` to make this destructive migration possible.
    """
    if not confirmed:
        _LOG.warning("Refusing learned-rule deletion by flag without explicit confirmation: %s", flag)
        return 0
    canonical = normalize_learned_flag(flag)
    with _LOCK:
        current = list(load_learned_image_rules())
        kept = [rule for rule in current if normalize_learned_flag(rule.get("flag", "")) != canonical]
        removed_records = [rule for rule in current if normalize_learned_flag(rule.get("flag", "")) == canonical]
        removed = len(current) - len(kept)
        if not removed:
            return 0
        _archive_deleted_rules(removed_records)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(removed_records)
        return removed


def restore_learned_image_rules(rules) -> int:
    """Restore previously removed rule records, skipping exact duplicates."""
    additions = [r for r in (rules or []) if isinstance(r, dict)]
    if not additions:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        keys = {(str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), str(r.get("created_at", "")).strip()) for r in current}
        new_items = [r for r in additions if (str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), str(r.get("created_at", "")).strip()) not in keys]
        if not new_items:
            return 0
        current.extend(new_items)
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(new_items)
        return len(new_items)


def replace_learned_image_rules(rules, *, confirmed: bool = False) -> int:
    """Explicitly restore a complete catalog snapshot after archiving current rules."""
    if not confirmed:
        raise ValueError("Replacing the learned catalog requires explicit confirmation.")
    replacement = [dict(row) for row in (rules or []) if isinstance(row, dict)]
    with _LOCK:
        current = list(load_learned_image_rules())
        if _RULES_LOAD_ERROR:
            raise RuntimeError(f"Current catalog is unreadable; refusing replacement: {_RULES_LOAD_ERROR}")
        _archive_deleted_rules(current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(replacement, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME, _RUNTIME_INDEX, _RUNTIME_INDEX_MTIME
        _RULES = replacement
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _RUNTIME_INDEX = None
        _RUNTIME_INDEX_MTIME = -1
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(current)
        _remote_upsert_rules(replacement)
        return len(replacement)


def restore_archived_learned_image_rules() -> int:
    """Restore all records from the append-only explicit-deletion archive."""
    if not DELETED_RULES_ARCHIVE.exists():
        return 0
    archived = []
    with DELETED_RULES_ARCHIVE.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
                record = payload.get("rule") if isinstance(payload, dict) else None
                if isinstance(record, dict):
                    archived.append(record)
            except (ValueError, TypeError):
                _LOG.warning("Skipping malformed line in learned-rule deletion archive")
    return restore_learned_image_rules(archived)


def update_learned_image_rule_flag(rule: dict, new_flag: str) -> bool:
    """Change one rule's canonical reason without changing its image identity."""
    if not isinstance(rule, dict):
        return False
    new_flag = normalize_learned_flag(new_flag)
    if new_flag not in LEARNABLE_FLAGS:
        return False
    target = (
        str(rule.get("image_url", "")).strip(),
        str(rule.get("phash", "")).strip(),
        str(rule.get("created_at", "")).strip(),
    )
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        changed = False
        for item in current:
            key = (
                str(item.get("image_url", "")).strip(),
                str(item.get("phash", "")).strip(),
                str(item.get("created_at", "")).strip(),
            )
            if key == target:
                item["flag"] = new_flag
                changed = True
                break
        if not changed:
            return False
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules([item for item in current if (
            str(item.get("image_url", "")).strip(),
            str(item.get("phash", "")).strip(),
            str(item.get("created_at", "")).strip(),
        ) == target])
        return True


def set_learned_image_rule_review(rule: dict, review_state: str) -> bool:
    """Set review metadata without changing whether a rule is active."""
    allowed = {"Unreviewed", "Confirmed", "Corrected", "Removed"}
    if not isinstance(rule, dict) or review_state not in allowed:
        return False
    target = (
        str(rule.get("image_url", "")).strip(),
        str(rule.get("phash", "")).strip(),
        str(rule.get("created_at", "")).strip(),
    )
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        changed = False
        for item in current:
            key = (str(item.get("image_url", "")).strip(), str(item.get("phash", "")).strip(), str(item.get("created_at", "")).strip())
            if key == target:
                item["review_state"] = review_state
                if review_state == "Confirmed":
                    item["last_confirmed_at"] = datetime.now(timezone.utc).isoformat()
                changed = True
                break
        if not changed:
            return False
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules([item for item in current if (
            str(item.get("image_url", "")).strip(),
            str(item.get("phash", "")).strip(),
            str(item.get("created_at", "")).strip(),
        ) == target])
        return True


def update_learned_image_rule_countries(rule: dict, countries) -> bool:
    """Set the allowed markets for one learned rule, preserving all other metadata."""
    if not isinstance(rule, dict):
        return False
    normalized = normalize_rule_countries(countries, legacy_global=False)
    if not normalized:
        return False
    target = (
        str(rule.get("image_url", "")).strip(),
        str(rule.get("phash", "")).strip(),
        str(rule.get("created_at", "")).strip(),
    )
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        changed_rules = []
        for item in current:
            key = (
                str(item.get("image_url", "")).strip(),
                str(item.get("phash", "")).strip(),
                str(item.get("created_at", "")).strip(),
            )
            if key == target:
                old = normalize_rule_countries(item.get("countries"), legacy_global=True)
                if old == normalized:
                    return False
                item["countries"] = normalized
                item["updated_at"] = datetime.now(timezone.utc).isoformat()
                changed_rules.append(item)
                break
        if not changed_rules:
            return False
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME, _RUNTIME_INDEX, _RUNTIME_INDEX_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _RUNTIME_INDEX = None
        _RUNTIME_INDEX_MTIME = -1
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(changed_rules)
        return True


def update_learned_image_rule_scope(rule: dict, *, categories=None, brands=None) -> bool:
    """Set optional category and declared-brand restrictions on one rule."""
    if not isinstance(rule, dict):
        return False
    normalized_categories = normalize_rule_values(categories)
    normalized_brands = normalize_rule_values(brands)
    target = (
        str(rule.get("image_url", "")).strip(),
        str(rule.get("phash", "")).strip(),
        str(rule.get("created_at", "")).strip(),
    )
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        changed_rules = []
        for item in current:
            key = (
                str(item.get("image_url", "")).strip(),
                str(item.get("phash", "")).strip(),
                str(item.get("created_at", "")).strip(),
            )
            if key == target:
                if (normalize_rule_values(item.get("scope_categories")) == normalized_categories
                        and normalize_rule_values(item.get("scope_brands")) == normalized_brands):
                    return False
                item["scope_categories"] = normalized_categories
                item["scope_brands"] = normalized_brands
                item["updated_at"] = datetime.now(timezone.utc).isoformat()
                changed_rules.append(item)
                break
        if not changed_rules:
            return False
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME, _RUNTIME_INDEX, _RUNTIME_INDEX_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _RUNTIME_INDEX = None
        _RUNTIME_INDEX_MTIME = -1
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(changed_rules)
        return True


def merge_learned_image_rules(blocked_map: dict, country_code: str = "") -> dict:
    """Merge active JSON rules into the URL/pHash matcher map."""
    merged = dict(blocked_map or {})
    with _LOCK:
        merged.update(load_learned_image_rule_index(country_code).get("by_key", {}))
    return merged


def load_learned_image_rule_index(country_code: str = "") -> dict:
    """Return compact URL/pHash lookups for validation runtime use."""
    global _RUNTIME_INDEX, _RUNTIME_INDEX_MTIME
    try:
        mtime = RULES_PATH.stat().st_mtime_ns
    except OSError:
        mtime = 0
    country_code = str(country_code or "").strip().upper()
    if _RUNTIME_INDEX is not None and _RUNTIME_INDEX_MTIME == (mtime, country_code):
        return _RUNTIME_INDEX
    # Validation callers consume the URL/pHash matcher only. Building two more
    # per-catalog aggregation dictionaries here duplicated tens of thousands
    # of strings/keys on every country-specific index build, while Dashboard
    # analytics already derive counts from the full catalog when requested.
    by_key = {}
    for rule in iter_learned_image_rules():
        if str(rule.get("status", "active")).lower() != "active":
            continue
        if not _rule_applies_in_country(rule, country_code):
            continue
        flag = normalize_learned_flag(rule.get("flag", ""))
        if flag not in LEARNABLE_FLAGS:
            continue
        entry = {
            "flag": flag,
            "brand": _brand_for_rule_country(rule, country_code),
            "brand_candidates": _brand_candidates_for_rule_country(rule, country_code),
            "url": rule.get("image_url", ""),
            "phash": rule.get("phash", ""),
            "comment": rule.get("comment", ""),
            "scope_categories": normalize_rule_values(rule.get("scope_categories")),
            "scope_brands": normalize_rule_values(rule.get("scope_brands")),
        }
        for key in (str(rule.get("image_url", "")).strip(), str(rule.get("phash", "")).strip()):
            if key:
                _old = by_key.get(key)
                if _old is None or _LEARNED_FLAG_PRIORITY.get(flag, 50) >= _LEARNED_FLAG_PRIORITY.get(str(_old.get("flag", "")), 50):
                    by_key[key] = entry
    _RUNTIME_INDEX = {"by_key": by_key, "flag_counts": {}, "seller_counts": {}}
    _RUNTIME_INDEX_MTIME = (mtime, country_code)
    # Validation keeps only the compact country-specific URL/pHash index.
    # Release the expanded catalog and Supabase response list; the Dashboard
    # reloads full rule details on demand, while validation reuses this index.
    global _RULES, _REMOTE_RULES
    _RULES = None
    _REMOTE_RULES = None
    return _RUNTIME_INDEX


def _brand_candidates_for_rule_country(rule: dict, country_code: str) -> list[str]:
    by_country = rule.get("brand_infringed_by_country", {})
    if isinstance(by_country, dict):
        values = by_country.get(str(country_code or "").strip().upper()) or by_country.get("*") or []
        if isinstance(values, str):
            values = [values]
        clean = list(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
        if clean:
            return clean
    fallback = str(rule.get("brand_infringed", "") or rule.get("brand", "")).strip()
    return [fallback] if fallback else []


def _brand_for_rule_country(rule: dict, country_code: str) -> str:
    return " / ".join(_brand_candidates_for_rule_country(rule, country_code))


def load_learned_image_rules_for_urls(urls) -> list[dict]:
    """Return learned rules for the supplied image URLs only.

    The review iframe needs rule provenance for the current page, not the
    complete catalog. On Supabase, query those URLs in batches; locally reuse
    the already cached JSON list and filter it in memory.
    """
    wanted = {
        str(url).strip()
        for url in (urls or [])
        if str(url).strip() and str(url).strip().lower() not in {"nan", "none"}
    }
    if not wanted:
        return []
    cache_key = tuple(sorted(wanted))
    cached = _PAGE_RULE_CACHE.get(cache_key)
    if cached is not None:
        _PAGE_RULE_CACHE.move_to_end(cache_key)
        return list(cached)
    if supabase_store.enabled():
        try:
            fields = ",".join(sorted(_REMOTE_RULE_COLUMNS))
            out = []
            values = list(wanted)
            for start in range(0, len(values), 100):
                chunk = values[start:start + 100]
                # URLs in the catalog do not contain commas. PostgREST handles
                # the URL encoding of the complete in.(...) expression.
                out.extend(supabase_store.fetch_where(
                    "learned_image_rules",
                    {"image_url": "in.(" + ",".join(chunk) + ")"},
                    select=fields,
                ))
            _PAGE_RULE_CACHE[cache_key] = list(out)
            _PAGE_RULE_CACHE.move_to_end(cache_key)
            while len(_PAGE_RULE_CACHE) > _PAGE_RULE_CACHE_MAX:
                _PAGE_RULE_CACHE.popitem(last=False)
            return list(out)
        except Exception as exc:
            _LOG.warning("Could not query learned rules for current images: %s", exc)
    with _LOCK:
        out = [
            rule for rule in iter_learned_image_rules()
            if str(rule.get("image_url", "")).strip() in wanted
        ]
        _PAGE_RULE_CACHE[cache_key] = list(out)
        _PAGE_RULE_CACHE.move_to_end(cache_key)
        while len(_PAGE_RULE_CACHE) > _PAGE_RULE_CACHE_MAX:
            _PAGE_RULE_CACHE.popitem(last=False)
        return out


def learn_image_rejections(
    data: pd.DataFrame,
    sids,
    flag: str,
    reason: str = "",
    comment: str = "",
    source: str = "validation",
    hash_by_url: dict | None = None,
    country_code: str = "",
) -> int:
    """Append unique image rules for rejected SIDs and return new-rule count."""
    flag = normalize_learned_flag(flag)
    if flag not in LEARNABLE_FLAGS or data is None or data.empty:
        return 0
    sid_values = [] if sids is None else sids
    sid_set = {str(s).strip() for s in sid_values}
    if not sid_set or "PRODUCT_SET_SID" not in data.columns:
        return 0
    image_col = next((c for c in ("MAIN_IMAGE", "IMAGE1", "image1", "IMAGE_URL") if c in data.columns), None)
    if not image_col:
        return 0
    hash_by_url = hash_by_url or {}
    subset = data[data["PRODUCT_SET_SID"].astype(str).str.strip().isin(sid_set)]
    _unique = subset.drop_duplicates(subset=["PRODUCT_SET_SID"]).copy()
    _unique["_learn_image_url"] = _unique[image_col].fillna("").astype(str).str.strip()
    _unique = _unique[~_unique["_learn_image_url"].str.casefold().isin({"", "nan", "none", "n/a"})]
    _created_at = datetime.now(timezone.utc).isoformat()
    _countries = _country_scope_for_learned_flag(flag)
    records = [{
            "image_url": url,
            "phash": str(hash_by_url.get(url, "") or "").strip(),
            "flag": flag,
            "reason": str(reason or ""),
            "comment": str(comment or ""),
            "brand": str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip(),
            "brand_infringed": str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip(),
            "brand_infringed_by_country": ({str(country_code).strip().upper(): [str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip()]} if country_code else {}),
            "category_code": str(row.get("CATEGORY_CODE", "") or "").strip(),
            "product_set_sid": str(row.get("PRODUCT_SET_SID", "")).strip(),
            "seller_name": str(row.get("SELLER_NAME", "") or "").strip(),
            "status": "active",
            "source": source,
            "created_at": _created_at,
            "last_matched_at": "",
            "last_confirmed_at": "",
            "review_due_at": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
            "countries": _countries,
            "scope_categories": [],
            "scope_brands": [],
            "match_count": 0,
            "matched_sids": [],
            "matched_sellers": [],
            "impact_truncated": False,
        } for row in _unique.to_dict("records")
        if flag != "Restricted brands" or str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip()
        for url in [str(row.get("_learn_image_url", "")).strip()]]
    if not records:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        # The dedupe scope is image identity + canonical reason.  Never use
        # image identity alone: one image may legitimately be learned for
        # different reasons (for example Poor images and Restricted brands).
        keys = {
            (
                str(r.get("image_url", "")).strip(),
                str(r.get("phash", "")).strip(),
                normalize_learned_flag(r.get("flag", "")),
            )
            for r in current
        }
        url_keys = {(url, flag) for url, _phash, flag in keys if url}
        phash_keys = {(_phash, flag) for _url, _phash, flag in keys if _phash}
        additions = []
        changed_rules = []
        for record in records:
            key = (record["image_url"], record["phash"], record["flag"])
            _record_brand = str(record.get("brand_infringed", "") or "").strip().casefold()
            existing = next((item for item in current
                if normalize_learned_flag(item.get("flag", "")) == record["flag"]
                and (record["flag"] != "Restricted brands" or
                     str(item.get("brand_infringed", item.get("brand", "")) or "").strip().casefold() == _record_brand)
                and ((record["image_url"] and str(item.get("image_url", "")).strip() == record["image_url"])
                     or (record["phash"] and str(item.get("phash", "")).strip().casefold() == record["phash"].casefold()))), None)
            if existing is not None:
                old_countries = normalize_rule_countries(existing.get("countries"), legacy_global=True)
                desired_countries = _country_scope_for_learned_flag(record["flag"])
                if record["flag"] == "Restricted brands":
                    _brands_by_country = existing.setdefault("brand_infringed_by_country", {})
                    if not isinstance(_brands_by_country, dict):
                        _brands_by_country = {}
                        existing["brand_infringed_by_country"] = _brands_by_country
                    _brand_country = str(country_code or "*").strip().upper()
                    _brand_values = _brands_by_country.setdefault(_brand_country, [])
                    if isinstance(_brand_values, str):
                        _brand_values = [_brand_values]
                    _new_brand = str(record.get("brand_infringed", "") or "").strip()
                    if _new_brand and _new_brand.casefold() not in {str(v).casefold() for v in _brand_values}:
                        _brand_values.append(_new_brand)
                        existing["brand_infringed_by_country"][_brand_country] = _brand_values
                        changed_rules.append(existing)
                if desired_countries != old_countries:
                    existing["countries"] = desired_countries
                    changed_rules.append(existing)
                continue
            keys.add(key)
            url_keys.add((record["image_url"], record["flag"]))
            if record["phash"]:
                phash_keys.add((record["phash"], record["flag"]))
            additions.append(record)
        if not additions and not changed_rules:
            return 0
        payload = current + additions
        _assert_no_rules_lost(existing_identities, payload)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = payload
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(additions + list({id(item): item for item in changed_rules}.values()))
        return len(additions)


def learn_image_rejections_async(*args, **kwargs):
    """Queue an iframe learning write without delaying the UI rerun."""
    return _WRITER.submit(learn_image_rejections, *args, **kwargs)


def record_learned_image_matches(matches, country_code: str = "") -> int:
    """Track unique matched SKUs/sellers and refresh rule recency metadata."""
    if not matches:
        return 0
    by_rule = {}
    for sid, item in matches.items():
        if not isinstance(item, dict):
            continue
        rule_url = str(item.get("matched_rule_url", "") or "").strip()
        rule_phash = str(item.get("matched_rule_phash", "") or "").strip().casefold()
        if not rule_url and not rule_phash:
            continue
        key = (rule_url, rule_phash)
        bucket = by_rule.setdefault(key, {"sids": set(), "sellers": set()})
        clean_sid = str(sid or "").strip()
        clean_seller = str(item.get("seller_name", "") or "").strip()
        if clean_sid:
            bucket["sids"].add(clean_sid)
        if clean_seller:
            bucket["sellers"].add(clean_seller)
    if not by_rule:
        return 0
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    sid_cap, seller_cap = 500, 100
    country_code = str(country_code or "").strip().upper()
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        changed = 0
        for rule in current:
            if not _rule_applies_in_country(rule, country_code):
                continue
            url = str(rule.get("image_url", "")).strip()
            phash = str(rule.get("phash", "")).strip().casefold()
            bucket = by_rule.get((url, phash)) or by_rule.get((url, "")) or by_rule.get(("", phash))
            if bucket is None:
                continue
            old_sids = list(dict.fromkeys(str(value).strip() for value in (rule.get("matched_sids") or []) if str(value).strip()))
            old_sellers = list(dict.fromkeys(str(value).strip() for value in (rule.get("matched_sellers") or []) if str(value).strip()))
            merged_sids = list(dict.fromkeys(old_sids + sorted(bucket["sids"])))
            merged_sellers = list(dict.fromkeys(old_sellers + sorted(bucket["sellers"])))
            new_sids = merged_sids[:sid_cap]
            new_sellers = merged_sellers[:seller_cap]
            if new_sids != old_sids[:sid_cap] or new_sellers != old_sellers[:seller_cap]:
                rule["matched_sids"] = new_sids
                rule["matched_sellers"] = new_sellers
                rule["match_count"] = len(new_sids)
                rule["impact_truncated"] = len(merged_sids) > sid_cap or len(merged_sellers) > seller_cap
                changed += 1
            previous = str(rule.get("last_matched_at", "") or "").strip()
            try:
                previous_dt = datetime.fromisoformat(previous.replace("Z", "+00:00")) if previous else None
                if previous_dt is not None and previous_dt.tzinfo is None:
                    previous_dt = previous_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                previous_dt = None
            if previous_dt is None or previous_dt.date() < now_dt.date():
                rule["last_matched_at"] = now
                changed += 1
        if not changed:
            return 0
        _assert_no_rules_lost(existing_identities, current)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        changed_rules = [rule for rule in current if (
            (str(rule.get("image_url", "")).strip(), str(rule.get("phash", "")).strip().casefold()) in by_rule
            or (str(rule.get("image_url", "")).strip(), "") in by_rule
            or ("", str(rule.get("phash", "")).strip().casefold()) in by_rule
        )]
        _remote_upsert_rules(changed_rules)
        return changed


def record_learned_image_matches_async(matches, country_code: str = ""):
    return _WRITER.submit(record_learned_image_matches, matches, country_code)


def learn_image_rejections_bulk(batches, progress_callback=None) -> int:
    """Import many report groups with one JSON write and one SQLite sync.

    ``batches`` contains dictionaries with ``data``, ``sids``, ``flag``,
    ``reason``, ``source`` and optional ``hash_by_url`` values. This avoids
    rebuilding the full catalog after every file or rejection reason.
    """
    all_records = []
    seen = {}
    batches = list(batches or [])
    for batch_index, batch in enumerate(batches, start=1):
        data = batch.get("data") if isinstance(batch, dict) else None
        flag = normalize_learned_flag(batch.get("flag", "") if isinstance(batch, dict) else "")
        if flag not in LEARNABLE_FLAGS or data is None or data.empty or "PRODUCT_SET_SID" not in data.columns:
            if progress_callback:
                progress_callback(batch_index, len(batches), batch)
            continue
        _batch_sids = batch.get("sids", [])
        if _batch_sids is None:
            _batch_sids = []
        sid_set = {
            str(s).strip()
            for s in _batch_sids
            if str(s).strip().casefold() not in {"", "nan", "none", "null"}
        }
        image_col = next((c for c in ("MAIN_IMAGE", "IMAGE1", "image1", "IMAGE_URL") if c in data.columns), None)
        if not sid_set or not image_col:
            if progress_callback:
                progress_callback(batch_index, len(batches), batch)
            continue
        hash_by_url = batch.get("hash_by_url", {}) or {}
        _country = str(batch.get("country_code", "") or "").strip().upper()
        _countries = _country_scope_for_learned_flag(flag)
        subset = data[data["PRODUCT_SET_SID"].astype(str).str.strip().isin(sid_set)]
        unique = subset.drop_duplicates(subset=["PRODUCT_SET_SID"]).copy()
        unique["_learn_image_url"] = unique[image_col].fillna("").astype(str).str.strip()
        unique = unique[~unique["_learn_image_url"].str.casefold().isin({"", "nan", "none", "n/a"})]
        for row in unique.to_dict("records"):
            url = str(row.get("_learn_image_url", "")).strip()
            _brand_infringed = str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip()
            if flag == "Restricted brands" and not _brand_infringed:
                continue
            _created_at = datetime.now(timezone.utc)
            record = {
                "image_url": url,
                "phash": str(hash_by_url.get(url, "") or "").strip(),
                "flag": flag,
                "reason": str(batch.get("reason", "") or ""),
                "comment": str(batch.get("comment", "") or ""),
                "brand": _brand_infringed,
                "brand_infringed": _brand_infringed,
                "brand_infringed_by_country": ({_country or "*": [_brand_infringed]} if _brand_infringed else {}),
                "category_code": str(row.get("CATEGORY_CODE", "") or "").strip(),
                "product_set_sid": str(row.get("PRODUCT_SET_SID", "")).strip(),
                "seller_name": str(row.get("SELLER_NAME", "") or "").strip(),
                "status": "active",
                "source": str(batch.get("source", "validation")),
                "created_at": _created_at.isoformat(),
                "last_matched_at": "",
                "last_confirmed_at": "",
                "review_due_at": (_created_at + timedelta(days=30)).isoformat(),
                "countries": _countries,
                "scope_categories": normalize_rule_values(batch.get("scope_categories")),
                "scope_brands": normalize_rule_values(batch.get("scope_brands")),
                "match_count": 0,
                "matched_sids": [],
                "matched_sellers": [],
                "impact_truncated": False,
            }
            # Collapse repeated rows within the same import using the same
            # URL/pHash + reason key used against the existing catalog. A
            # different flag for the same image remains a valid addition.
            identity = (record["image_url"], record["phash"], record["flag"])
            pending = seen.get(identity)
            if pending is None:
                seen[identity] = record
                all_records.append(record)
                continue
            # One durable image/reason row can track country-specific brands
            # without broadening the actual restriction to another market.
            pending["countries"] = _country_scope_for_learned_flag(flag)
            pending_brands = pending.setdefault("brand_infringed_by_country", {})
            for code, brands in (record.get("brand_infringed_by_country") or {}).items():
                current_brands = pending_brands.setdefault(code, [])
                for brand in (brands if isinstance(brands, list) else [brands]):
                    if brand and str(brand).casefold() not in {str(value).casefold() for value in current_brands}:
                        current_brands.append(brand)
        if progress_callback:
            progress_callback(batch_index, len(batches), batch)
    if not all_records:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        existing_identities = _rule_identity_counts(current)
        additions = []
        changed_rules = []
        for record in all_records:
            existing = next((item for item in current
                if normalize_learned_flag(item.get("flag", "")) == record["flag"]
                and ((record["image_url"] and str(item.get("image_url", "")).strip() == record["image_url"])
                     or (record["phash"] and str(item.get("phash", "")).strip().casefold() == record["phash"].casefold()))), None)
            if existing is not None:
                old_countries = normalize_rule_countries(existing.get("countries"), legacy_global=True)
                desired_countries = _country_scope_for_learned_flag(record["flag"])
                if record["flag"] == "Restricted brands":
                    existing_brands = existing.setdefault("brand_infringed_by_country", {})
                    for country, brands in (record.get("brand_infringed_by_country") or {}).items():
                        target = existing_brands.setdefault(country, [])
                        if isinstance(target, str):
                            target = [target]
                            existing_brands[country] = target
                        for brand in (brands if isinstance(brands, list) else [brands]):
                            if brand and str(brand).casefold() not in {str(value).casefold() for value in target}:
                                target.append(brand)
                                changed_rules.append(existing)
                if desired_countries != old_countries:
                    existing["countries"] = desired_countries
                    changed_rules.append(existing)
                continue
            additions.append(record)
        if not additions and not changed_rules:
            return 0
        payload = current + additions
        _assert_no_rules_lost(existing_identities, payload)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        _atomic_replace_rules(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = payload
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(additions + list({id(item): item for item in changed_rules}.values()))
        return len(additions)


def remove_image_rules_for_sids(
    data: pd.DataFrame,
    sids,
    flag: str,
    hash_by_url: dict | None = None,
) -> int:
    """Deprecated no-op: approval/re-evaluation never deletes learned rules.

    Learned rules are durable knowledge. Removing one must happen through an
    explicit Dashboard action, which archives the record first.
    """
    return 0


def reconcile_image_rules(
    data: pd.DataFrame,
    flagged_sids,
    flag: str,
    hash_by_url: dict | None = None,
    evaluated_sids=None,
) -> int:
    """Keep learned rules unchanged when validation later approves a product.

    A validation result can be transient or based on changed code. It must
    never erase learned knowledge; use the explicit Dashboard removal action.
    Kept as a compatibility function because the report pipeline still calls it.
    """
    return 0
