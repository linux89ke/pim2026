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
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import supabase_store

RULES_PATH = Path(__file__).resolve().parent / "learned_image_rules.json"
LEARNABLE_FLAGS = frozenset({
    "Poor images", "Image Stretched", "Image Blurry", "Image Mismatch",
    "Image Infringing", "Image Too Many things displayed",
    "Restricted brands", "Suspected Fake product",
    "Brand Image Mismatch", "Counterfeit Sneakers",
    "Suspected counterfeit Jerseys",
})
FLAG_ALIASES = {
    "poor image": "Poor images", "poor images": "Poor images",
    "poor image quality": "Poor images",
    "image stretched": "Image Stretched",
    "image blurry": "Image Blurry",
    "image mismatch": "Image Mismatch",
    "image infringing": "Image Infringing",
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
}

_LOCK = threading.RLock()
_RULES_MTIME = -1.0
_RULES = []
_WRITER = ThreadPoolExecutor(max_workers=1, thread_name_prefix="learned-rules")
_REMOTE_RULES = None
_REMOTE_RULES_FETCHED_AT = 0.0
_REMOTE_RULES_TTL = 60.0
_PAGE_RULE_CACHE = OrderedDict()
_PAGE_RULE_CACHE_MAX = 64
_LOG = logging.getLogger(__name__)


def _clear_page_rule_cache() -> None:
    _PAGE_RULE_CACHE.clear()

def _sync_sqlite_locked(rules: list) -> None:
    """Compatibility no-op for callers from the pre-JSON-only implementation."""
    return None


def normalize_learned_flag(value: str) -> str:
    """Map report spelling variants to one exact canonical flag."""
    raw = re.sub(r"\s*\(prefetched\)\s*$", "", str(value or "").strip(), flags=re.IGNORECASE)
    raw = re.sub(r"\s*-\s*blocked fingerprint\s*$", "", raw, flags=re.IGNORECASE)
    return FLAG_ALIASES.get(raw.casefold(), raw)


def _load_locked() -> list:
    global _RULES_MTIME, _RULES
    try:
        # Nanosecond precision matters when two browser tabs write the same
        # catalog within one filesystem second. Second-resolution mtimes could
        # make the other tab keep serving its stale in-memory rule list.
        mtime = RULES_PATH.stat().st_mtime_ns
    except OSError:
        mtime = 0.0
    if mtime == _RULES_MTIME:
        return _RULES
    _clear_page_rule_cache()
    try:
        with RULES_PATH.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
        _RULES = loaded if isinstance(loaded, list) else []
    except (OSError, ValueError, TypeError):
        _RULES = []
    _RULES_MTIME = mtime
    return _RULES


def load_learned_image_rules() -> list:
    global _REMOTE_RULES, _REMOTE_RULES_FETCHED_AT
    if supabase_store.enabled():
        now = time.monotonic()
        if _REMOTE_RULES is None or now - _REMOTE_RULES_FETCHED_AT >= _REMOTE_RULES_TTL:
            try:
                _REMOTE_RULES = supabase_store.fetch_all("learned_image_rules")
                _REMOTE_RULES_FETCHED_AT = now
            except Exception as exc:
                _LOG.warning("Could not read Supabase learned rules; using local fallback: %s", exc)
                _REMOTE_RULES = None
        if _REMOTE_RULES is not None:
            return list(_REMOTE_RULES)
    with _LOCK:
        return list(_load_locked())


_REMOTE_RULE_COLUMNS = {
    "image_url", "phash", "flag", "reason", "comment", "brand_infringed",
    "category_code", "product_set_sid", "seller_name", "status", "review_state",
    "source", "created_at", "updated_at", "last_matched_at", "last_confirmed_at",
    "review_due_at",
}


def _remote_upsert_rules(records: list[dict]) -> None:
    if not records or not supabase_store.enabled():
        return
    now = supabase_store.now_iso()
    payload = []
    for record in records:
        row = {key: record.get(key) for key in _REMOTE_RULE_COLUMNS}
        row["updated_at"] = row.get("updated_at") or now
        row["created_at"] = row.get("created_at") or now
        for field in ("last_matched_at", "last_confirmed_at", "review_due_at"):
            if row.get(field) == "":
                row[field] = None
        payload.append(row)
    try:
        supabase_store.upsert("learned_image_rules", payload, on_conflict="image_url,flag")
        global _REMOTE_RULES, _REMOTE_RULES_FETCHED_AT
        _REMOTE_RULES = None
        _REMOTE_RULES_FETCHED_AT = 0.0
        _clear_page_rule_cache()
    except Exception as exc:
        _LOG.warning("Could not write learned rules to Supabase: %s", exc)


def _remote_delete_rules(records: list[dict]) -> None:
    if not records or not supabase_store.enabled():
        return
    try:
        for record in records:
            filters = {}
            if record.get("id") is not None:
                filters["id"] = record.get("id")
            else:
                for field in ("image_url", "phash", "flag", "created_at"):
                    value = str(record.get(field, "") or "").strip()
                    if value:
                        filters[field] = value
            if filters:
                supabase_store.delete_where("learned_image_rules", filters)
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
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules(removed_records)
        return removed


def remove_learned_rules_by_flag(flag: str) -> int:
    """Remove all learned image rules for a flag that is now validator-owned."""
    canonical = normalize_learned_flag(flag)
    with _LOCK:
        current = list(load_learned_image_rules())
        kept = [rule for rule in current if normalize_learned_flag(rule.get("flag", "")) != canonical]
        removed_records = [rule for rule in current if normalize_learned_flag(rule.get("flag", "")) == canonical]
        removed = len(current) - len(kept)
        if not removed:
            return 0
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
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
        keys = {(str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), str(r.get("created_at", "")).strip()) for r in current}
        new_items = [r for r in additions if (str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), str(r.get("created_at", "")).strip()) not in keys]
        if not new_items:
            return 0
        current.extend(new_items)
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(new_items)
        return len(new_items)


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
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
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
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
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


def merge_learned_image_rules(blocked_map: dict) -> dict:
    """Merge active JSON rules into the URL/pHash matcher map."""
    merged = dict(blocked_map or {})
    with _LOCK:
        for rule in load_learned_image_rules():
            if str(rule.get("status", "active")).lower() != "active":
                continue
            if normalize_learned_flag(rule.get("flag", "")) not in LEARNABLE_FLAGS:
                continue
            entry = {
                "flag": rule.get("flag", "Poor images"),
                "brand": rule.get("brand_infringed", "") or rule.get("brand", ""),
                "url": rule.get("image_url", ""),
                "phash": rule.get("phash", ""),
                "source": "json",
            }
            url = str(rule.get("image_url", "")).strip()
            phash = str(rule.get("phash", "")).strip()
            if url:
                merged[url] = entry
            if phash:
                merged[phash] = entry
    return merged


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
            rule for rule in load_learned_image_rules()
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
    records = [{
            "image_url": url,
            "phash": str(hash_by_url.get(url, "") or "").strip(),
            "flag": flag,
            "reason": str(reason or ""),
            "comment": str(comment or ""),
            "brand": str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip(),
            "brand_infringed": str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip(),
            "category_code": str(row.get("CATEGORY_CODE", "") or "").strip(),
            "product_set_sid": str(row.get("PRODUCT_SET_SID", "")).strip(),
            "seller_name": str(row.get("SELLER_NAME", "") or "").strip(),
            "status": "active",
            "source": source,
            "created_at": _created_at,
            "last_matched_at": "",
            "last_confirmed_at": "",
            "review_due_at": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
        } for row in _unique.to_dict("records")
        if flag != "Restricted brands" or str(row.get("BRAND_INFRINGED", row.get("BRAND", "")) or "").strip()
        for url in [str(row.get("_learn_image_url", "")).strip()]]
    if not records:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        keys = {(str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), r.get("flag", "")) for r in current}
        url_keys = {(url, flag) for url, _phash, flag in keys if url}
        phash_keys = {(_phash, flag) for _url, _phash, flag in keys if _phash}
        additions = []
        for record in records:
            key = (record["image_url"], record["phash"], record["flag"])
            if key in keys or (record["image_url"], record["flag"]) in url_keys or (
                record["phash"] and (record["phash"], record["flag"]) in phash_keys
            ):
                continue
            keys.add(key)
            url_keys.add((record["image_url"], record["flag"]))
            if record["phash"]:
                phash_keys.add((record["phash"], record["flag"]))
            additions.append(record)
        if not additions:
            return 0
        payload = current + additions
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = payload
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(additions)
        return len(additions)


def learn_image_rejections_async(*args, **kwargs):
    """Queue an iframe learning write without delaying the UI rerun."""
    return _WRITER.submit(learn_image_rejections, *args, **kwargs)


def record_learned_image_matches(matches) -> int:
    """Update match timestamps at most once per rule per day.

    Matching happens during validation, so writing the entire JSON catalog for
    every run would recreate the old performance problem even without SQLite.
    A daily timestamp is enough for lifecycle reporting and keeps validation
    reads effectively read-only.
    """
    if not matches:
        return 0
    urls = {str(item.get("url", "")).strip() for item in matches.values() if isinstance(item, dict) and item.get("url")}
    phashes = {str(item.get("phash", "")).strip() for item in matches.values() if isinstance(item, dict) and item.get("phash")}
    if not urls and not phashes:
        return 0
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    cutoff = now_dt - timedelta(days=1)
    with _LOCK:
        current = list(load_learned_image_rules())
        changed = 0
        for rule in current:
            if str(rule.get("image_url", "")).strip() in urls or str(rule.get("phash", "")).strip() in phashes:
                previous = str(rule.get("last_matched_at", "") or "").strip()
                if previous:
                    try:
                        previous_dt = datetime.fromisoformat(previous.replace("Z", "+00:00"))
                        if previous_dt.tzinfo is None:
                            previous_dt = previous_dt.replace(tzinfo=timezone.utc)
                        if previous_dt >= cutoff:
                            continue
                    except ValueError:
                        pass
                rule["last_matched_at"] = now
                changed += 1
        if not changed:
            return 0
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = current
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        changed_rules = [rule for rule in current if str(rule.get("last_matched_at", "")) == now]
        _remote_upsert_rules(changed_rules)
        return changed


def record_learned_image_matches_async(matches):
    return _WRITER.submit(record_learned_image_matches, matches)


def learn_image_rejections_bulk(batches, progress_callback=None) -> int:
    """Import many report groups with one JSON write and one SQLite sync.

    ``batches`` contains dictionaries with ``data``, ``sids``, ``flag``,
    ``reason``, ``source`` and optional ``hash_by_url`` values. This avoids
    rebuilding the full catalog after every file or rejection reason.
    """
    all_records = []
    seen = set()
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
                "category_code": str(row.get("CATEGORY_CODE", "") or "").strip(),
                "product_set_sid": str(row.get("PRODUCT_SET_SID", "")).strip(),
                "seller_name": str(row.get("SELLER_NAME", "") or "").strip(),
                "status": "active",
                "source": str(batch.get("source", "validation")),
                "created_at": _created_at.isoformat(),
                "last_matched_at": "",
                "last_confirmed_at": "",
                "review_due_at": (_created_at + timedelta(days=30)).isoformat(),
            }
            identity = (record["image_url"], record["phash"], record["flag"])
            if identity not in seen:
                seen.add(identity)
                all_records.append(record)
        if progress_callback:
            progress_callback(batch_index, len(batches), batch)
    if not all_records:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        keys = {(str(r.get("image_url", "")).strip(), str(r.get("phash", "")).strip(), normalize_learned_flag(r.get("flag", ""))) for r in current}
        url_keys = {(url, flag) for url, _phash, flag in keys if url}
        phash_keys = {(_phash, flag) for _url, _phash, flag in keys if _phash}
        additions = []
        for record in all_records:
            key = (record["image_url"], record["phash"], record["flag"])
            if key in keys or (record["image_url"], record["flag"]) in url_keys or (record["phash"] and (record["phash"], record["flag"]) in phash_keys):
                continue
            keys.add(key)
            url_keys.add((record["image_url"], record["flag"]))
            if record["phash"]:
                phash_keys.add((record["phash"], record["flag"]))
            additions.append(record)
        if not additions:
            return 0
        payload = current + additions
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = payload
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_upsert_rules(additions)
        return len(additions)


def remove_image_rules_for_sids(
    data: pd.DataFrame,
    sids,
    flag: str,
    hash_by_url: dict | None = None,
) -> int:
    """Remove rules for images that now pass the named validation."""
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
    urls = {str(v).strip() for v in subset[image_col] if str(v).strip() and str(v).strip().lower() not in {"nan", "none", "n/a"}}
    phashes = {str(hash_by_url.get(url, "")).strip() for url in urls if hash_by_url.get(url)}
    if not urls and not phashes:
        return 0
    with _LOCK:
        current = list(load_learned_image_rules())
        kept = []
        removed = 0
        for rule in current:
            same_flag = normalize_learned_flag(rule.get("flag", "")) == flag
            same_image = str(rule.get("image_url", "")).strip() in urls or str(rule.get("phash", "")).strip() in phashes
            if same_flag and same_image:
                removed += 1
            else:
                kept.append(rule)
        if not removed:
            return 0
        temp = RULES_PATH.with_suffix(".json.tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(kept, handle, indent=2, ensure_ascii=False)
        os.replace(temp, RULES_PATH)
        global _RULES, _RULES_MTIME
        _RULES = kept
        _RULES_MTIME = RULES_PATH.stat().st_mtime_ns
        _sync_sqlite_locked(_RULES)
        _remote_delete_rules([
            rule for rule in current
            if rule not in kept and normalize_learned_flag(rule.get("flag", "")) == flag
        ])
        return removed


def reconcile_image_rules(
    data: pd.DataFrame,
    flagged_sids,
    flag: str,
    hash_by_url: dict | None = None,
    evaluated_sids=None,
) -> int:
    """Remove stale rules for evaluated products, then leave flagged rules intact."""
    if data is None or data.empty or "PRODUCT_SET_SID" not in data.columns:
        return 0
    all_sids = set(data["PRODUCT_SET_SID"].astype(str).str.strip()) if evaluated_sids is None else {
        str(s).strip() for s in evaluated_sids
    }
    flagged_values = [] if flagged_sids is None else flagged_sids
    flagged = {str(s).strip() for s in flagged_values}
    return remove_image_rules_for_sids(data, all_sids - flagged, flag, hash_by_url=hash_by_url)
