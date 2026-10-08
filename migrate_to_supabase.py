"""Import the local learned catalogs into Supabase.

This is a one-time (or repeatable) migration utility. It uses Supabase's
PostgREST endpoint, so it does not add another runtime dependency beyond the
requests package already used by the app.

PowerShell example:
    $env:SUPABASE_URL = "https://your-project.supabase.co"
    $env:SUPABASE_SERVICE_ROLE_KEY = "sb_secret_..."
    python migrate_to_supabase.py

Use --replace-categories when rerunning the category import. It clears the
two category tables first, then imports the current SQLite contents exactly
once. Learned image rules use an upsert on image_url + flag.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests


ROOT = Path(__file__).resolve().parent
DEFAULT_RULES = ROOT / "learned_image_rules.json"
DEFAULT_CATEGORY_DB = ROOT / "cat_learning.db"


def _config(args: argparse.Namespace) -> tuple[str, str]:
    url = (args.url or os.getenv("SUPABASE_URL", "")).strip().rstrip("/")
    key = (
        args.key
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
        or os.getenv("SUPABASE_SECRET_KEY", "")
    ).strip()
    if not url or not key:
        raise SystemExit(
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or pass --url and --key)."
        )
    return url, key


def _headers(key: str, *, upsert: bool = False) -> dict[str, str]:
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if upsert:
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    else:
        headers["Prefer"] = "return=minimal"
    return headers


def _chunks(rows: Iterable[dict], size: int) -> Iterable[list[dict]]:
    batch: list[dict] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _post_batch(
    session: requests.Session,
    endpoint: str,
    rows: list[dict],
    key: str,
    *,
    on_conflict: str | None = None,
) -> None:
    params = {"on_conflict": on_conflict} if on_conflict else None
    response = session.post(
        endpoint,
        params=params,
        headers=_headers(key, upsert=bool(on_conflict)),
        json=rows,
        timeout=120,
    )
    if not response.ok:
        detail = response.text[:2_000]
        raise RuntimeError(f"Supabase request failed ({response.status_code}): {detail}")


def _delete_all(session: requests.Session, endpoint: str, key: str) -> None:
    # PostgREST requires a filter on DELETE. Every row has a non-null id.
    response = session.delete(
        endpoint,
        params={"id": "not.is.null"},
        headers=_headers(key),
        timeout=120,
    )
    if not response.ok:
        detail = response.text[:2_000]
        raise RuntimeError(f"Could not clear {endpoint}: {response.status_code}: {detail}")


def _clean(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text and text.casefold() not in {"nan", "none", "null"} else None


def _clean_url(value) -> str | None:
    """Return a URL, unwrapping Markdown links copied from reports."""
    text = _clean(value)
    if not text:
        return None
    match = re.fullmatch(r"\[([^\]]+)\]\((https?://[^)]+)\)", text)
    return match.group(2).strip() if match else text


def import_image_rules(
    session: requests.Session,
    endpoint: str,
    key: str,
    path: Path,
    batch_size: int,
    allow_small: bool = False,
) -> int:
    with path.open("r", encoding="utf-8") as handle:
        source = json.load(handle)
    if not isinstance(source, list):
        raise ValueError(f"{path} must contain a JSON list")
    valid_count = sum(
        1 for raw in source
        if isinstance(raw, dict) and _clean_url(raw.get("image_url")) and _clean(raw.get("flag"))
    )
    if valid_count < 100 and not allow_small:
        raise ValueError(
            f"Refusing to migrate only {valid_count:,} learned rules from {path}. "
            "This may be a partial catalog; pass --allow-small-rules only after confirming it."
        )

    def rows():
        for raw in source:
            if not isinstance(raw, dict):
                continue
            image_url = _clean_url(raw.get("image_url"))
            flag = _clean(raw.get("flag"))
            if not image_url or not flag:
                continue
            now = datetime.now(timezone.utc).isoformat()
            created_at = raw.get("created_at") or now
            item = {
                "image_url": image_url,
                "phash": _clean(raw.get("phash")),
                "flag": flag,
                "reason": _clean(raw.get("reason")),
                "comment": _clean(raw.get("comment")),
                "brand_infringed": _clean(raw.get("brand_infringed") or raw.get("brand")),
                "category_code": _clean(raw.get("category_code")),
                "product_set_sid": _clean(raw.get("product_set_sid")),
                "seller_name": _clean(raw.get("seller_name")),
                "status": _clean(raw.get("status")) or "active",
                "review_state": _clean(raw.get("review_state")) or "Unreviewed",
                "source": _clean(raw.get("source")),
                "created_at": created_at,
                "updated_at": raw.get("updated_at") or created_at,
                "last_matched_at": raw.get("last_matched_at") or None,
                "last_confirmed_at": raw.get("last_confirmed_at") or None,
                "review_due_at": raw.get("review_due_at") or None,
            }
            yield item

    imported = 0
    seen = set()
    for batch in _chunks(rows(), batch_size):
        unique = []
        for item in batch:
            identity = (item.get("phash") or item.get("image_url"), item.get("flag"))
            if identity not in seen:
                seen.add(identity)
                unique.append(item)
        hashed = [item for item in unique if item.get("phash")]
        unhashed = [item for item in unique if not item.get("phash")]
        if hashed:
            _post_batch(session, endpoint, hashed, key, on_conflict="phash,flag")
            imported += len(hashed)
        for item in unhashed:
            try:
                _post_batch(session, endpoint, [item], key)
                imported += 1
            except RuntimeError as exc:
                if "409" not in str(exc) and "duplicate" not in str(exc).lower():
                    raise
        print(f"Image rules: {imported:,} uploaded", flush=True)
    return imported


def _category_rows(db: sqlite3.Connection, table: str):
    query = (
        "select name, category, timestamp from category_corrections"
        if table == "category_corrections"
        else "select name, category, reason, timestamp from category_negatives"
    )
    cursor = db.execute(query)
    seen: set[tuple] = set()
    for row in cursor:
        values = tuple(_clean(value) or "" for value in row)
        # The local database has no unique constraint. Deduplicate during
        # migration so repeated learning events do not inflate the cloud data.
        key = values[:2] if table == "category_corrections" else values[:3]
        if key in seen:
            continue
        seen.add(key)
        if table == "category_corrections":
            name, category, timestamp = values
            if name and category:
                yield {"name": name, "category": category, "learned_at": timestamp or None}
        else:
            name, category, reason, timestamp = values
            if name and category:
                yield {
                    "name": name,
                    "category": category,
                    "reason": reason or None,
                    "learned_at": timestamp or None,
                }


def import_categories(
    session: requests.Session,
    base_url: str,
    key: str,
    path: Path,
    batch_size: int,
    replace: bool,
) -> tuple[int, int]:
    db = sqlite3.connect(path)
    counts: list[int] = []
    try:
        for table in ("category_corrections", "category_negatives"):
            endpoint = f"{base_url}/rest/v1/{table}"
            if replace:
                print(f"Clearing {table}...", flush=True)
                _delete_all(session, endpoint, key)
            count = 0
            for batch in _chunks(_category_rows(db, table), batch_size):
                _post_batch(session, endpoint, batch, key)
                count += len(batch)
                print(f"{table}: {count:,} uploaded", flush=True)
            counts.append(count)
    finally:
        db.close()
    return counts[0], counts[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Supabase project URL")
    parser.add_argument("--key", help="Supabase secret/service-role key")
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--category-db", type=Path, default=DEFAULT_CATEGORY_DB)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--replace-categories",
        action="store_true",
        help="Clear category tables before importing them",
    )
    parser.add_argument("--skip-rules", action="store_true")
    parser.add_argument("--skip-categories", action="store_true")
    parser.add_argument(
        "--allow-small-rules",
        action="store_true",
        help="Allow migration of fewer than 100 learned image rules after manual review",
    )
    args = parser.parse_args()

    if args.batch_size < 1 or args.batch_size > 2_000:
        parser.error("--batch-size must be between 1 and 2000")
    url, key = _config(args)
    session = requests.Session()

    try:
        if not args.skip_rules:
            import_image_rules(
                session,
                f"{url}/rest/v1/learned_image_rules",
                key,
                args.rules,
                args.batch_size,
                args.allow_small_rules,
            )
        if not args.skip_categories:
            import_categories(
                session,
                url,
                key,
                args.category_db,
                args.batch_size,
                args.replace_categories,
            )
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1
    print("Migration complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
