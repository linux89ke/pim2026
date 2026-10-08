"""Small optional Supabase REST client used by the local/cloud app.

The app remains fully local when no Supabase secrets are configured. On
Streamlit Cloud the same functions use values from st.secrets, so learned
rules survive container restarts.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)
_session = requests.Session()


def _secret(name: str) -> str:
    value = os.getenv(name, "")
    if value:
        return str(value).strip()
    try:
        import streamlit as st
        return str(st.secrets.get(name, "") or "").strip()
    except Exception:
        return ""


def config() -> tuple[str, str]:
    url = _secret("SUPABASE_URL").rstrip("/")
    key = _secret("SUPABASE_SERVICE_ROLE_KEY") or _secret("SUPABASE_SECRET_KEY")
    return url, key


def enabled() -> bool:
    url, key = config()
    return bool(url and key)


def _headers(prefer: str = "return=minimal") -> dict[str, str]:
    _url, key = config()
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _endpoint(table: str) -> str:
    url, _key = config()
    return f"{url}/rest/v1/{table}"


def fetch_all(table: str, *, page_size: int = 1000, order: str = "id") -> list[dict]:
    """Fetch a table through PostgREST pagination."""
    if not enabled():
        return []
    rows: list[dict] = []
    offset = 0
    while True:
        response = _session.get(
            _endpoint(table),
            params={"select": "*", "order": f"{order}.asc", "limit": page_size, "offset": offset},
            headers=_headers(),
            timeout=120,
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected Supabase response for {table}")
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def fetch_where(table: str, params: dict[str, str], *, select: str = "*", page_size: int = 1000) -> list[dict]:
    """Fetch only rows matching PostgREST filters.

    This is intentionally separate from ``fetch_all``. Large reference tables
    such as learned image rules should be queried by the current upload's URLs
    or hashes instead of downloading the entire table on every review rerun.
    """
    if not enabled():
        return []
    rows: list[dict] = []
    offset = 0
    base_params = dict(params or {})
    base_params["select"] = select
    while True:
        query = {**base_params, "limit": page_size, "offset": offset}
        response = _session.get(
            _endpoint(table), params=query, headers=_headers(), timeout=120
        )
        response.raise_for_status()
        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected Supabase response for {table}")
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


def upsert(table: str, rows: list[dict], *, on_conflict: str | None = None) -> None:
    if not rows or not enabled():
        return
    # PostgREST requires every object in a bulk request to have the same keys.
    keys = sorted({key for row in rows for key in row})
    payload = [{key: row.get(key) for key in keys} for row in rows]
    params = {"on_conflict": on_conflict} if on_conflict else None
    response = _session.post(
        _endpoint(table),
        params=params,
        headers=_headers("resolution=merge-duplicates,return=minimal" if on_conflict else "return=minimal"),
        json=payload,
        timeout=120,
    )
    response.raise_for_status()


def insert(table: str, rows: list[dict]) -> None:
    """Insert rows without requiring a unique constraint on the table."""
    if not rows or not enabled():
        return
    keys = sorted({key for row in rows for key in row})
    payload = [{key: row.get(key) for key in keys} for row in rows]
    response = _session.post(
        _endpoint(table),
        headers=_headers("return=minimal"),
        json=payload,
        timeout=120,
    )
    response.raise_for_status()


def delete_by_ids(table: str, ids: list, *, id_column: str = "id") -> None:
    values = [str(value).strip() for value in ids if str(value).strip()]
    if not values or not enabled():
        return
    response = _session.delete(
        _endpoint(table),
        params={id_column: f"in.({','.join(values)})"},
        headers=_headers(),
        timeout=120,
    )
    response.raise_for_status()


def delete_where(table: str, filters: dict[str, object]) -> None:
    if not filters or not enabled():
        return
    params = {key: f"eq.{value}" for key, value in filters.items()}
    response = _session.delete(
        _endpoint(table),
        params=params,
        headers=_headers(),
        timeout=120,
    )
    response.raise_for_status()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
