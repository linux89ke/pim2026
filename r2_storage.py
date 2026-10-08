"""Optional Cloudflare R2 persistence for reports, thumbnails, and exports."""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logger = logging.getLogger(__name__)
_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="r2-upload")
_CLIENT = None


def _secret(name: str) -> str:
    value = os.getenv(name, "")
    if value:
        return str(value).strip()
    try:
        import streamlit as st
        return str(st.secrets.get(name, "") or "").strip()
    except Exception:
        return ""


def enabled() -> bool:
    return all(_secret(name) for name in (
        "R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
    ))


def _client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    if not enabled():
        return None
    try:
        import boto3
        _CLIENT = boto3.client(
            "s3",
            endpoint_url=f"https://{_secret('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
            aws_access_key_id=_secret("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_secret("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        )
        return _CLIENT
    except Exception as exc:
        logger.warning("R2 is configured but unavailable: %s", exc)
        return None


def upload_bytes(data: bytes, key: str, content_type: str = "application/octet-stream") -> bool:
    client = _client()
    if client is None or not data:
        return False
    try:
        client.put_object(Bucket=_secret("R2_BUCKET"), Key=str(key), Body=data, ContentType=content_type)
        return True
    except Exception as exc:
        logger.warning("R2 upload failed for %s: %s", key, exc)
        return False


def upload_file(path: str | Path, key: str, content_type: str = "application/octet-stream") -> bool:
    try:
        return upload_bytes(Path(path).read_bytes(), key, content_type)
    except Exception as exc:
        logger.warning("Could not read R2 upload file %s: %s", path, exc)
        return False


def upload_bytes_async(data: bytes, key: str, content_type: str = "application/octet-stream") -> None:
    if enabled() and data:
        _POOL.submit(upload_bytes, data, key, content_type)


def upload_file_async(path: str | Path, key: str, content_type: str = "application/octet-stream") -> None:
    if enabled():
        _POOL.submit(upload_file, path, key, content_type)
