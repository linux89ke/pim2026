"""Optional Cloudflare R2 persistence for reports, thumbnails, and exports."""

from __future__ import annotations

import logging
import hashlib
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
        client = _client()
        source = Path(path)
        if client is None or not source.is_file():
            return False
        client.upload_file(
            str(source), _secret("R2_BUCKET"), str(key),
            ExtraArgs={"ContentType": content_type},
        )
        return True
    except Exception as exc:
        logger.warning("Could not read R2 upload file %s: %s", path, exc)
        return False


def upload_file_verified(path: str | Path, key: str, content_type: str = "application/octet-stream") -> bool:
    """Stream a file to R2 and verify its size and SHA-256 metadata."""
    source = Path(path)
    client = _client()
    if client is None or not source.is_file():
        return False
    try:
        digest = hashlib.sha256()
        with source.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        sha256 = digest.hexdigest()
        try:
            remote = client.head_object(Bucket=_secret("R2_BUCKET"), Key=str(key))
            if (
                int(remote.get("ContentLength", -1)) == source.stat().st_size
                and str(remote.get("Metadata", {}).get("sha256", "")).lower() == sha256
            ):
                return True
        except Exception:
            pass
        client.upload_file(
            str(source), _secret("R2_BUCKET"), str(key),
            ExtraArgs={"ContentType": content_type, "Metadata": {"sha256": sha256}},
        )
        remote = client.head_object(Bucket=_secret("R2_BUCKET"), Key=str(key))
        return (
            int(remote.get("ContentLength", -1)) == source.stat().st_size
            and str(remote.get("Metadata", {}).get("sha256", "")).lower() == sha256
        )
    except Exception as exc:
        logger.warning("Verified R2 upload failed for %s: %s", key, exc)
        return False


def download_file_verified(
    key: str, path: str | Path, *, expected_sha256: str = "", expected_size: int | None = None,
) -> bool:
    """Stream an R2 object to disk, then verify it before exposing the file."""
    client = _client()
    target = Path(path)
    if client is None:
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(target.name + ".download")
    try:
        client.download_file(_secret("R2_BUCKET"), str(key), str(temp))
        if expected_size is not None and temp.stat().st_size != int(expected_size):
            temp.unlink(missing_ok=True)
            return False
        if expected_sha256:
            digest = hashlib.sha256()
            with temp.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest().lower() != str(expected_sha256).lower():
                temp.unlink(missing_ok=True)
                return False
        os.replace(temp, target)
        return True
    except Exception as exc:
        logger.warning("R2 download failed for %s: %s", key, exc)
        temp.unlink(missing_ok=True)
        return False


def download_bytes(key: str) -> bytes:
    """Read an optional object from R2; return empty bytes when unavailable."""
    client = _client()
    if client is None:
        return b""
    try:
        response = client.get_object(Bucket=_secret("R2_BUCKET"), Key=str(key))
        return response["Body"].read()
    except Exception:
        return b""


def upload_bytes_async(data: bytes, key: str, content_type: str = "application/octet-stream") -> None:
    if enabled() and data:
        _POOL.submit(upload_bytes, data, key, content_type)


def upload_file_async(path: str | Path, key: str, content_type: str = "application/octet-stream") -> None:
    if enabled():
        _POOL.submit(upload_file, path, key, content_type)
