"""
api_client.py  —  Streamlit-side HTTP client for the FastAPI service.
"""

from __future__ import annotations

import hashlib
import os
import time
from io import BytesIO
from typing import Any

import pandas as pd
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
_SESSION = requests.Session()  # reuse TCP connections

POLL_INTERVAL = 1.0  # seconds between status checks
POLL_TIMEOUT = 300  # give up after 5 minutes


def _file_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:24]


# ── Public API ────────────────────────────────────────────────────────────────


def submit_file(
    file_bytes: bytes,
    filename: str,
    country: str,
    skip_validators: list | None = None,   # ← NEW
) -> dict[str, Any]:
    resp = _SESSION.post(
        f"{API_BASE}/validate",
        files={"file": (filename, BytesIO(file_bytes), "application/octet-stream")},
        data={
            "country": country,
            "skip_validators": ",".join(skip_validators) if skip_validators else "",  # ← NEW
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def poll_until_done(job_id: str) -> dict[str, Any]:
    """
    Block (with a Streamlit progress bar) until the job finishes.
    """
    placeholder = st.empty()
    progress_bar = st.progress(0, text="Validating…")
    deadline = time.time() + POLL_TIMEOUT

    while time.time() < deadline:
        resp = _SESSION.get(f"{API_BASE}/status/{job_id}", timeout=10)
        resp.raise_for_status()
        status = resp.json()

        pct = int(status.get("progress", 0))
        msg = status.get("message", "Working…")
        progress_bar.progress(pct, text=msg)
        placeholder.caption(f"Job `{job_id[:8]}…` — {msg}")

        if status["status"] == "done":
            progress_bar.empty()
            placeholder.empty()
            return status

        if status["status"] == "error":
            progress_bar.empty()
            placeholder.empty()
            raise RuntimeError(f"Validation failed: {msg}")

        time.sleep(POLL_INTERVAL)

    raise RuntimeError(f"Validation timed out after {POLL_TIMEOUT}s")


def fetch_summary(country: str, file_hash: str) -> dict[str, Any]:
    resp = _SESSION.get(f"{API_BASE}/result/summary/{country}/{file_hash}", timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_report(country: str, file_hash: str) -> pd.DataFrame:
    """Returns the final_report DataFrame."""
    resp = _SESSION.get(f"{API_BASE}/result/report/{country}/{file_hash}", timeout=60)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def fetch_data(country: str, file_hash: str) -> pd.DataFrame:
    """Returns the all_data_map DataFrame."""
    resp = _SESSION.get(f"{API_BASE}/result/data/{country}/{file_hash}", timeout=60)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def invalidate(country: str, file_hash: str) -> None:
    """
    Ask the API server to drop its cached result for this file.
    When the server is not running this is a no-op — the local
    parquet cache is invalidated separately by the caller.
    """
    if not _probe_server():
        return  # nothing to invalidate server-side; local cache handled by caller
    try:
        resp = _SESSION.delete(f"{API_BASE}/result/{country}/{file_hash}", timeout=10)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        global _SERVER_AVAILABLE
        _SERVER_AVAILABLE = False  # server disappeared; future calls go direct


# ── Streamlit integration helper ────────────────────────────────────────────

# Registry populated by streamlit_app.py after its functions are defined.
# Avoids any sys.modules / re-import guessing.
_DIRECT_FUNCS: dict = {}


def register_direct_pipeline(
    country_validator_cls,
    validate_products_fn,
    prefetch_map: dict,
    prefetch_key_fn,
    prefetch_reason_fn,
) -> None:
    """
    Called once by streamlit_app.py after all its functions are defined.
    Stores the callables that _run_direct needs so it never has to
    re-import streamlit_app (which would cause duplicate widget-ID errors).
    """
    _DIRECT_FUNCS.update(
        {
            "CountryValidator": country_validator_cls,
            "validate_products": validate_products_fn,
            "PREFETCH_MAP": prefetch_map,
            "_prefetch_key_from_status_col": prefetch_key_fn,
            "_prefetch_reason_from_row": prefetch_reason_fn,
        }
    )


_SERVER_AVAILABLE: bool | None = None
_PROBE_TIMESTAMP: float = 0.0
_PROBE_TTL = 60  # re-probe every 60 seconds


def _probe_server() -> bool:
    """Return True if the API server is reachable; cache the result with a TTL."""
    global _SERVER_AVAILABLE, _PROBE_TIMESTAMP
    if _SERVER_AVAILABLE is not None and (time.time() - _PROBE_TIMESTAMP) < _PROBE_TTL:
        return _SERVER_AVAILABLE
    try:
        r = _SESSION.get(f"{API_BASE}/health", timeout=2)
        _SERVER_AVAILABLE = r.status_code == 200
    except Exception:
        _SERVER_AVAILABLE = False
    _PROBE_TIMESTAMP = time.time()
    return _SERVER_AVAILABLE


def _run_direct(
    file_bytes: bytes,
    filename: str,
    country: str,
    skip_validators: list | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    In-process validation pipeline — runs without the FastAPI server.

    WHY we do NOT use `from api import _run_full_pipeline`
    -------------------------------------------------------
    Streamlit executes streamlit_app.py as the ``__main__`` module, NOT as
    ``streamlit_app``.  If api.py does ``from streamlit_app import …`` while
    the app is already running, Python cannot find ``streamlit_app`` in
    sys.modules (it only knows ``__main__``), so it re-imports the file from
    disk — re-executing every top-level widget call and producing the
    ``multiple selectbox elements with the same auto-generated ID`` error.

    Instead we fetch CountryValidator, validate_products, etc. directly from
    the already-loaded ``__main__`` module (zero re-import, zero widget
    duplication) and replicate the pipeline inline.
    """
    import zipfile

    # ── 1. grab symbols from the registry populated by streamlit_app.py ──────
    if not _DIRECT_FUNCS:
        raise RuntimeError(
            "Direct validation not ready: register_direct_pipeline() was not "
            "called. This is done automatically at app startup — if you see "
            "this error, streamlit_app.py may not have finished loading."
        )

    CountryValidator = _DIRECT_FUNCS["CountryValidator"]
    validate_products = _DIRECT_FUNCS["validate_products"]
    PREFETCH_MAP = _DIRECT_FUNCS["PREFETCH_MAP"]
    _prefetch_key = _DIRECT_FUNCS["_prefetch_key_from_status_col"]
    _prefetch_reason = _DIRECT_FUNCS["_prefetch_reason_from_row"]

    # ── 2. imports that carry no Streamlit widget code ─────────────────────
    from data_utils import (
        _detect_and_read_csv,
        _repair_mojibake,
        filter_by_country,
        propagate_metadata,
        standardize_input_data,
    )
    from loaders import load_support_files_lazy

    buf = BytesIO(file_bytes)
    zip_qc_results = pd.DataFrame()

    # ── 3. read the uploaded file (same logic as api._run_full_pipeline) ───
    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(buf) as zf:
            members = zf.infolist()
            qc_file = next(
                (
                    i
                    for i in members
                    if "qc_results" in i.filename.lower()
                    and i.filename.lower().endswith((".xlsx", ".csv"))
                ),
                None,
            )
            if qc_file:
                content = zf.read(qc_file)
                zip_qc_results = (
                    pd.read_csv(BytesIO(content), dtype=str)
                    if qc_file.filename.endswith(".csv")
                    else pd.read_excel(BytesIO(content), dtype=str)
                )
                raw_data = zip_qc_results.copy()
            else:
                raw_data = pd.DataFrame()
    elif "qc_results" in filename.lower():
        zip_qc_results = (
            pd.read_excel(buf, engine="openpyxl", dtype=str)
            if filename.endswith(".xlsx")
            else _detect_and_read_csv(buf)
        )
        raw_data = zip_qc_results.copy()
    elif filename.lower().endswith(".xlsx"):
        raw_data = pd.read_excel(buf, engine="openpyxl", dtype=str)
    else:
        raw_data = _detect_and_read_csv(buf)

    if raw_data.empty:
        raise ValueError("File is empty or could not be read.")

    # ── 4. standardise & filter ─────────────────────────────────────────
    raw_data = _repair_mojibake(raw_data)
    data_std = standardize_input_data(raw_data)
    data_prop = propagate_metadata(data_std)
    cv = CountryValidator(country)
    data_filtered, _ = filter_by_country(data_prop, cv)

    if data_filtered.empty:
        raise ValueError(f"No {country} items found after filtering.")

    # variation counts
    actual_counts = data_filtered.groupby("PRODUCT_SET_SID")[
        "PRODUCT_SET_SID"
    ].transform("count")
    if "COUNT_VARIATIONS" in data_filtered.columns:
        file_counts = pd.to_numeric(
            data_filtered["COUNT_VARIATIONS"], errors="coerce"
        ).fillna(1)
        data_filtered["COUNT_VARIATIONS"] = actual_counts.combine(file_counts, max)
    else:
        data_filtered["COUNT_VARIATIONS"] = actual_counts

    data_unique = data_filtered.drop_duplicates(
        subset=["PRODUCT_SET_SID"], keep="first"
    )
    data_has_warranty = all(
        c in data_unique.columns for c in ["PRODUCT_WARRANTY", "WARRANTY_DURATION"]
    )
    support_files = load_support_files_lazy()

    # ── 5. validate ────────────────────────────────────────────────────
    final_report, _ = validate_products(
        data_unique,
        support_files,
        cv,
        data_has_warranty,
        skip_validators=skip_validators or [],
    )

    # ── 6. ZIP rejection mapping (prefetch statuses from a QC-results file) ─
    # NOTE: Prefetch mapping (zip_qc_results -> final_report) is applied by
    # streamlit_app.py's processing block, not here. This ensures it runs
    # correctly for both the server path and the direct path, and handles
    # separately-uploaded QC-results files (not just embedded ZIPs).

    return final_report, data_unique


def validate_and_load(
    file_bytes: bytes,
    filename: str,
    country: str,
    skip_validators: list | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run validation and return (final_report, all_data) DataFrames.

    Strategy
    --------
    1. Probe the API server (one quick GET /health, result cached for the
       lifetime of the process so it doesn’t slow down every upload).
    2. If the server is UP  → use the async HTTP pipeline (progress bar, caching).
    3. If the server is DOWN → fall back transparently to a direct in-process
       call via api._run_full_pipeline — no error shown to the user,
       no server required.
    """
    if not _probe_server():
        # ─ no server — run everything in-process ──────────────────────
        with st.spinner("Validating products…"):
            return _run_direct(
                file_bytes, filename, country, skip_validators=skip_validators
            )

    # ─ server is up — use async HTTP pipeline ─────────────────────────
    try:
        fhash = _file_hash(file_bytes)
        submit_resp = submit_file(
    file_bytes,
    filename,
    country,
    skip_validators=skip_validators,   # ← NEW
)

        if not submit_resp["cache_hit"]:
            poll_until_done(submit_resp["job_id"])

        with st.spinner("Loading results…"):
            report = fetch_report(country, fhash)
            data = fetch_data(country, fhash)

        return report, data

    except requests.exceptions.ConnectionError:
        # Server went away mid-session — reset probe flag and fall back
        global _SERVER_AVAILABLE
        _SERVER_AVAILABLE = False
        with st.spinner("Validating products (direct mode)…"):
            return _run_direct(
                file_bytes, filename, country, skip_validators=skip_validators
            )


def get_summary_metrics(file_bytes: bytes, country: str) -> dict[str, Any] | None:
    """
    Fetch just the summary dict.
    """
    fhash = _file_hash(file_bytes)
    try:
        return fetch_summary(country, fhash)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        raise
