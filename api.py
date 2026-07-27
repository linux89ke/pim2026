"""
api.py  —  FastAPI validation service
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import pickle
import time
import uuid
import zipfile
import re
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any, List, Dict, Optional

import pandas as pd
import redis.asyncio as aioredis
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import json
from PIL import Image

logger = logging.getLogger(__name__)

# SentenceTransformers / CLIP removed — Visual Brand Guard disabled

# ── Visual Brand Guard (disabled — CLIP/SentenceTransformers removed) ─────
class VisualBrandGuard:
    """No-op stub. CLIP model dependency has been removed."""
    def __init__(self, *args, **kwargs):
        self.is_ready = False

    def build_index(self): pass

    def check_image(self, *args, **kwargs):
        return None

brand_guard = VisualBrandGuard()

app = FastAPI(title="Product Validation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Redis ───────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis: aioredis.Redis | None = None

async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url(REDIS_URL, decode_responses=False)
    return _redis

_executor = ThreadPoolExecutor(max_workers=int(os.getenv("VALIDATOR_WORKERS", "4")))
RESULT_TTL = 7200
JOB_TTL    = 600

@app.on_event("startup")
async def startup_event():
    # Build the brand index on background thread to not block FastAPI startup
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, brand_guard.build_index)

# ── Models ───────────────────────────────────────────────────────────────────
class SubmitResponse(BaseModel):
    job_id: str
    cache_hit: bool
    message: str

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: int
    message: str
    result_key: str | None = None

class ValidationSummary(BaseModel):
    total: int
    approved: int
    rejected: int
    rejection_rate: float
    flags: dict[str, int]

# ── Helpers ──────────────────────────────────────────────────────────────────
def _file_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:24]

def _result_key(file_hash: str, country: str) -> str:
    return f"result:{country}:{file_hash}"

def _job_key(job_id: str) -> str:
    return f"job:{job_id}"

# ── FULL PIPELINE ────────────────────────────────────────────────────────────
def _run_full_pipeline(
    file_bytes: bytes,
    filename: str,
    country: str,
    progress_state: dict | None = None,
) -> dict[str, Any]:
    from data_utils import standardize_input_data, propagate_metadata, filter_by_country, _repair_mojibake, _detect_and_read_csv
    from loaders import load_support_files_lazy
    from streamlit_app import CountryValidator, validate_products, PREFETCH_MAP, _prefetch_key_from_status_col, _prefetch_reason_from_row

    # progress_state is a plain dict shared with the async caller (running in
    # a different thread via ThreadPoolExecutor) so the /status endpoint can
    # report which stage is running instead of a static "Processing…" message
    # for the whole job. Dict writes of primitives are safe without a lock
    # under the GIL — the caller only ever reads, never mutates.
    def _stage(msg: str, pct: int):
        if progress_state is not None:
            progress_state["message"] = msg
            progress_state["pct"] = pct

    _stage("Reading file…", 12)
    buf = BytesIO(file_bytes)
    zip_qc_results = pd.DataFrame()

    # 1. Multi-format Reader
    if filename.lower().endswith('.zip'):
        with zipfile.ZipFile(buf) as zf:
            members = zf.infolist()
            qc_file = next((i for i in members if 'qc_results' in i.filename.lower() and i.filename.lower().endswith(('.xlsx', '.csv'))), None)
            if qc_file:
                content = zf.read(qc_file)
                zip_qc_results = pd.read_csv(BytesIO(content), dtype=str) if qc_file.filename.endswith('.csv') else pd.read_excel(BytesIO(content), dtype=str)
                raw_data = zip_qc_results.copy()
            else:
                raw_data = pd.DataFrame()
    elif 'qc_results' in filename.lower():
        zip_qc_results = pd.read_excel(buf, engine='openpyxl', dtype=str) if filename.endswith('.xlsx') else _detect_and_read_csv(buf)
        raw_data = zip_qc_results.copy()
    elif filename.lower().endswith('.xlsx'):
        raw_data = pd.read_excel(buf, engine='openpyxl', dtype=str)
    else:
        raw_data = _detect_and_read_csv(buf)

    if raw_data.empty:
        raise ValueError("File is empty.")

    # 2. Preparation
    _stage("Repairing text encoding…", 18)
    raw_data = _repair_mojibake(raw_data)
    data_std = standardize_input_data(raw_data)
    data_prop = propagate_metadata(data_std)
    cv = CountryValidator(country)
    data_filtered, _ = filter_by_country(data_prop, cv)

    if data_filtered.empty:
        raise ValueError(f"No {country} items.")

    # Variation counts
    actual_counts = data_filtered.groupby('PRODUCT_SET_SID')['PRODUCT_SET_SID'].transform('count')
    if 'COUNT_VARIATIONS' in data_filtered.columns:
        file_counts = pd.to_numeric(data_filtered['COUNT_VARIATIONS'], errors='coerce').fillna(1)
        data_filtered['COUNT_VARIATIONS'] = actual_counts.combine(file_counts, max)
    else:
        data_filtered['COUNT_VARIATIONS'] = actual_counts

    data_unique = data_filtered.drop_duplicates(subset=['PRODUCT_SET_SID'], keep='first')
    data_has_warranty = all(c in data_unique.columns for c in ['PRODUCT_WARRANTY', 'WARRANTY_DURATION'])
    _stage("Loading rule files…", 25)
    support_files = load_support_files_lazy()

    # 3. Validation — on_progress reports which specific check is running
    # (e.g. "Checking: Wrong Category") so /status shows real stage info
    # instead of a static message for the whole job, matching what the
    # in-process Streamlit path already shows via its own progress bar.
    def _on_check_progress(flag_name: str, i: int, total: int):
        pct = 25 + int(i / max(total, 1) * 55)  # validation spans ~25%-80%
        _stage(f"Checking: {flag_name}", pct)

    final_report, results = validate_products(
        data_unique, support_files, cv, data_has_warranty, on_progress=_on_check_progress,
    )

    # 4. ZIP Rejection Mapping — melt to find the (usually small) set of
    #    'rejected' entries instead of scanning every row × status column in
    #    Python (same vectorized approach as streamlit_app's ZIP path).
    _stage("Applying prefetched ZIP results…", 85)
    if not zip_qc_results.empty:
        sid_col = next((c for c in ['PRODUCT_SET_SID', 'ProductSetSid', 'SID'] if c in zip_qc_results.columns), None)
        if sid_col:
            status_cols = [c for c in zip_qc_results.columns if 'status' in c.lower()]
            fmap = support_files.get('flags_mapping', {})
            fr_sid_map = pd.Series(final_report.index, index=final_report['ProductSetSid'].astype(str).str.strip()).to_dict()

            if status_cols:
                melted = zip_qc_results[[sid_col] + status_cols].melt(id_vars=sid_col, var_name='col', value_name='val')
                rejected_entries = melted[melted['val'].astype(str).str.lower().str.strip() == 'rejected']
                if not rejected_entries.empty:
                    qc_indexed = zip_qc_results.set_index(sid_col)
                    updates: dict = {}  # fr index -> (flag, reason_code, comment)
                    for sid_raw, col in zip(rejected_entries[sid_col], rejected_entries['col']):
                        sid = str(sid_raw).strip()
                        if sid not in fr_sid_map: continue
                        pre_key = _prefetch_key_from_status_col(col)
                        flag = PREFETCH_MAP.get(pre_key)
                        if not flag: continue
                        r = qc_indexed.loc[sid_raw]
                        if isinstance(r, pd.DataFrame): r = r.iloc[0]
                        mapped_info = fmap.get(flag, {})
                        reason = _prefetch_reason_from_row(r, col, zip_qc_results.columns)
                        comment = reason if (reason and reason.lower() != 'rejected') else mapped_info.get('comment', 'Rejected')
                        updates[fr_sid_map[sid]] = (flag + " (Prefetched)", mapped_info.get('reason', '1000007 - Other Reason'), comment)

                    if updates:
                        idxs = list(updates.keys())
                        final_report.loc[idxs, 'Status'] = 'Rejected'
                        final_report.loc[idxs, 'FLAG'] = [updates[i][0] for i in idxs]
                        final_report.loc[idxs, 'Reason'] = [updates[i][1] for i in idxs]
                        final_report.loc[idxs, 'Comment'] = [updates[i][2] for i in idxs]

    # 5. Summary
    rej = final_report[final_report["Status"] == "Rejected"]
    summary = {
        "total": len(final_report),
        "approved": int((final_report["Status"] == "Approved").sum()),
        "rejected": len(rej),
        "rejection_rate": round(len(rej) / max(len(final_report), 1) * 100, 1),
        "flags": rej["FLAG"].value_counts().to_dict(),
    }

    # 6. Visual Brand Guard (Optional - Kenya Only for now)
    if brand_guard.is_ready and country.lower() == 'kenya':
        from data_utils import _get_image_from_zip
        import requests
        logger.info("Running Visual Brand Guard check...")
        
        # Determine if we are processing a ZIP
        zf = None
        if filename.lower().endswith('.zip'):
            zf = zipfile.ZipFile(BytesIO(file_bytes))
            
        for idx, row in final_report.iterrows():
            if row['Status'] == 'Rejected': continue
            sid = str(row['ProductSetSid']).strip()
            product_rows = data_unique[data_unique['PRODUCT_SET_SID'] == sid]
            if product_rows.empty: continue
            p_row = product_rows.iloc[0]
            
            img_url = str(p_row.get('MAIN_IMAGE_URL', '')).strip()
            if not img_url: continue
            
            seller_name = str(row.get('SELLER_NAME', 'Unknown')).strip()
            
            img_bytes = None
            try:
                if zf and img_url.startswith('images/'):
                    img_bytes = zf.read(img_url)
                elif img_url.startswith(('http://', 'https://')):
                    resp = requests.get(img_url, timeout=5)
                    if resp.status_code == 200:
                        img_bytes = resp.content
            except:
                continue

            if img_bytes:
                detected_brand = brand_guard.check_image(img_bytes, seller_name)
                if detected_brand:
                    seller_brand = str(row.get('BRAND', 'Generic')).strip()
                    if seller_brand.lower() != detected_brand.lower():
                        final_report.at[idx, 'Status'] = 'Rejected'
                        final_report.at[idx, 'FLAG'] = f"Restricted Brand ({detected_brand})"
                        final_report.at[idx, 'Reason'] = "1000002 - Restricted Brand"
                        final_report.at[idx, 'Comment'] = f"Visual match for restricted brand: {detected_brand.upper()}. Seller declared: {seller_brand}."
        
        if zf: zf.close()

    _stage("Finalizing report…", 98)
    return {
        "report": pickle.dumps(final_report),
        "data": pickle.dumps(data_unique),
        "summary": summary,
    }

async def _validation_task(job_id, file_bytes, filename, country, result_key):
    r = await get_redis()
    async def _up(s, p, m, rk=None):
        await r.setex(_job_key(job_id), JOB_TTL, pickle.dumps({"job_id":job_id,"status":s,"progress":p,"message":m,"result_key":rk or ""}))

    await _up("running", 10, "Processing pipeline…")

    # Shared with _run_full_pipeline running on a worker thread. Plain dict
    # writes/reads of primitives are safe without a lock under the GIL — the
    # pipeline is the sole writer, the heartbeat below is the sole reader.
    progress_state = {"message": "Processing pipeline…", "pct": 10}

    async def _heartbeat():
        # Refreshes the job status key frequently — both to keep it alive
        # (JOB_TTL) during long-running pipelines, and so /status reflects
        # which specific check is currently running (see _stage() calls in
        # _run_full_pipeline) instead of a static message for the whole job.
        try:
            while True:
                await asyncio.sleep(2)
                await _up("running", progress_state["pct"], progress_state["message"])
        except asyncio.CancelledError:
            pass

    hb_task = asyncio.create_task(_heartbeat())
    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(_executor, _run_full_pipeline, file_bytes, filename, country, progress_state)
        pipe = r.pipeline()
        pipe.setex(result_key + ":report", RESULT_TTL, res["report"])
        pipe.setex(result_key + ":data", RESULT_TTL, res["data"])
        pipe.setex(result_key + ":summary", RESULT_TTL, pickle.dumps(res["summary"]))
        await pipe.execute()
        await _up("done", 100, "Done", result_key)
    except Exception as e:
        logger.exception("Failed")
        await _up("error", 0, str(e))
    finally:
        hb_task.cancel()
        try:
            await hb_task
        except asyncio.CancelledError:
            pass
        # Release the in-flight claim so a later resubmission (e.g. after cache
        # invalidation) isn't permanently blocked by a stale marker.
        await r.delete(result_key + ":inflight")

@app.post("/validate", response_model=SubmitResponse)
async def submit_validation(background_tasks: BackgroundTasks, file: UploadFile = File(...), country: str = Form("Kenya")):
    file_bytes = await file.read()
    fhash = _file_hash(file_bytes)
    rkey = _result_key(fhash, country)
    r = await get_redis()
    if await r.exists(rkey + ":summary"):
        return SubmitResponse(job_id=f"cached-{fhash[:8]}", cache_hit=True, message="Cached")

    job_id = str(uuid.uuid4())
    # Atomically claim the in-flight slot for this file+country. If another
    # request already claimed it (e.g. two people uploading the same export
    # at once), reuse that job_id instead of starting a duplicate pipeline run.
    claimed = await r.set(rkey + ":inflight", job_id, nx=True, ex=JOB_TTL)
    if not claimed:
        existing_job_id = await r.get(rkey + ":inflight")
        if existing_job_id:
            return SubmitResponse(job_id=existing_job_id.decode(), cache_hit=False, message="Already in progress")

    background_tasks.add_task(_validation_task, job_id, file_bytes, file.filename or "up.csv", country, rkey)
    return SubmitResponse(job_id=job_id, cache_hit=False, message="Queued")

@app.get("/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    if job_id.startswith("cached-"): return JobStatus(job_id=job_id, status="done", progress=100, message="Cached")
    r = await get_redis()
    raw = await r.get(_job_key(job_id))
    if not raw: raise HTTPException(404)
    return JobStatus(**pickle.loads(raw))

@app.get("/result/summary/{country}/{file_hash}", response_model=ValidationSummary)
async def get_summary(country: str, file_hash: str):
    r = await get_redis()
    raw = await r.get(_result_key(file_hash, country) + ":summary")
    if not raw: raise HTTPException(404)
    return ValidationSummary(**pickle.loads(raw))

@app.get("/result/report/{country}/{file_hash}")
async def get_report(country: str, file_hash: str):
    r = await get_redis()
    raw = await r.get(_result_key(file_hash, country) + ":report")
    if not raw: raise HTTPException(404)
    return Response(content=raw, media_type="application/octet-stream")

@app.get("/result/data/{country}/{file_hash}")
async def get_data(country: str, file_hash: str):
    r = await get_redis()
    raw = await r.get(_result_key(file_hash, country) + ":data")
    if not raw: raise HTTPException(404)
    return Response(content=raw, media_type="application/octet-stream")

@app.delete("/result/{country}/{file_hash}")
async def invalidate_cache(country: str, file_hash: str):
    r = await get_redis()
    rkey = _result_key(file_hash, country)
    await r.delete(rkey + ":report", rkey + ":data", rkey + ":summary", rkey + ":inflight")
    return {"deleted": True}

@app.get("/health")
async def health():
    r = await get_redis()
    await r.ping()
    return {"status": "ok", "timestamp": time.time()}