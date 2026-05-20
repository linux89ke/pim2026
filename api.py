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
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import json
from PIL import Image

logger = logging.getLogger(__name__)

try:
    from sentence_transformers import SentenceTransformer, util
except (ImportError, OSError) as e:
    logger.warning(f"SentenceTransformers failed to load ({e}). Advanced AI features disabled.")
    SentenceTransformer = None
    util = None

# ── Visual Brand Guard ──────────────────────────────────────────────────────
class VisualBrandGuard:
    def __init__(self, reference_dir="restricted_brand_references"):
        self.reference_dir = reference_dir
        self.model = None
        self.index = {} # {brand: [embeddings]}
        self.is_ready = False

    def _ensure_model(self):
        if self.model is None:
            if SentenceTransformer is None:
                logger.error("SentenceTransformer is not available. Visual Brand Guard cannot load.")
                return
            logger.info("Loading CLIP model for Visual Brand Guard...")
            self.model = SentenceTransformer('clip-ViT-B-32')
            logger.info("CLIP model loaded.")

    def build_index(self):
        if not os.path.exists(self.reference_dir):
            logger.warning(f"Reference dir {self.reference_dir} not found. Visual Brand Guard disabled.")
            return

        self._ensure_model()
        new_index = {}
        
        # Load approved sellers list from Restricted_Brands.xlsx
        approved_map = {}
        try:
            if os.path.exists("Restricted_Brands.xlsx"):
                xl = pd.ExcelFile("Restricted_Brands.xlsx")
                for sheet in xl.sheet_names:
                    df = xl.parse(sheet)
                    df.columns = [str(c).strip().lower() for c in df.columns]
                    if 'brand' in df.columns and 'approved sellers' in df.columns:
                        for _, row in df.iterrows():
                            b = str(row.get('brand', '')).strip().lower()
                            s_raw = str(row.get('approved sellers', '')).strip().lower()
                            if b and b != 'nan':
                                sellers = set([s.strip() for s in s_raw.split(',') if s.strip() and s.strip() != 'nan'])
                                approved_map.setdefault(b, set()).update(sellers)
        except Exception as e:
            logger.error(f"Failed to load approved sellers: {e}")

        for brand in os.listdir(self.reference_dir):
            brand_path = os.path.join(self.reference_dir, brand)
            if not os.path.isdir(brand_path): continue
            
            logger.info(f"Indexing restricted brand: {brand}")
            embeddings = []
            for img_name in os.listdir(brand_path):
                if not img_name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.webm')): continue
                try:
                    img_path = os.path.join(brand_path, img_name)
                    img = Image.open(img_path)
                    if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                        bg = Image.new("RGB", img.size, (255, 255, 255))
                        bg.paste(img, mask=img.convert('RGBA').split()[3])
                        img = bg
                    else:
                        img = img.convert("RGB")
                    emb = self.model.encode(img)
                    embeddings.append(emb)
                except Exception as e:
                    logger.error(f"Failed to index {img_name}: {e}")
            
            if embeddings:
                new_index[brand] = {
                    'embeddings': embeddings,
                    'approved_sellers': approved_map.get(brand.lower(), set())
                }
        
        self.index = new_index
        self.is_ready = bool(self.index)
        logger.info(f"Visual Brand Guard index built for {len(self.index)} brands.")

    def check_image(self, img_bytes: bytes, seller_name: str, threshold=0.9) -> Optional[str]:
        if not self.is_ready: return None
        try:
            self._ensure_model()
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
            query_emb = self.model.encode(img)
            
            best_brand = None
            max_sim = 0
            
            seller_lower = str(seller_name).strip().lower()
            
            for brand, data in self.index.items():
                # If seller is already approved for this brand, skip visual check to save time
                if seller_lower in data['approved_sellers']:
                    continue
                    
                ref_embs = data['embeddings']
                similarities = util.cos_sim(query_emb, ref_embs)[0]
                brand_max = similarities.max().item()
                if brand_max > max_sim:
                    max_sim = brand_max
                    best_brand = brand
            
            if max_sim >= threshold:
                return best_brand
        except Exception as e:
            logger.error(f"Visual check failed: {e}")
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
) -> dict[str, Any]:
    from data_utils import standardize_input_data, propagate_metadata, filter_by_country, _repair_mojibake, _detect_and_read_csv
    from loaders import load_support_files_lazy
    from streamlit_app import CountryValidator, validate_products, PREFETCH_MAP, _prefetch_key_from_status_col, _prefetch_reason_from_row

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
    support_files = load_support_files_lazy()

    # 3. Validation
    final_report, results = validate_products(data_unique, support_files, cv, data_has_warranty)

    # 4. ZIP Rejection Mapping
    if not zip_qc_results.empty:
        sid_col = next((c for c in ['PRODUCT_SET_SID', 'ProductSetSid', 'SID'] if c in zip_qc_results.columns), None)
        if sid_col:
            status_cols = [c for c in zip_qc_results.columns if 'status' in c.lower()]
            fmap = support_files.get('flags_mapping', {})
            fr_sid_map = pd.Series(final_report.index, index=final_report['ProductSetSid'].astype(str).str.strip()).to_dict()
            
            for _, r in zip_qc_results.iterrows():
                sid = str(r[sid_col]).strip()
                if sid not in fr_sid_map: continue
                idx = fr_sid_map[sid]
                
                for col in status_cols:
                    if str(r[col]).lower().strip() == 'rejected':
                        pre_key = _prefetch_key_from_status_col(col)
                        flag = PREFETCH_MAP.get(pre_key)
                        if flag:
                            mapped_info = fmap.get(flag, {})
                            reason = _prefetch_reason_from_row(r, col, zip_qc_results.columns)
                            final_report.at[idx, 'Status'] = 'Rejected'
                            final_report.at[idx, 'FLAG'] = flag + " (Prefetched)"
                            final_report.at[idx, 'Reason'] = mapped_info.get('reason', '1000007 - Other Reason')
                            final_report.at[idx, 'Comment'] = reason if (reason and reason.lower() != 'rejected') else mapped_info.get('comment', 'Rejected')

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
    try:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(_executor, _run_full_pipeline, file_bytes, filename, country)
        pipe = r.pipeline()
        pipe.setex(result_key + ":report", RESULT_TTL, res["report"])
        pipe.setex(result_key + ":data", RESULT_TTL, res["data"])
        pipe.setex(result_key + ":summary", RESULT_TTL, pickle.dumps(res["summary"]))
        await pipe.execute()
        await _up("done", 100, "Done", result_key)
    except Exception as e:
        logger.exception("Failed")
        await _up("error", 0, str(e))

@app.post("/validate", response_model=SubmitResponse)
async def submit_validation(background_tasks: BackgroundTasks, file: UploadFile = File(...), country: str = Form("Kenya")):
    file_bytes = await file.read()
    fhash = _file_hash(file_bytes)
    rkey = _result_key(fhash, country)
    r = await get_redis()
    if await r.exists(rkey + ":summary"):
        return SubmitResponse(job_id=f"cached-{fhash[:8]}", cache_hit=True, message="Cached")
    job_id = str(uuid.uuid4())
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
    return pickle.loads(raw).to_dict(orient="records")

@app.get("/result/data/{country}/{file_hash}")
async def get_data(country: str, file_hash: str):
    r = await get_redis()
    raw = await r.get(_result_key(file_hash, country) + ":data")
    if not raw: raise HTTPException(404)
    return pickle.loads(raw).to_dict(orient="records")

@app.delete("/result/{country}/{file_hash}")
async def invalidate_cache(country: str, file_hash: str):
    r = await get_redis()
    rkey = _result_key(file_hash, country)
    await r.delete(rkey + ":report", rkey + ":data", rkey + ":summary")
    return {"deleted": True}

@app.get("/health")
async def health():
    r = await get_redis()
    await r.ping()
    return {"status": "ok", "timestamp": time.time()}
