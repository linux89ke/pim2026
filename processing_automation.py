"""Large-file validation helpers: manifests, thumbnails, similarity and review queues."""
from __future__ import annotations
import base64, hashlib, io, json, os, re, threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
import r2_storage

ROOT = Path(__file__).resolve().parent
AUTOMATION_DIR = ROOT / ".validation_automation"
MANIFEST_DIR = AUTOMATION_DIR / "manifests"
THUMB_DIR = AUTOMATION_DIR / "thumbnails"
MANIFEST_DIR.mkdir(exist_ok=True, parents=True)
THUMB_DIR.mkdir(exist_ok=True, parents=True)
DOWNLOAD_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="image-download")
_LOCK = threading.RLock()

def chunk_artifact_dir(signature: str) -> Path:
    path = AUTOMATION_DIR / "chunks" / str(signature)
    path.mkdir(exist_ok=True, parents=True)
    return path

def _safe_artifact_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value))[:100] or "result"

def save_chunk_results(manifest: dict, batch_id: str, report, results: dict) -> dict:
    """Persist one validation chunk so a restart can reuse it safely."""
    import pandas as pd
    root = chunk_artifact_dir(str(manifest.get("signature", "unknown")))
    report_path = root / f"{_safe_artifact_name(batch_id)}_report.parquet"
    report.to_parquet(report_path, index=False)
    r2_storage.upload_file_async(report_path, f"validation/chunks/{report_path.name}", "application/octet-stream")
    result_paths = {}
    for flag, frame in (results or {}).items():
        if not isinstance(frame, pd.DataFrame):
            continue
        path = root / f"{_safe_artifact_name(batch_id)}_{_safe_artifact_name(flag)}.parquet"
        frame.to_parquet(path, index=False)
        r2_storage.upload_file_async(path, f"validation/chunks/{path.name}", "application/octet-stream")
        result_paths[str(flag)] = str(path)
    item = manifest.setdefault("batches", {}).setdefault(str(batch_id), {})
    item["report_path"] = str(report_path)
    item["result_paths"] = result_paths
    return item

def load_chunk_results(manifest: dict, batch_id: str):
    """Return saved report/results, or None when the chunk is incomplete."""
    import pandas as pd
    item = manifest.get("batches", {}).get(str(batch_id), {})
    report_path = item.get("report_path")
    result_paths = item.get("result_paths", {})
    if not report_path or not Path(report_path).exists():
        return None
    try:
        report = pd.read_parquet(report_path)
        results = {
            flag: pd.read_parquet(path)
            for flag, path in result_paths.items()
            if Path(path).exists()
        }
        return report, results
    except Exception:
        return None

def save_stage_frame(manifest: dict, stage: str, frame) -> str:
    """Persist a reusable pipeline stage artifact and record its path."""
    root = chunk_artifact_dir(str(manifest.get("signature", "unknown")))
    path = root / f"stage_{_safe_artifact_name(stage)}.parquet"
    frame.to_parquet(path, index=False)
    manifest.setdefault("stage_paths", {})[str(stage)] = str(path)
    return str(path)

def load_stage_frame(manifest: dict, stage: str):
    import pandas as pd
    path = manifest.get("stage_paths", {}).get(str(stage))
    if not path or not Path(path).exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None

def file_signature(name: str, payload: bytes) -> str:
    return hashlib.sha256((str(name) + "\0").encode() + payload).hexdigest()[:24]

def manifest_path(signature: str) -> Path:
    return MANIFEST_DIR / f"{signature}.json"

def load_manifest(signature: str) -> dict:
    path = manifest_path(signature)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"signature": signature, "created_at": datetime.now(timezone.utc).isoformat(), "batches": {}}

def save_manifest(manifest: dict) -> None:
    path = manifest_path(str(manifest.get("signature", "unknown")))
    tmp = path.with_suffix(".tmp")
    with _LOCK:
        tmp.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    r2_storage.upload_file_async(path, f"validation/manifests/{path.name}", "application/json")

def mark_batch(manifest: dict, batch_id: str, **fields) -> dict:
    batches = manifest.setdefault("batches", {})
    item = batches.setdefault(str(batch_id), {})
    item.update(fields)
    item["updated_at"] = datetime.now(timezone.utc).isoformat()
    save_manifest(manifest)
    return item

def completed_batches(manifest: dict) -> set[str]:
    return {str(k) for k,v in manifest.get("batches", {}).items() if v.get("status") == "complete"}

def thumbnail_key(url: str, size: int = 320) -> Path:
    digest = hashlib.sha256(f"{size}:{url}".encode()).hexdigest()
    return THUMB_DIR / f"{digest}.jpg"

def thumbnail_data_uri(raw: bytes, url: str, size: int = 240) -> str:
    """Create/read a compact cached JPEG thumbnail for iframe delivery."""
    path = thumbnail_key(url, size)
    try:
        if not path.exists():
            from PIL import Image
            image = Image.open(io.BytesIO(raw)).convert("RGB")
            image.thumbnail((size, size), Image.Resampling.LANCZOS)
            image.save(path, format="JPEG", quality=72, optimize=True)
            r2_storage.upload_file_async(path, f"validation/thumbnails/{path.name}", "image/jpeg")
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return "data:image/jpeg;base64," + encoded
    except Exception:
        return ""

def similarity_label(match_method: str, distance=None) -> str:
    if match_method == "url": return "Exact URL match"
    if match_method == "phash": return "Exact pHash match"
    if match_method == "near-phash": return f"Near-pHash match: distance {distance}/64" if distance is not None else "Near-pHash match"
    return "Unmatched"

def cluster_image_keys(rows: Iterable[dict]) -> dict[str, list[dict]]:
    clusters = {}
    for row in rows:
        key = str(row.get("phash") or row.get("image_url") or "").strip()
        if key: clusters.setdefault(key, []).append(row)
    return clusters

def false_positive_suggestions(rules: Iterable[dict], approvals: dict[str, int], threshold: int = 3) -> list[dict]:
    return [r for r in rules if approvals.get(str(r.get("image_url", "")).strip(), 0) >= threshold]
