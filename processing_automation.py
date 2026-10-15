"""Large-file validation helpers: manifests, thumbnails, similarity and review queues."""
from __future__ import annotations
import base64, time, hashlib, io, json, os, re, threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from collections import OrderedDict
from typing import Iterable
import r2_storage

ROOT = Path(__file__).resolve().parent
AUTOMATION_DIR = ROOT / ".validation_automation"
MANIFEST_DIR = AUTOMATION_DIR / "manifests"
THUMB_DIR = AUTOMATION_DIR / "thumbnails"
IMAGE_CACHE_PATH = AUTOMATION_DIR / "image_metadata.json"
IMAGE_JOURNAL_PATH = AUTOMATION_DIR / "image_metadata.checkpoints.jsonl"
MANIFEST_DIR.mkdir(exist_ok=True, parents=True)
THUMB_DIR.mkdir(exist_ok=True, parents=True)
DOWNLOAD_POOL = ThreadPoolExecutor(max_workers=6, thread_name_prefix="image-download")
_LOCK = threading.RLock()
_VALIDATION_LOCKS: OrderedDict[str, threading.Lock] = OrderedDict()
_LAST_R2_CACHE_UPLOAD = 0.0


def validation_lock(signature: str) -> threading.Lock:
    """Return a process-shared lock for one upload's validation artifacts."""
    key = str(signature or "")
    with _LOCK:
        lock = _VALIDATION_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _VALIDATION_LOCKS[key] = lock
        _VALIDATION_LOCKS.move_to_end(key)
        # Bound the registry without evicting a lock currently protecting work.
        while len(_VALIDATION_LOCKS) > 64:
            for old_key, old_lock in tuple(_VALIDATION_LOCKS.items()):
                if old_key != key and not old_lock.locked():
                    _VALIDATION_LOCKS.pop(old_key, None)
                    break
            else:
                break
        return lock

def load_image_metadata_cache(max_entries: int = 50000) -> dict:
    """Load the durable URL -> dimensions/pHash cache.

    This is a small derived cache, deliberately separate from learned rules.
    It can be deleted safely and is ignored by Git.  Invalid or oversized
    entries are ignored so a damaged cache never prevents validation startup.
    """
    try:
        if IMAGE_CACHE_PATH.exists():
            raw = IMAGE_CACHE_PATH.read_bytes()
        else:
            raw = r2_storage.download_bytes("validation/image_metadata.json")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            return {}
        entries = payload.get("entries", payload)
        if not isinstance(entries, dict):
            return {}
        entries = dict(list(entries.items())[-max_entries:])
    except (OSError, ValueError, TypeError):
        entries = {}
    try:
        with IMAGE_JOURNAL_PATH.open("r", encoding="utf-8") as journal:
            for line in journal:
                try:
                    item = json.loads(line)
                    url, value = str(item.get("url") or "").strip(), item.get("value")
                    if url and isinstance(value, dict):
                        entries[url] = value
                except (ValueError, TypeError):
                    continue
    except OSError:
        pass
    return dict(list(entries.items())[-max_entries:])


def append_image_metadata_checkpoint(entries: dict) -> None:
    """Journal successful image checks so an interrupted run can reuse them."""
    if not isinstance(entries, dict) or not entries:
        return
    now = datetime.now(timezone.utc).isoformat()
    lines = []
    for url, value in entries.items():
        if not str(url).strip() or not isinstance(value, dict):
            continue
        size, phash = value.get("size"), str(value.get("phash") or "").strip()
        if not phash and not size:
            continue
        lines.append(json.dumps({"url": str(url), "value": {
            "size": list(size) if isinstance(size, (tuple, list)) and len(size) == 2 else None,
            "phash": phash, "updated_at": value.get("updated_at") or now,
        }}, ensure_ascii=False, separators=(",", ":")))
    if not lines:
        return
    try:
        with _LOCK, IMAGE_JOURNAL_PATH.open("a", encoding="utf-8") as journal:
            journal.write("\n".join(lines) + "\n")
            journal.flush()
            os.fsync(journal.fileno())
    except OSError:
        pass

def save_image_metadata_cache(entries: dict, max_entries: int = 50000) -> None:
    """Atomically persist successful image metadata without touching rules."""
    if not isinstance(entries, dict):
        return
    with _LOCK:
        existing = load_image_metadata_cache(max_entries=max_entries)
        existing.update(entries)
        clean = {}
        for url, value in list(existing.items())[-max_entries:]:
            if not str(url).strip() or not isinstance(value, dict):
                continue
            size = value.get("size")
            phash = str(value.get("phash") or "").strip()
            if not phash and not size:
                continue
            clean[str(url)] = {
                "size": list(size) if isinstance(size, (tuple, list)) and len(size) == 2 else None,
                "phash": phash,
                "updated_at": value.get("updated_at") or datetime.now(timezone.utc).isoformat(),
            }
        try:
            tmp = IMAGE_CACHE_PATH.with_suffix(".tmp")
            tmp.write_text(json.dumps({"version": 1, "entries": clean}, separators=(",", ":")), encoding="utf-8")
            replaced = False
            for _attempt in range(5):
                try:
                    os.replace(tmp, IMAGE_CACHE_PATH)
                    replaced = True
                    break
                except (PermissionError, OSError):
                    time.sleep(0.05 * (2 ** _attempt))
            if replaced:
                try:
                    IMAGE_JOURNAL_PATH.write_text("", encoding="utf-8")
                except OSError:
                    pass
            global _LAST_R2_CACHE_UPLOAD
            if r2_storage.enabled() and time.monotonic() - _LAST_R2_CACHE_UPLOAD > 30:
                _LAST_R2_CACHE_UPLOAD = time.monotonic()
                r2_storage.upload_bytes_async(
                    json.dumps({"version": 1, "entries": clean}, separators=(",", ":")).encode("utf-8"),
                    "validation/image_metadata.json",
                    "application/json",
                )
        except OSError:
            pass

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
