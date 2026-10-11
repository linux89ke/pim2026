"""Import historical rejection reports into the learned image-rule catalog."""

import json
import hashlib
import os
import shutil
import zipfile
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import r2_storage

from learned_rules import (
    LEARNABLE_FLAGS,
    delete_learned_image_rules,
    load_learned_image_rules,
    learn_image_rejections_bulk,
    normalize_learned_flag,
    restore_learned_image_rules,
    set_learned_image_rule_review,
    update_learned_image_rule_flag,
)


def _configure_rar_backend(rarfile_module):
    """Configure rarfile with the local or Streamlit Cloud extractor."""
    configured = os.environ.get("UNRAR_TOOL", "").strip()
    candidates = [configured] if configured else []
    candidates.extend([
        shutil.which("7z"), shutil.which("7zz"), shutil.which("unrar"),
        shutil.which("unar"), shutil.which("bsdtar"),
        os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "WinRAR", "UnRAR.exe"),
        os.path.join(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"), "WinRAR", "UnRAR.exe"),
    ])
    for candidate in candidates:
        if candidate and (os.path.isfile(candidate) or shutil.which(candidate)):
            if os.path.basename(candidate).lower() in {"7z", "7z.exe", "7zz", "7zz.exe"}:
                rarfile_module.SEVENZIP_TOOL = candidate
                rarfile_module.SEVENZIP2_TOOL = candidate
            elif os.path.basename(candidate).lower() in {"unar", "unar.exe"}:
                rarfile_module.UNAR_TOOL = candidate
            else:
                rarfile_module.UNRAR_TOOL = candidate
            return candidate
    return None


def _iter_archive_entries(archive_name: str, payload: bytes):
    """Yield (member_name, bytes) for supported members inside ZIP/RAR archives."""
    lower_name = archive_name.lower()
    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(BytesIO(payload)) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                member_name = info.filename
                member_lower = member_name.lower()
                if member_lower.endswith((".xlsx", ".xls", ".csv")):
                    yield member_name, archive.read(info)
        return

    if lower_name.endswith(".rar"):
        try:
            import rarfile
        except ImportError as exc:  # pragma: no cover - environment dependency
            raise RuntimeError("RAR support requires the rarfile package.") from exc

        if not _configure_rar_backend(rarfile):
            raise RuntimeError(
                "RAR extraction backend not found. Install 7-Zip/WinRAR or set UNRAR_TOOL."
            )

        with rarfile.RarFile(BytesIO(payload), "r") as archive:
            for info in archive.infolist():
                if info.isdir():
                    continue
                member_name = info.filename
                member_lower = member_name.lower()
                if member_lower.endswith((".xlsx", ".xls", ".csv")):
                    yield member_name, archive.read(info)
        return


st.set_page_config(page_title="Dashboard", page_icon=":material/auto_awesome:", layout="wide")
st.title("Dashboard")
st.caption("Upload old rejection reports. Only rejected rows with an image and a supported flag are imported; duplicate URL/pHash/flag rules are skipped.")
_runtime_review_matches = st.session_state.get("_learned_review_matches", {})
if _runtime_review_matches:
    st.info(f":material/rate_review: {len(_runtime_review_matches):,} uncertain near-pHash match(es) were sent to review instead of automatic rejection. Open the validation review queue to inspect[...]
