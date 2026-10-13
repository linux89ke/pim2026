"""Import legacy Excel/hash catalogs into learned_image_rules.json.

Examples:
  python migrate_learned_rules.py
  python migrate_learned_rules.py --excel old1.xlsx --excel old2.xlsx --cache old_cache.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from learned_rules import LEARNABLE_FLAGS, RULES_PATH, normalize_learned_flag

def _load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def migrate(excel_paths, cache_paths, output: Path) -> tuple[int, int]:
    existing = []
    if output.exists():
        # Fail closed. Treating a damaged/unreadable catalog as empty would
        # replace it with only the imported rows and silently discard rules.
        with output.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, list):
            raise ValueError(f"Refusing to overwrite {output}: expected a JSON array")
        existing = value

    cached_by_url = {}
    for cache_path in cache_paths:
        for url, value in _load_json(Path(cache_path)).items():
            if isinstance(value, dict):
                cached_by_url[str(url).strip()] = value

    imported = list(existing)
    keys = {
        (str(row.get("image_url", "")).strip(), str(row.get("phash", "")).strip(), row.get("flag", ""))
        for row in imported
    }
    now = datetime.now(timezone.utc).isoformat()

    for excel_path in excel_paths:
        path = Path(excel_path)
        if not path.exists():
            print(f"Skipped missing file: {path}")
            continue
        try:
            frame = pd.read_excel(path, dtype=str).fillna("")
        except Exception as exc:
            print(f"Skipped unreadable file {path}: {exc}")
            continue
        frame.columns = [str(c).strip().upper().replace(" ", "_") for c in frame.columns]
        url_col = next((c for c in frame.columns if "URL" in c or "IMAGE" in c), None)
        flag_col = next((c for c in frame.columns if "FLAG" in c), None)
        brand_col = next((c for c in frame.columns if "BRAND" in c), None)
        if not url_col or not flag_col:
            print(f"Skipped {path}: missing image URL or flag column")
            continue

        for _, row in frame.iterrows():
            url = str(row.get(url_col, "")).strip()
            raw_flag = str(row.get(flag_col, "")).strip()
            if not url:
                continue
            flag = normalize_learned_flag(raw_flag) or "Poor images"
            if flag not in LEARNABLE_FLAGS:
                print(f"Skipped unsupported flag {raw_flag!r} for {url}")
                continue
            cache = cached_by_url.get(url, {})
            phash = str(cache.get("phash", "") or "").strip()
            key = (url, phash, flag)
            if key in keys:
                continue
            keys.add(key)
            imported.append({
                "image_url": url,
                "phash": phash,
                "flag": flag,
                "reason": "Imported from legacy image catalog",
                "comment": "",
                "brand": str(row.get(brand_col, "") if brand_col else cache.get("brand", "")).strip(),
                "category_code": "",
                "product_set_sid": "",
                "seller_name": "",
                "status": "active",
                "source": "legacy-import",
                "created_at": now,
            })

    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".json.tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(imported, handle, indent=2, ensure_ascii=False)
    # `imported` starts with every existing rule; migrations are additive only.
    # A same-directory replace keeps readers from observing a partial JSON file.
    temp.replace(output)
    return len(imported) - len(existing), len(imported)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--excel", action="append", default=None, help="Legacy Excel catalog; repeatable")
    parser.add_argument("--cache", action="append", default=None, help="Legacy hash cache; repeatable")
    parser.add_argument("--output", default=str(RULES_PATH), help="Destination JSON rule catalog")
    args = parser.parse_args()
    added, total = migrate(args.excel or ["Image_learn.xlsx"], args.cache or ["Image_learn_cache.json"], Path(args.output))
    print(f"Added {added:,} rules; catalog now contains {total:,} rules: {args.output}")


if __name__ == "__main__":
    main()
