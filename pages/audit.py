"""
Category QC Checker - Streamlit app

Upload a CSV export (like cate.csv) and this app will, for each row:
  1. Ask the AI what the product actually IS and what it's used for
  2. Ask the AI whether the assigned CATEGORY is correct given that
  3. Compare that verdict against what your QC pipeline already decided
     (Category_Check_Status) and flag mismatches with a reason

Run locally with:
    streamlit run category_checker_app.py

You'll need an OpenAI-compatible endpoint (e.g. Jumia's ai-gateway) with
a base_url + api_key + model name, entered in the sidebar.
"""

import hashlib
import itertools
import json
import os
import sqlite3
import time
import concurrent.futures as cf

import pandas as pd
import streamlit as st
from openai import OpenAI

KEYS_FILE = os.path.join(os.path.dirname(__file__), "keys.txt")
CACHE_DB = os.path.join(os.path.dirname(__file__), "verdict_cache.sqlite")


def _init_cache_db():
    conn = sqlite3.connect(CACHE_DB)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS verdict_cache (
            content_hash TEXT PRIMARY KEY,
            result_json TEXT NOT NULL,
            created_at REAL NOT NULL
        )"""
    )
    conn.commit()
    conn.close()


def row_content_hash(row: dict, cols: list) -> str:
    """Hash exactly what gets sent to the AI for this row, so identical
    products (same name/category/description/etc across duplicate listings)
    reuse the same verdict instead of re-calling the API."""
    parts = []
    for col in cols:
        val = row.get(col, "")
        if pd.isna(val):
            val = ""
        parts.append(f"{col}={val}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_cached_verdicts(hashes: list) -> dict:
    """Return {content_hash: result_dict} for any hashes already cached."""
    if not hashes:
        return {}
    conn = sqlite3.connect(CACHE_DB)
    placeholders = ",".join("?" for _ in hashes)
    rows = conn.execute(
        f"SELECT content_hash, result_json FROM verdict_cache WHERE content_hash IN ({placeholders})",
        hashes,
    ).fetchall()
    conn.close()
    return {h: json.loads(rj) for h, rj in rows}


def store_verdicts(hash_result_pairs: list):
    """hash_result_pairs: list of (content_hash, result_dict)"""
    if not hash_result_pairs:
        return
    conn = sqlite3.connect(CACHE_DB)
    conn.executemany(
        "INSERT OR REPLACE INTO verdict_cache (content_hash, result_json, created_at) VALUES (?, ?, ?)",
        [(h, json.dumps(r), time.time()) for h, r in hash_result_pairs],
    )
    conn.commit()
    conn.close()


def load_keys_from_file() -> list:
    """Load API keys from keys.txt sitting next to this script.
    Each line is either just a key, or "key:model_name" to tag which model
    that key is authorized/intended for (e.g. if keys are scoped per-model).
    Returns a list of (key, model_hint_or_None) tuples."""
    if not os.path.exists(KEYS_FILE):
        return []
    entries = []
    with open(KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if ":" in line and line.count(":") == 1 and not line.startswith("http"):
                key, hint = line.split(":", 1)
                entries.append((key.strip(), hint.strip()))
            else:
                entries.append((line, None))
    return entries

st.set_page_config(page_title="Category QC Checker", layout="wide")

# ---------- Columns we care about, all optional except NAME/CATEGORY ----------
CATEGORY_COLS = [
    "NAME",
    "CATEGORY",
    "CATEGORY_CODE",
    "Initial_Category_Path",
    "Suggested_Categories",
    "Top1_Category",
    "Top1_Score",
    "Category_Match_Score",
    "Category_Check_Status",
    "Category_Check_Rejection_Reason",
    "DESCRIPTION",
    "SHORT_DESCRIPTION",
    "BRAND",
]

SYSTEM_PROMPT = """You are a strict e-commerce catalog QC auditor for Jumia.

You will be given a product's name, description, brand, and all category-related
fields from our pipeline. Pay special attention to these two fields:

- Initial_Category_Path: this is the FULL category tree path the product is
  currently placed in, from top level down to the most specific leaf category
  (e.g. "Home & Office / Home & Furniture / Lighting / Lighting Bulbs & Component /
  Lighting Bulb / Specialty Bulb"). Use this to understand exactly where in the
  tree the product sits today, not just the leaf CATEGORY name.
- Category_Check_Rejection_Reason: if present, this is our existing pipeline's
  stated reason for rejecting the category. Use it to understand what our
  pipeline THOUGHT was wrong, then judge whether that reasoning was actually
  correct or a mistake.

Your job, in order:
1. Identify what the product actually IS and its primary USE CASE, based only on
   the name and description text (ignore the category fields for this step).
2. Using the full Initial_Category_Path (all levels, not just the leaf), decide
   whether the product's CURRENT placement in the tree is correct for that
   product and use case.
3. If Category_Check_Rejection_Reason is present, evaluate whether that specific
   stated reason is valid or flawed given what the product actually is.
4. Compare your independent verdict to our existing Category_Check_Status.
   - If our pipeline APPROVED the category and you agree it's correct -> verdict "correct_approval"
   - If our pipeline APPROVED the category but you think it's wrong -> verdict "wrong_approval"
   - If our pipeline REJECTED the category and you agree the rejection reason is valid -> verdict "correct_rejection"
   - If our pipeline REJECTED the category but you think the rejection reason is flawed and the category is actually fine (or the rejection reason doesn't match the actual product) -> verdict "wrong_rejection"
   - If there was no clear approve/reject status to compare against, just give your own
     independent judgement as "correct_category" or "wrong_category"

Respond with ONLY valid JSON, no markdown, no preamble, in this exact shape:
{
  "product_type": "short description of what the product is and its use",
  "full_category_path_assessment": "your judgement on whether the full Initial_Category_Path placement (all levels) fits the product",
  "rejection_reason_assessment": "if Category_Check_Rejection_Reason was given, state whether it was valid or flawed and why; otherwise empty string",
  "correct_category_guess": "what you think the right category should be",
  "verdict": "one of: correct_approval | wrong_approval | correct_rejection | wrong_rejection | correct_category | wrong_category",
  "reason": "1-2 sentence explanation of your verdict, referencing the mismatch if any"
}
"""


BATCH_SYSTEM_PROMPT_LITE = """You are a strict e-commerce catalog QC auditor for Jumia.

You will be given a JSON array of products, each with name, description, brand,
and category-related fields. Pay special attention to two fields:

- Initial_Category_Path: the FULL category tree path the product currently sits
  in, top level down to leaf. Judge placement using this whole path, not just
  the leaf CATEGORY name.
- Category_Check_Rejection_Reason: if present, our pipeline's stated reason for
  rejecting the category. Judge whether that specific stated reason is valid.

For each product:
1. Identify what the product actually IS and its primary USE CASE from name and
   description alone.
2. Judge whether the full Initial_Category_Path placement fits that use case,
   and whether any given rejection reason is valid or flawed.
3. Compare your verdict to the pipeline's Category_Check_Status:
   - pipeline APPROVED, you agree -> "correct_approval"
   - pipeline APPROVED, you disagree -> "wrong_approval"
   - pipeline REJECTED, you agree with the reason -> "correct_rejection"
   - pipeline REJECTED, you think the reason is flawed / category is actually fine -> "wrong_rejection"
   - no clear status to compare -> "correct_category" or "wrong_category"

Respond with ONLY a valid JSON array, no markdown, no preamble, same order as input,
one compact object per product:
{
  "id": "<the id field from input, copied exactly>",
  "verdict": "one of: correct_approval | wrong_approval | correct_rejection | wrong_rejection | correct_category | wrong_category",
  "reason": "1 short sentence explanation, max ~20 words"
}
"""


BATCH_SYSTEM_PROMPT_FULL = """You are a strict e-commerce catalog QC auditor for Jumia,
performing a careful SECOND-OPINION review of products a faster first-pass model
already flagged as a likely mismatch. Be thorough and precise here.

You will be given a JSON array of products, each with name, description, brand,
and category-related fields. Pay special attention to two fields:

- Initial_Category_Path: the FULL category tree path the product currently sits
  in, top level down to leaf. Judge placement using this whole path, not just
  the leaf CATEGORY name.
- Category_Check_Rejection_Reason: if present, our pipeline's stated reason for
  rejecting the category. Judge whether that specific stated reason is valid.

For each product, in order:
1. Identify what the product actually IS and its primary USE CASE from name and
   description alone.
2. Judge whether the full Initial_Category_Path placement fits that use case.
3. If a rejection reason is given, judge whether it's valid or flawed.
4. Compare your verdict to the pipeline's Category_Check_Status:
   - pipeline APPROVED, you agree -> "correct_approval"
   - pipeline APPROVED, you disagree -> "wrong_approval"
   - pipeline REJECTED, you agree with the reason -> "correct_rejection"
   - pipeline REJECTED, you think the reason is flawed / category is actually fine -> "wrong_rejection"
   - no clear status to compare -> "correct_category" or "wrong_category"

Respond with ONLY a valid JSON array, no markdown, no preamble, same order as input,
one object per product, each shaped exactly like:
{
  "id": "<the id field from input, copied exactly>",
  "product_type": "short description of what the product is and its use",
  "full_category_path_assessment": "your judgement on the full path placement",
  "rejection_reason_assessment": "valid/flawed judgement on the stated rejection reason, or empty string if none given",
  "correct_category_guess": "what you think the right category should be",
  "verdict": "one of: correct_approval | wrong_approval | correct_rejection | wrong_rejection | correct_category | wrong_category",
  "reason": "1-2 sentence explanation"
}
"""


def build_batch_user_prompt(rows: list, desc_limit: int = 500) -> str:
    compact_rows = []
    for i, row in enumerate(rows):
        entry = {"id": str(i)}
        for col in CATEGORY_COLS:
            val = row.get(col, "")
            if pd.isna(val) or val == "":
                continue
            val_str = str(val)
            if col in ("DESCRIPTION", "SHORT_DESCRIPTION") and len(val_str) > desc_limit:
                val_str = val_str[:desc_limit] + "...[truncated]"
            entry[col] = val_str
        compact_rows.append(entry)
    return json.dumps(compact_rows, ensure_ascii=False)


LITE_EMPTY_RESULT = {"verdict": "error", "reason": ""}
FULL_EMPTY_RESULT = {
    "product_type": "",
    "full_category_path_assessment": "",
    "rejection_reason_assessment": "",
    "correct_category_guess": "",
    "verdict": "error",
    "reason": "",
}


def call_ai_batch(client: OpenAI, model: str, rows: list, timeout: int = 90, lite: bool = True) -> list:
    """Send a batch of rows in a single API call, return list of result dicts
    in the same order as input rows.

    lite=True (default, used for the fast Haiku pass): slimmer prompt + schema
    (verdict + short reason only) — smaller request/response, faster, cheaper.
    lite=False (used for the GPT-4o-mini escalation pass): full schema with detailed
    assessments, since those rows are the minority worth the extra depth.
    """
    desc_limit = 250 if lite else 500
    user_prompt = build_batch_user_prompt(rows, desc_limit=desc_limit)
    system_prompt = BATCH_SYSTEM_PROMPT_LITE if lite else BATCH_SYSTEM_PROMPT_FULL
    empty_result = LITE_EMPTY_RESULT if lite else FULL_EMPTY_RESULT
    tokens_per_row = 120 if lite else 400
    text = ""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": system_prompt + "\n\n---\n\n" + user_prompt},
            ],
            max_tokens=min(tokens_per_row * len(rows) + 150, 8000),
            timeout=timeout,
        )
        text = resp.choices[0].message.content.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        parsed_list = json.loads(text)

        by_id = {str(item.get("id")): item for item in parsed_list}
        ordered = []
        for i in range(len(rows)):
            item = by_id.get(str(i))
            if item is None:
                item = dict(empty_result)
                item["reason"] = "Missing from batch response"
            ordered.append(item)
        return ordered
    except json.JSONDecodeError:
        err = dict(empty_result)
        err["reason"] = f"Could not parse batch AI response as JSON: {text[:200]}"
        return [dict(err) for _ in rows]
    except Exception as e:
        err = dict(empty_result)
        err["reason"] = f"API error: {e}"
        return [dict(err) for _ in rows]


def build_user_prompt(row: dict) -> str:
    lines = []
    for col in CATEGORY_COLS:
        val = row.get(col, "")
        if pd.isna(val) or val == "":
            continue
        # trim long text fields so we don't blow context on huge HTML descriptions
        val_str = str(val)
        if col in ("DESCRIPTION", "SHORT_DESCRIPTION") and len(val_str) > 800:
            val_str = val_str[:800] + "... [truncated]"
        lines.append(f"{col}: {val_str}")
    return "\n".join(lines)


def call_ai(client: OpenAI, model: str, row: dict, timeout: int = 60) -> dict:
    user_prompt = build_user_prompt(row)
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": SYSTEM_PROMPT + "\n\n---\n\n" + user_prompt},
            ],
            max_tokens=500,
            timeout=timeout,
        )
        text = resp.choices[0].message.content.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        parsed = json.loads(text)
        return parsed
    except json.JSONDecodeError:
        return {
            "product_type": "",
            "full_category_path_assessment": "",
            "rejection_reason_assessment": "",
            "correct_category_guess": "",
            "verdict": "error",
            "reason": f"Could not parse AI response as JSON: {text[:200]}",
        }
    except Exception as e:
        return {
            "product_type": "",
            "full_category_path_assessment": "",
            "rejection_reason_assessment": "",
            "correct_category_guess": "",
            "verdict": "error",
            "reason": f"API error: {e}",
        }


VERDICT_LABELS = {
    "correct_approval": "✅ Correctly Approved",
    "wrong_approval": "❌ Wrongly Approved",
    "correct_rejection": "✅ Correctly Rejected",
    "wrong_rejection": "❌ Wrongly Rejected",
    "correct_category": "✅ Category Correct",
    "wrong_category": "❌ Category Wrong",
    "error": "⚠️ Error",
}


def main():
    st.title("🗂️ Category QC Checker")
    st.caption(
        "Upload a QC export CSV. For each row the AI independently judges the "
        "product's category and compares it to your pipeline's existing verdict."
    )

    with st.sidebar:
        st.header("API Settings")
        base_url = st.text_input(
            "Base URL",
            value="https://ai-gateway.zuma.jumia.com/v1",
            help="OpenAI-compatible endpoint base URL",
        )
        file_keys = load_keys_from_file()  # list of (key, model_hint_or_None)

        if file_keys:
            st.success(f"Loaded {len(file_keys)} key(s) from keys.txt")
            st.caption(
                "Assign each key to a model below. Tip: tag keys directly in "
                "keys.txt as `key:haiku` or `key:gpt4o` to skip this step."
            )
            key_model_map = []
            for i, (k, hint) in enumerate(file_keys):
                default_idx = 0
                hint_lower = hint.lower() if hint else ""
                if any(tag in hint_lower for tag in ("gpt4o", "gpt-4o", "openai")):
                    default_idx = 1
                choice = st.selectbox(
                    f"Key {i+1} ({k[:12]}...)",
                    ["claude-haiku-4.5", "gpt-4o-mini"],
                    index=default_idx,
                    key=f"key_model_{i}",
                )
                key_model_map.append((k, choice))
            haiku_keys = [k for k, m in key_model_map if m == "claude-haiku-4.5"]
            escalation_keys = [k for k, m in key_model_map if m == "gpt-4o-mini"]
        else:
            st.caption("No keys.txt found. Paste a single key below (Haiku only).")
            api_key = st.text_input("API Key", type="password")
            haiku_keys = [api_key] if api_key else []
            escalation_keys = []

        st.divider()
        st.subheader("⚡ Speed settings")
        max_workers = st.slider("Parallel requests", min_value=1, max_value=40, value=20)
        batch_size = st.slider(
            "Rows per API call (batching)",
            min_value=1, max_value=20, value=10,
            help="Higher = fewer requests = faster, but risk of truncation on very large batches.",
        )
        use_escalation = st.checkbox(
            "Escalate uncertain rows to GPT-4o-mini",
            value=bool(escalation_keys),
            disabled=not escalation_keys,
            help="Haiku handles all rows first (fast). Rows where Haiku flags a "
            "mismatch (wrong_approval/wrong_rejection) get a second, more careful "
            "pass with GPT-4o-mini to confirm — since those are the cases worth double-checking.",
        )
        use_cache = st.checkbox(
            "Reuse cached verdicts for duplicate/repeat products",
            value=True,
            help="Skips the API call entirely for any row whose name + category "
            "fields exactly match a row already verdicted in a previous run "
            "(stored in verdict_cache.sqlite next to this script).",
        )
        auto_trust_threshold = st.slider(
            "Auto-trust approved rows with match score ≥",
            min_value=0.0, max_value=1.0, value=0.95, step=0.01,
            help="Rows already Approved by the pipeline with a Top1_Score/"
            "Category_Match_Score at or above this value skip the AI check "
            "entirely and are marked 'correct_approval' automatically. Set to "
            "1.01 to disable this shortcut.",
        )
        if os.path.exists(CACHE_DB):
            cache_size_kb = os.path.getsize(CACHE_DB) / 1024
            st.caption(f"Cache file: {cache_size_kb:.0f} KB")
            if st.button("🗑️ Clear cache"):
                os.remove(CACHE_DB)
                st.rerun()
        row_limit = st.number_input(
            "Max rows to process (0 = all)", min_value=0, value=0, step=50,
            help="Set to 0 to process every row in the uploaded file.",
        )

    uploaded = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx", "xls"])
    if not uploaded:
        st.info("Upload a CSV or Excel file to get started.")
        return

    if uploaded.name.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(uploaded)
    else:
        df = pd.read_csv(uploaded)
    st.write(f"Loaded **{len(df)}** rows, **{len(df.columns)}** columns from **{uploaded.name}**.")

    missing = [c for c in ["NAME", "CATEGORY"] if c not in df.columns]
    if missing:
        st.error(f"Missing required columns: {missing}")
        return

    available_cols = [c for c in CATEGORY_COLS if c in df.columns]
    with st.expander("Columns that will be sent to the AI"):
        st.write(available_cols)

    if row_limit and row_limit > 0:
        df_to_process = df.head(int(row_limit)).copy()
        st.warning(
            f"⚠️ Row limit is set to {row_limit} — only the first {row_limit} of "
            f"{len(df)} rows will be processed. Set 'Max rows to process' to 0 in "
            f"the sidebar to run all rows."
        )
    else:
        df_to_process = df.copy()
        st.info(f"Will process all **{len(df_to_process)}** rows.")

    est_batches = -(-len(df_to_process) // batch_size)  # ceil division
    st.caption(f"This will make approximately **{est_batches}** API calls (batch size {batch_size}).")

    run = st.button("🚀 Run Category Check", type="primary", disabled=not haiku_keys)
    if not haiku_keys:
        st.warning("Add at least one Haiku key in the sidebar to enable this.")

    if run:
        _init_cache_db()

        haiku_clients = itertools.cycle([OpenAI(base_url=base_url, api_key=k) for k in haiku_keys])
        escalation_clients = (
            itertools.cycle([OpenAI(base_url=base_url, api_key=k) for k in escalation_keys])
            if escalation_keys else None
        )

        rows = df_to_process[available_cols].to_dict(orient="records")
        results = [None] * len(rows)

        # --- CACHE LOOKUP: skip rows we've already verdicted before ---
        row_hashes = [row_content_hash(r, available_cols) for r in rows]
        if use_cache:
            cached = get_cached_verdicts(list(set(row_hashes)))
            cache_hits = 0
            for i, h in enumerate(row_hashes):
                if h in cached:
                    results[i] = dict(cached[h])
                    results[i]["from_cache"] = True
                    cache_hits += 1
            if cache_hits:
                st.info(
                    f"⚡ {cache_hits}/{len(rows)} rows matched a previously-cached "
                    f"verdict (identical name/category/description) — skipping the API call for those."
                )

        # --- AUTO-TRUST: pipeline-approved rows with high match confidence skip AI too ---
        if auto_trust_threshold <= 1.0:
            auto_trust_hits = 0
            for i, row in enumerate(rows):
                if results[i] is not None:
                    continue  # already resolved by cache
                status = str(row.get("Category_Check_Status", "")).strip().lower()
                if status != "approved":
                    continue
                score = row.get("Top1_Score", row.get("Category_Match_Score", None))
                try:
                    score = float(score)
                except (TypeError, ValueError):
                    continue
                if score >= auto_trust_threshold:
                    results[i] = {
                        "verdict": "correct_approval",
                        "reason": f"Auto-trusted: pipeline approved with match score {score:.2f} ≥ {auto_trust_threshold:.2f}",
                        "auto_trusted": True,
                    }
                    auto_trust_hits += 1
            if auto_trust_hits:
                st.info(
                    f"🎯 {auto_trust_hits}/{len(rows)} rows auto-trusted "
                    f"(pipeline-approved, match score ≥ {auto_trust_threshold:.2f}) — skipping AI for those too."
                )

        uncached_indices = [i for i, r in enumerate(results) if r is None]

        rows_to_send = [rows[i] for i in uncached_indices]
        chunks = [
            (uncached_indices[start], rows_to_send[start : start + batch_size], uncached_indices[start : start + batch_size])
            for start in range(0, len(rows_to_send), batch_size)
        ]

        progress = st.progress(0.0)
        status_text = st.empty()
        done_chunks = 0
        start_time = time.time()

        # --- PASS 1: Haiku on uncached rows only, fast + cheap ---
        def haiku_worker(chunk):
            _placeholder, chunk_rows, orig_indices = chunk
            client = next(haiku_clients)
            batch_results = call_ai_batch(client, "claude-haiku-4.5", chunk_rows, lite=True)
            return orig_indices, batch_results

        new_verdicts_to_cache = []
        if not chunks:
            status_text.text("✅ All rows served from cache — no API calls needed for Pass 1.")
            progress.progress(0.7 if use_escalation and escalation_clients else 1.0)
        else:
            with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(haiku_worker, c) for c in chunks]
                for future in cf.as_completed(futures):
                    orig_indices, batch_results = future.result()
                    for orig_i, result in zip(orig_indices, batch_results):
                        results[orig_i] = result
                        if use_cache and result.get("verdict") != "error":
                            new_verdicts_to_cache.append((row_hashes[orig_i], result))
                    done_chunks += 1
                    progress.progress(done_chunks / len(chunks) * (0.7 if use_escalation and escalation_clients else 1.0))
                    status_text.text(
                        f"[Haiku] batch {done_chunks}/{len(chunks)} "
                        f"({time.time() - start_time:.1f}s elapsed)"
                    )

        if use_cache and new_verdicts_to_cache:
            store_verdicts(new_verdicts_to_cache)

        # --- PASS 2: GPT-4o-mini re-checks only NEWLY-flagged rows (skip cache hits, since
        # a cached row already went through this pipeline in a prior run) ---
        if use_escalation and escalation_clients:
            escalate_idx = [
                i for i, r in enumerate(results)
                if r and not r.get("from_cache")
                and r.get("verdict") in ("wrong_approval", "wrong_rejection", "wrong_category")
            ]
            if escalate_idx:
                status_text.text(
                    f"[GPT-4o-mini] double-checking {len(escalate_idx)} flagged row(s)..."
                )
                escalate_rows = [rows[i] for i in escalate_idx]
                escalate_chunks = [
                    (pos, escalate_idx[pos : pos + batch_size], escalate_rows[pos : pos + batch_size])
                    for pos in range(0, len(escalate_idx), batch_size)
                ]

                def escalation_worker(chunk):
                    pos, orig_idxs, chunk_rows = chunk
                    client = next(escalation_clients)
                    batch_results = call_ai_batch(client, "gpt-4o-mini", chunk_rows, lite=False)
                    return orig_idxs, batch_results

                escalation_verdicts_to_cache = []
                done_escalation = 0
                with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(escalation_worker, c) for c in escalate_chunks]
                    for future in cf.as_completed(futures):
                        orig_idxs, batch_results = future.result()
                        for orig_i, result in zip(orig_idxs, batch_results):
                            result["escalated_to_gpt4o"] = True
                            results[orig_i] = result
                            if use_cache and result.get("verdict") != "error":
                                escalation_verdicts_to_cache.append((row_hashes[orig_i], result))
                        done_escalation += 1
                        progress.progress(0.7 + 0.3 * done_escalation / len(escalate_chunks))
                        status_text.text(
                            f"[GPT-4o-mini] confirmed {done_escalation}/{len(escalate_chunks)} batches "
                            f"({time.time() - start_time:.1f}s elapsed)"
                        )
                if use_cache and escalation_verdicts_to_cache:
                    store_verdicts(escalation_verdicts_to_cache)
            else:
                progress.progress(1.0)

        progress.progress(1.0)
        total_time = time.time() - start_time
        if total_time < 0.01:
            st.success(f"✅ Done instantly ({len(rows)} rows, all served from cache/auto-trust)")
        else:
            st.success(f"✅ Done in {total_time:.1f}s ({len(rows)/total_time:.1f} rows/sec)")

        result_df = pd.DataFrame(results)
        for col in [
            "product_type",
            "full_category_path_assessment",
            "rejection_reason_assessment",
            "correct_category_guess",
            "verdict",
            "reason",
            "escalated_to_gpt4o",
            "from_cache",
            "auto_trusted",
        ]:
            if col not in result_df.columns:
                result_df[col] = False if col in ("escalated_to_gpt4o", "from_cache", "auto_trusted") else ""
        result_df["escalated_to_gpt4o"] = result_df["escalated_to_gpt4o"].fillna(False)
        result_df["from_cache"] = result_df["from_cache"].fillna(False)
        result_df["auto_trusted"] = result_df["auto_trusted"].fillna(False)
        result_df["Verdict_Label"] = result_df["verdict"].map(VERDICT_LABELS).fillna(result_df["verdict"])

        def _model_label(row):
            if row["auto_trusted"]:
                return "Auto-trusted (no AI)"
            if row["from_cache"]:
                return "Cached"
            if row["escalated_to_gpt4o"]:
                return "GPT-4o-mini (confirmed)"
            return "Haiku"

        result_df["Model"] = result_df.apply(_model_label, axis=1)

        final = pd.concat(
            [
                df_to_process[["NAME", "CATEGORY"]].reset_index(drop=True),
                df_to_process["Initial_Category_Path"].reset_index(drop=True)
                if "Initial_Category_Path" in df_to_process.columns
                else pd.Series([""] * len(df_to_process), name="Initial_Category_Path"),
                df_to_process["Category_Check_Status"].reset_index(drop=True)
                if "Category_Check_Status" in df_to_process.columns
                else pd.Series([""] * len(df_to_process), name="Category_Check_Status"),
                df_to_process["Category_Check_Rejection_Reason"].reset_index(drop=True)
                if "Category_Check_Rejection_Reason" in df_to_process.columns
                else pd.Series([""] * len(df_to_process), name="Category_Check_Rejection_Reason"),
                result_df[
                    [
                        "Verdict_Label",
                        "Model",
                        "product_type",
                        "full_category_path_assessment",
                        "rejection_reason_assessment",
                        "correct_category_guess",
                        "reason",
                    ]
                ],
            ],
            axis=1,
        )
        final.columns = [
            "Product Name",
            "Assigned Category",
            "Full Category Path",
            "Pipeline Status",
            "Pipeline Rejection Reason",
            "AI Verdict",
            "Model Used",
            "AI: Product Type/Use",
            "AI: Category Path Assessment",
            "AI: Rejection Reason Assessment",
            "AI: Correct Category Guess",
            "AI: Reason",
        ]

        st.subheader("Results")

        # quick summary counts
        counts = final["AI Verdict"].value_counts()
        cols = st.columns(len(counts) if len(counts) > 0 else 1)
        for c, (label, n) in zip(cols, counts.items()):
            c.metric(label, n)

        wrong_only = st.checkbox("Show only mismatches / errors", value=False)
        display_df = final
        if wrong_only:
            display_df = final[
                final["AI Verdict"].str.contains("❌|⚠️", na=False)
            ]

        st.dataframe(display_df, use_container_width=True, height=600)

        csv = final.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download results as CSV",
            data=csv,
            file_name="category_qc_results.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()