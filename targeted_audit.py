import os
import socket
from urllib.parse import urlparse

import streamlit as st
import pandas as pd
from targeted_audit_filters import (
    evaluate_all_checks,
    diagnose_columns,
    verify_category_rejections_with_ai,
    CHECK_ORDER,
    CHECK_LABELS,
)
from report_builder import build_docx_report

# Default gateway used by verify_category_rejections_with_ai(). Kept here so the
# availability probe checks the same host the call will actually use.
AI_GATEWAY_URL = "https://ai-gateway.zuma.jumia.com/v1"
_AI_KEYS_FILE = os.path.join(os.path.dirname(__file__), "keys.txt")


@st.cache_data(ttl=300, show_spinner=False)
def _gateway_reachable(base_url: str) -> bool:
    """Can we open a TCP connection to the AI gateway?

    Deliberately just a socket connect, not an API call: it needs no key, costs
    nothing, and answers the only question that matters — is there a route.
    Two-second timeout so a dead host cannot stall the modal, cached for five
    minutes so opening the modal repeatedly does not re-probe.
    """
    try:
        parsed = urlparse(base_url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((parsed.hostname, port), timeout=2):
            return True
    except Exception:
        return False


def ai_check_available(base_url: str = AI_GATEWAY_URL):
    """(available, reason_if_not) for the optional AI category check."""
    if not os.path.exists(_AI_KEYS_FILE):
        return False, "no keys.txt found next to the app"
    if not _gateway_reachable(base_url):
        host = urlparse(base_url).hostname or base_url
        return False, f"cannot reach {host} (internal network only)"
    return True, ""

_CHECK_ICONS = {
    "skip": "⏭️",
    "duplicate": "📑",
    "category": "🗂️",
    "color": "🎨",
    "warranty": "🛡️",
    "variation": "🔀",
    "fda": "💊",
    "title_weight": "⚖️",
    "title_english": "🌐",
    "name_brand": "🏷️",
    "image_quality": "🖼️",
    "image_extraction": "🔌",
    "ai_caption": "🖋️",
    "brand_image": "🔎",
}

_VERDICT_STYLE = {
    "True Rejection":      ("#16a34a", "#dcfce7", "✅"),
    "False Rejection":     ("#d97706", "#fef3c7", "⚠️"),
    "False Approval":      ("#dc2626", "#fee2e2", "❌"),
    "Needs Manual Review": ("#2563eb", "#dbeafe", "👁️"),
    "AI Error":            ("#7c3aed", "#ede9fe", "🚨"),
    "Skipped":             ("#64748b", "#f1f5f9", "⏭️"),
    "Duplicate":           ("#0891b2", "#cffafe", "📑"),
}
_VERDICT_ORDER = ["False Approval", "False Rejection", "True Rejection",
                  "Needs Manual Review", "AI Error", "Duplicate", "Skipped"]


def _inject_css():
    st.markdown("""
    <style>
      div[data-testid="stDialog"] div[data-testid="stMetric"] {
          background: linear-gradient(180deg, rgba(255,255,255,0.6), rgba(248,250,252,0.9));
          border: 1px solid rgba(0,0,0,0.06);
          border-radius: 12px;
          padding: 14px 12px 10px 12px;
          box-shadow: 0 1px 3px rgba(0,0,0,0.04);
      }
      .audit-pill {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 3px 12px;
          border-radius: 999px;
          font-size: 0.82rem;
          font-weight: 600;
          margin: 2px 6px 8px 0;
      }
      .audit-reason-label {
          font-size: 0.88rem;
          font-weight: 600;
          margin: 10px 0 4px 0;
          color: rgba(49,51,63,0.85);
      }
      .audit-section-title {
          font-size: 1.05rem;
          font-weight: 700;
          margin: 18px 0 6px 0;
          letter-spacing: 0.2px;
      }
      .audit-caption {
          color: rgba(49,51,63,0.6);
          font-size: 0.85rem;
          margin-bottom: 10px;
      }
      hr.audit-divider {
          margin: 14px 0;
          border: none;
          border-top: 1px solid rgba(0,0,0,0.08);
      }
    </style>
    """, unsafe_allow_html=True)


def _pill(text: str, fg: str, bg: str) -> str:
    return f'<span class="audit-pill" style="color:{fg}; background:{bg};">{text}</span>'


def _has_content(series: pd.Series) -> bool:
    return series.fillna("").astype(str).str.strip().replace({"nan": "", "None": ""}).ne("").any()


def _display_columns(df: pd.DataFrame) -> list:
    universal = ["ProductSetSid", "Product Name", "Category"]
    tail = [c for c in ("Detail",) if c in df.columns]
    reserved = universal + tail + ["Check", "Reason Type", "Verdict"]
    # Only keep check-specific columns that actually have data in *this*
    # slice — since all checks share one combined DataFrame, an unrelated
    # check's column (e.g. FDA on a Color-check row) would otherwise show
    # up entirely empty.
    middle = [c for c in df.columns if c not in reserved and _has_content(df[c])]
    return [c for c in universal if c in df.columns] + middle + tail


def _column_config(df: pd.DataFrame) -> dict:
    cfg = {}
    if "Image" in df.columns:
        try:
            cfg["Image"] = st.column_config.ImageColumn("Image", width="small")
        except Exception:
            cfg["Image"] = st.column_config.LinkColumn("Image")
    if "ProductSetSid" in df.columns:
        cfg["ProductSetSid"] = st.column_config.TextColumn("SID", width="small")
    if "Product Name" in df.columns:
        cfg["Product Name"] = st.column_config.TextColumn("Product Name", width="medium")
    if "Initial Category Path" in df.columns:
        cfg["Initial Category Path"] = st.column_config.TextColumn("Initial Category Path", width="large")
    if "Category" in df.columns:
        cfg["Category"] = st.column_config.TextColumn("Category", width="large")
    if "Detail" in df.columns:
        cfg["Detail"] = st.column_config.TextColumn("Detail", width="large")
    return cfg


def _render_table(df: pd.DataFrame):
    cols = _display_columns(df)
    st.dataframe(df[cols], width='stretch', hide_index=True, column_config=_column_config(df))


@st.dialog("Targeted Audit", width="large", dismissible=True)
def targeted_audit_modal(support_files):
    _inject_css()

    st.markdown("### :material/fact_check: Targeted Audit")

    fr = st.session_state.get("final_report", pd.DataFrame())
    data = st.session_state.get("all_data_map", pd.DataFrame())
    country = st.session_state.get("selected_country", "Egypt")
    country_code = {"Kenya": "KE", "Uganda": "UG", "Nigeria": "NG", "Ghana": "GH", "Morocco": "MA", "Egypt": "EG", "Senegal": "SN", "Ivory Coast": "CI"}.get(country, "EG")

    # If the user uploaded a ZIP file (which contains the QC results), restrict the
    # targeted audit to ONLY those products. This prevents mixing in non-QC'd 
    # products from an additionally uploaded CSV.
    qc_zip = st.session_state.get("zip_qc_results", pd.DataFrame())
    if not qc_zip.empty and not data.empty:
        _sid_col_qc = next((c for c in ("PRODUCT_SET_SID", "ProductSetSid", "Product Set SID", "cod_productset_sid", "SID") if c in qc_zip.columns), None)
        if _sid_col_qc and "PRODUCT_SET_SID" in data.columns:
            zip_sids = set(qc_zip[_sid_col_qc].astype(str).str.strip().unique())
            data = data[data["PRODUCT_SET_SID"].astype(str).str.strip().isin(zip_sids)].copy()
            if not fr.empty and "ProductSetSid" in fr.columns:
                fr = fr[fr["ProductSetSid"].astype(str).str.strip().isin(zip_sids)].copy()

    if data.empty:
        st.warning("No data available to audit. Please upload and process files first.")
        if st.button("Close", key="btn_close_audit_empty"):
            st.session_state.show_targeted_audit_modal = False
            st.rerun()
        return

    run_col, search_col, clear_col = st.columns([1.6, 2, 1])
    with run_col:
        run_clicked = st.button("🔍 Run Full Audit", width='stretch', type="primary")
    with search_col:
        search_term = st.text_input(
            "Filter by product name or SID", value="", placeholder="Search results...",
            label_visibility="collapsed",
        )
    with clear_col:
        if st.button("Clear Results", width='stretch'):
            st.session_state.pop("_audit_results", None)
            st.session_state.pop("_category_ai_results", None)
            st.rerun()

    # ── Separate, optional AI category-rejection check ──────────────────────
    # Independent from Run Full Audit: only sends products the pipeline
    # REJECTED for category to an AI model, to catch false rejections. The
    # toggle controlling visibility lives at the bottom of the modal (off by
    # default); this just reacts to that flag.
    ai_category_clicked = False
    if st.session_state.get("_show_ai_category_check", False):
        ai_cat_col, ai_cat_status_col = st.columns([1.6, 3])
        with ai_cat_col:
            ai_category_clicked = st.button(
                "🤖 AI-Check Category Rejections", width='stretch',
                help="Sends every product rejected for category to an AI model to "
                     "double-check whether the rejection was actually correct. "
                     "Results are added to the audit below and the Word report.",
            )
        with ai_cat_status_col:
            st.caption("Only checks products the pipeline rejected for category. "
                       "Requires a `keys.txt` file next to the app.")

    if ai_category_clicked:
        # Always wipe the previous result so we never show stale data
        st.session_state.pop("_category_ai_results", None)
        with st.spinner("Preparing to ask AI..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            try:
                base_results = st.session_state.get("_audit_results", pd.DataFrame())
                st.session_state["_category_ai_results"] = verify_category_rejections_with_ai(
                    data, 
                    audit_df=base_results,
                    progress_bar=progress_bar,
                    status_text=status_text
                )
                progress_bar.progress(1.0)
                status_text.markdown("**AI Category Verification** — Done ✅")
                if st.session_state["_category_ai_results"].empty:
                    st.warning("No results — either there were no category-rejected products, "
                               "or no `keys.txt` was found next to the app.")
            except Exception as e:
                st.error(f"AI category check failed: {type(e).__name__}: {e}")
                st.session_state["_category_ai_results"] = pd.DataFrame()

        # Merge into the main audit results (if a full audit has already been
        # run) so this shows up in the same tables/report as everything else.
        base_results = st.session_state.get("_audit_results", pd.DataFrame())
        cat_ai_results = st.session_state.get("_category_ai_results", pd.DataFrame())
        if not cat_ai_results.empty:
            if base_results.empty:
                st.session_state["_audit_results"] = cat_ai_results
            else:
                st.session_state["_audit_results"] = pd.concat(
                    [base_results, cat_ai_results], ignore_index=True
                )
            # Rebuild the Word report so it reflects the merged results too.
            st.session_state.pop("_audit_docx_bytes", None)
            st.session_state.pop("_audit_docx_error", None)
            with st.spinner("Rebuilding Word report with AI category results..."):
                try:
                    st.session_state["_audit_docx_bytes"] = build_docx_report(
                        fr, st.session_state["_audit_results"], country_label=country_code,
                    )
                except Exception as e:
                    st.session_state["_audit_docx_error"] = f"{type(e).__name__}: {e}"

    if run_clicked:
        st.session_state.pop("_category_ai_results", None)
        with st.spinner("Re-validating every check against the file's own rules..."):
            st.session_state["_audit_results"] = evaluate_all_checks(data, country_code)

        # Build the Word report once here, not on every rerun — with 1000+
        # rows this can take a few seconds, and rebuilding it on every
        # keystroke in the search box made it look broken/missing.
        st.session_state.pop("_audit_docx_bytes", None)
        st.session_state.pop("_audit_docx_error", None)
        with st.spinner("Building Word report... this can take a moment for large files."):
            try:
                st.session_state["_audit_docx_bytes"] = build_docx_report(
                    fr, st.session_state["_audit_results"], country_label=country_code,
                )
            except Exception as e:
                st.session_state["_audit_docx_error"] = f"{type(e).__name__}: {e}"

    results = st.session_state.get("_audit_results", pd.DataFrame())

    def _filter(df):
        if df.empty or not search_term.strip():
            return df
        term = search_term.strip().lower()
        mask = pd.Series(False, index=df.index)
        for col in ("ProductSetSid", "Product Name", "NAME"):
            if col in df.columns:
                mask = mask | df[col].astype(str).str.lower().str.contains(term, na=False)
        return df[mask]

    results_f = _filter(results)

    # Only these verdicts represent something that actually needs attention.
    # "True Rejection" (system was right) and "Skipped" (correctly excluded
    # before QC) are the pipeline working as intended, not issues — they're
    # still counted in the metrics strip for context, just not listed below.
    _ISSUE_VERDICTS = {"False Approval", "False Rejection", "Needs Manual Review", "AI Error", "Duplicate"}
    results_issues = results_f[results_f["Verdict"].isin(_ISSUE_VERDICTS)] if not results_f.empty else results_f

    has_results = not results.empty

    if has_results:
        st.markdown('<hr class="audit-divider">', unsafe_allow_html=True)

        counts = results["Verdict"].value_counts() if not results.empty else pd.Series(dtype=int)
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("❌ False Approvals", int(counts.get("False Approval", 0)))
        s2.metric("⚠️ False Rejections", int(counts.get("False Rejection", 0)))
        s3.metric("✅ True Rejections", int(counts.get("True Rejection", 0)))
        s4.metric("👁️ Needs Review", int(counts.get("Needs Manual Review", 0)))
        s5.metric("🚨 AI Errors", int(counts.get("AI Error", 0)))
        s6.metric("📑 Duplicates", int(counts.get("Duplicate", 0)))

        st.markdown('<div class="audit-section-title">Issues by check</div>', unsafe_allow_html=True)
        st.caption("Correctly confirmed rejections and normal pre-QC exclusions are counted above "
                   "but not listed here — only items that need a decision or a fix are shown.")

        any_visible = False
        for check_key in CHECK_ORDER:
            label = CHECK_LABELS[check_key]
            icon = _CHECK_ICONS.get(check_key, "🔹")

            check_slice = results_issues[results_issues["Check"] == check_key] if not results_issues.empty else pd.DataFrame()
            if check_slice.empty:
                continue
            any_visible = True

            with st.expander(f"{icon}  **{label}**   —   {len(check_slice)} item(s) flagged", expanded=False):
                for reason_type in check_slice["Reason Type"].unique():
                    reason_slice = check_slice[check_slice["Reason Type"] == reason_type]
                    st.markdown(f'<div class="audit-reason-label">{reason_type} '
                                f'({len(reason_slice)})</div>', unsafe_allow_html=True)

                    for verdict in _VERDICT_ORDER:
                        sub = reason_slice[reason_slice["Verdict"] == verdict]
                        if sub.empty:
                            continue
                        fg, bg, emoji = _VERDICT_STYLE[verdict]
                        st.markdown(_pill(f"{emoji} {verdict} ({len(sub)})", fg, bg), unsafe_allow_html=True)
                        _render_table(sub)

        if not any_visible and search_term.strip():
            st.info(f"No results match '{search_term}'.")

        # ── Export ───────────────────────────────────────────────────────────
        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            if not results.empty:
                st.download_button(
                    "⬇️ Download CSV",
                    data=results.to_csv(index=False).encode("utf-8"),
                    file_name="targeted_audit_results.csv",
                    mime="text/csv",
                    width='stretch',
                )
        with dl_col2:
            docx_bytes = st.session_state.get("_audit_docx_bytes")
            docx_error = st.session_state.get("_audit_docx_error")
            if docx_bytes:
                st.download_button(
                    "📄 Download Report (Word)",
                    data=docx_bytes,
                    file_name=f"QC_{country_code}_Audit_Report_{pd.Timestamp.today():%d_%b_%Y}.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    width='stretch',
                )
            elif docx_error:
                st.error(f"Couldn't build the Word report: {docx_error}")
            else:
                st.caption("Word report will be ready after you run the audit.")

    elif run_clicked:
        st.success("✅ No issues found across any check.")

    st.markdown('<hr class="audit-divider">', unsafe_allow_html=True)
    st.session_state.setdefault("_show_ai_category_check", False)

    # The AI check needs two things that are often absent: API keys on disk,
    # and a route to the gateway. The gateway is an internal host (resolves to
    # a private 10.x address), so it is unreachable from anywhere outside the
    # corporate network — Streamlit Cloud included. Offering the toggle there
    # just buys the reviewer several minutes of request timeouts followed by
    # "AI Error" on every row, so hide it and say why.
    _ai_ready, _ai_blocked_reason = ai_check_available()
    if _ai_ready:
        st.toggle(
            "🤖 Enable AI Category Rejection Check",
            key="_show_ai_category_check",
            help="Reveals a button above to send category-rejected products to an "
                 "AI model for a second opinion. Off by default since it costs "
                 "real API calls and time.",
        )
    else:
        # Force it off, so a toggle left on before losing connectivity cannot
        # leave the run button visible.
        st.session_state["_show_ai_category_check"] = False
        st.caption(f":material/cloud_off: AI Category Check unavailable — {_ai_blocked_reason}")

    st.markdown('<hr class="audit-divider">', unsafe_allow_html=True)
    if st.button("Close", key="btn_close_audit_modal", type="secondary", width='stretch'):
        st.session_state.show_targeted_audit_modal = False
        st.rerun()