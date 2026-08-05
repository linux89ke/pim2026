import os
import socket
from urllib.parse import urlparse

import streamlit as st
import pandas as pd
from targeted_audit_filters import (
    evaluate_all_checks,
    diagnose_columns,
    verify_category_rejections_with_ai,
    _context_columns,
    CHECK_ORDER,
    CHECK_LABELS,
)
from report_builder import build_docx_report
from design_tokens import SEVERITY

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

# ── Acting on an audit result ──────────────────────────────────────────────
#
# The verdict already says what the correct action is, so the UI offers that
# action rather than a pair of neutral Approve/Reject buttons: a False
# Approval shipped and needs rejecting, a False Rejection penalised a seller
# and needs approving. "Needs Manual Review" is the only verdict where the
# reviewer genuinely chooses, and "AI Error" means the check never ran, so
# there is nothing to act on.
#
#   verdict -> (status, button label, help)
_VERDICT_ACTION = {
    "False Approval": (
        "Rejected", "Reject these",
        "The pipeline approved these but shouldn't have. Rejects them now.",
    ),
    "False Rejection": (
        "Approved", "Approve these",
        "The pipeline rejected these wrongly. Restores them to approved.",
    ),
    "Duplicate": (
        "Rejected", "Reject as duplicate",
        "Rejects these as duplicate listings.",
    ),
}
# Both actions offered; neither is the obvious default.
_VERDICT_BOTH = {"Needs Manual Review"}
# The check could not complete, so its verdict is not evidence of anything.
_VERDICT_NO_ACTION = {"AI Error"}

# Rejections carry a reason code so they export the same way a normal
# rejection does, rather than landing in the report as an unlabelled manual
# action.
_CHECK_REASON_CODE = {
    "duplicate":     "REJECT_DUPLICATE",
    "category":      "REJECT_WRONG_CAT",
    "color":         "REJECT_COLOR",
    "warranty":      "REJECT_WARRANTY",
    "variation":     "REJECT_VARIATION",
    "fda":           "REJECT_FDA",
    "title_weight":  "REJECT_WEIGHT_VOL",
    "title_english": "REJECT_TITLE_LANG",
    "name_brand":    "REJECT_BRAND_REPEAT",
    "image_quality": "REJECT_POOR_IMAGE",
    "brand_image":   "REJECT_BRAND_MISMATCH",
}

_ACTIONED_KEY = "_audit_actioned"


def _live_status() -> dict:
    """{sid: (status, flag, is_manual)} straight from the live report.

    The audit's Verdict says what the pipeline *should* have done. It says
    nothing about what the product's status is right now — so a row could read
    "False Approval" while the product had already been rejected for something
    else entirely, and the UI would still offer to reject it again.

    is_manual carries whether a human already decided it in the flag expanders
    or the visual grid, which is a different thing from the pipeline's own
    automatic verdict and worth showing before someone re-decides it.
    """
    fr = st.session_state.get("final_report", pd.DataFrame())
    if not isinstance(fr, pd.DataFrame) or fr.empty or "ProductSetSid" not in fr.columns:
        return {}
    n = len(fr)
    sids = fr["ProductSetSid"].astype(str).str.strip()
    statuses = fr["Status"].astype(str) if "Status" in fr.columns else pd.Series([""] * n)
    flags = fr["FLAG"].astype(str) if "FLAG" in fr.columns else pd.Series([""] * n)
    manual = (
        fr["Is_Manual"].fillna(False).astype(bool) if "Is_Manual" in fr.columns
        else pd.Series([False] * n)
    )
    return {
        s: (st_, fl, bool(mn))
        for s, st_, fl, mn in zip(sids, statuses, flags, manual)
    }


def _actioned() -> dict:
    """{sid: "Approved"|"Rejected"} for everything actioned from this modal.

    The audit results are computed from `all_data_map`, not from final_report,
    so applying a status does not change a row's Verdict — re-running the whole
    audit just to grey out one row would cost seconds. This ledger records what
    has been actioned so rows can be marked and excluded from a second pass.
    """
    return st.session_state.setdefault(_ACTIONED_KEY, {})


def _apply_from_audit(sids, status: str, check_key: str, reason_type: str, verdict: str) -> int:
    """Push an audit decision into the real report.

    Imported here rather than at module scope: ui_components imports
    targeted_audit_modal from this module, so a top-level import of
    ui_components would be circular.
    """
    from ui_components import apply_status_change, checkpoint_final_report

    sids = [s for s in sids if s]
    if not sids:
        return 0

    # apply_status_change() expands the SID set to every product sharing an
    # image when the FLAG contains "image"/"blurry"/"poor"/"mismatch" etc.
    # That cascade is right for a reviewer rejecting a bad photo, and wrong
    # here — the reviewer picked specific rows. Keeping those words out of
    # FLAG and putting the detail in the comment avoids acting on products
    # that were never on screen.
    flag = f"Audit correction — {status.lower()}"
    comment = f"{CHECK_LABELS.get(check_key, check_key)} · {reason_type} · audited as {verdict}"
    reason = _CHECK_REASON_CODE.get(check_key, "") if status == "Rejected" else ""

    # Capture what each product was BEFORE the change, so the row can say it
    # was overwritten rather than just showing the new status. A reviewer
    # needs to see that this approval reversed an existing rejection —
    # otherwise "Approved" looks the same whether it confirmed the pipeline
    # or overruled it. Rows are never dropped from the table; only labelled.
    _before = _live_status()

    n = apply_status_change(
        sids, status=status, flag=flag, reason=reason, comment=comment, is_manual=True,
    )
    if n:
        ledger = _actioned()
        for sid in sids:
            _sid = str(sid).strip()
            _prev = (_before.get(_sid) or ("", ""))[0]
            if _prev and _prev != status:
                ledger[_sid] = f"Overwritten {status.lower()}"
            else:
                ledger[_sid] = status
        checkpoint_final_report()
    return n


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


def _display_columns(df: pd.DataFrame, check_key: str = None) -> list:
    universal = ["ProductSetSid", "Product Name", "Category"]
    tail = [c for c in ("Detail",) if c in df.columns]
    reserved = universal + tail + ["Check", "Reason Type", "Verdict"]

    # A check's own context columns always show, empty or not.
    #
    # The emptiness filter below exists because all checks share one combined
    # DataFrame, so an unrelated check's column (FDA on a Color-check row)
    # would otherwise appear blank. But it also hid the column the check is
    # *about*: "Color Missing" means COLOR is empty by definition, so the
    # evidence for the finding was filtered out precisely when it mattered,
    # and there was no way to tell a genuinely blank colour from one the
    # audit had simply not shown.
    #
    # Derived by calling _context_columns with an empty record, so it tracks
    # that function automatically rather than duplicating its column list.
    always = set()
    if check_key:
        try:
            always = set(_context_columns(check_key, {}).keys())
        except Exception:
            always = set()

    middle = [
        c for c in df.columns
        if c not in reserved and (c in always or _has_content(df[c]))
    ]
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


def _render_table(df: pd.DataFrame, key: str = None, selectable: bool = False,
                  check_key: str = None):
    """Render one verdict's rows. Returns the SIDs the reviewer ticked.

    Selection is positional, so the frame handed to st.dataframe and the frame
    indexed afterwards have to be the same object — hence building `view` once
    and slicing it with .iloc rather than re-deriving the columns.
    """
    cols = _display_columns(df, check_key)
    view = df[cols].copy()

    # What the product is right now, next to what the audit thinks it should
    # be — without it there is no way to see that a "False Approval" row has
    # already been rejected for an unrelated reason.
    live = _live_status()
    if live and "ProductSetSid" in view.columns:
        _sid = view["ProductSetSid"].astype(str).str.strip()
        view.insert(1, "Now", _sid.map(lambda s: (live.get(s) or ("", "", False))[0]))
        # Whether a human already decided this one, so a reviewer does not
        # silently overturn a colleague's call without noticing.
        _dec = _sid.map(
            lambda s: "Reviewer" if (live.get(s) or ("", "", False))[2] else "Automatic"
        )
        view.insert(2, "Decided by", _dec)
        _why = _sid.map(lambda s: (live.get(s) or ("", "", False))[1])
        if _why.astype(str).str.strip().ne("").any():
            view.insert(3, "Rejected for", _why)

    # Mark anything already actioned from this modal so a reviewer does not
    # apply the same decision twice.
    ledger = _actioned()
    if ledger and "ProductSetSid" in view.columns:
        applied = view["ProductSetSid"].astype(str).map(ledger).fillna("")
        if applied.ne("").any():
            view.insert(0, "Applied", applied)

    cfg = _column_config(df)
    if "Applied" in view.columns:
        cfg["Applied"] = st.column_config.TextColumn("Applied", width="small")
    if "Now" in view.columns:
        cfg["Now"] = st.column_config.TextColumn(
            "Now", width="small", help="The product's current status in the report",
        )
    if "Decided by" in view.columns:
        cfg["Decided by"] = st.column_config.TextColumn(
            "Decided by", width="small",
            help="Reviewer = someone already actioned this in the flag list or "
                 "the visual grid. Automatic = the pipeline's own verdict.",
        )
    if "Rejected for" in view.columns:
        cfg["Rejected for"] = st.column_config.TextColumn(
            "Rejected for", width="medium",
            help="The flag it currently carries, which may be unrelated to this check",
        )

    # Shade the Now cell — green for approved, red for rejected — so a
    # reviewer can see a row's current state without reading it. Only that
    # one cell: `subset=["Now"]` keeps the shading off the rest of the row,
    # where it would compete with the selection highlight.
    #
    # Colours come from the severity tokens rather than raw hex, and the ink
    # is the darker paired value so both stay above 4.5:1 on their wash.
    _render_obj = view
    if "Now" in view.columns:
        _APPROVED = SEVERITY["resolved"]
        _REJECTED = SEVERITY["blocker"]

        def _shade_now(v):
            s = str(v).strip().lower()
            if s == "approved":
                return f"background-color: {_APPROVED['wash']}; color: {_APPROVED['color']};"
            if s == "rejected":
                return f"background-color: {_REJECTED['wash']}; color: {_REJECTED['color']};"
            return ""

        try:
            _render_obj = view.style.map(_shade_now, subset=["Now"])
        except Exception:
            _render_obj = view      # older pandas: fall back to unstyled

    if not selectable:
        st.dataframe(_render_obj, width='stretch', hide_index=True, column_config=cfg)
        return []

    event = st.dataframe(
        _render_obj, width='stretch', hide_index=True, column_config=cfg,
        selection_mode="multi-row", on_select="rerun", key=key,
    )
    rows = event.selection.rows if event and event.selection else []
    if not rows or "ProductSetSid" not in view.columns:
        return []
    return view.iloc[rows]["ProductSetSid"].astype(str).tolist()


def _render_verdict_actions(sub, selected, key_base, check_key, reason_type, verdict):
    """The action row under one verdict's table."""
    if verdict in _VERDICT_NO_ACTION:
        st.caption(
            "These checks never completed, so there is nothing to act on. "
            "Re-run the audit once the AI check is reachable."
        )
        return

    ledger = _actioned()
    all_sids = (
        sub["ProductSetSid"].astype(str).str.strip().tolist()
        if "ProductSetSid" in sub.columns else []
    )
    pending = [s for s in all_sids if s not in ledger]
    targets = selected or pending
    scope = "selected" if selected else "all pending"

    if not targets:
        st.caption(f"All {len(all_sids)} actioned.")
        return

    # Applying a status a product already has does nothing useful — and for a
    # rejection it is actively harmful, because it overwrites the existing
    # FLAG and destroys the original reason. So each action only ever targets
    # the products it would actually change, whichever direction it goes:
    # already-approved are dropped from an Approve, already-rejected from a
    # Reject. This covers decisions made anywhere, since final_report is what
    # the flag expanders and the visual grid both write to.
    live = _live_status()

    def _cur(sid):
        return (live.get(sid) or ("", "", False))

    def _changeable(status):
        return [s for s in targets if _cur(s)[0] != status]

    def _describe(sids, status):
        """'12 already rejected (8 decided by a reviewer) — Restricted brands'"""
        if not sids:
            return ""
        _manual = sum(1 for s in sids if _cur(s)[2])
        bits = [f"{len(sids)} already {status.lower()}"]
        if _manual:
            bits.append(
                f"{_manual} decided by a reviewer in the flag list or the grid"
            )
        _flags = sorted({_cur(s)[1].strip() for s in sids if _cur(s)[1].strip()})
        if _flags and status == "Rejected":
            bits.append("flagged " + ", ".join(_flags[:3]))
        return " · ".join(bits)

    # The action this verdict actually recommends.
    _primary = None
    if verdict in _VERDICT_ACTION:
        _primary = _VERDICT_ACTION[verdict][0]

    if _primary and not _changeable(_primary):
        _same = [s for s in targets if _cur(s)[0] == _primary]
        _opposite = "Approved" if _primary == "Rejected" else "Rejected"
        st.info(
            f"Nothing to do — {_describe(_same, _primary)}. "
            f"The audit's suggestion has already been applied to all "
            f"{len(targets)} of these.",
            icon=":material/task_alt:",
        )
        # Still allow the reverse, in case the audit shows the existing
        # decision was the wrong one.
        _rev = _changeable(_opposite)
        if _rev and st.button(
            f"{'Approve' if _opposite == 'Approved' else 'Reject'} instead ({len(_rev)})",
            key=f"{key_base}_{_opposite}_reverse", type="secondary", width="stretch",
            help="Use this only if the existing decision was wrong.",
        ):
            n = _apply_from_audit(_rev, _opposite, check_key, reason_type, verdict)
            st.session_state["_audit_flash"] = (
                f"{_opposite} {n:,} product{'s' if n != 1 else ''}."
                if n else "Nothing changed — those products are no longer in the report."
            )
        return

    for _st_name in ("Approved", "Rejected"):
        _same = [s for s in targets if _cur(s)[0] == _st_name]
        if _same and len(_same) < len(targets):
            st.caption(
                f"{_describe(_same, _st_name)}. "
                f"The {'approve' if _st_name == 'Approved' else 'reject'} action "
                f"below skips them and applies to the other "
                f"{len(targets) - len(_same)}."
            )

    def _button(status, label, help_text, col, kind):
        _t = _changeable(status)
        if not _t:
            return
        if col.button(
            f"{label} ({len(_t)} {scope})",
            key=f"{key_base}_{status}", type=kind, width="stretch", help=help_text,
        ):
            n = _apply_from_audit(_t, status, check_key, reason_type, verdict)
            st.session_state["_audit_flash"] = (
                f"{status} {n:,} product{'s' if n != 1 else ''} "
                f"from {CHECK_LABELS.get(check_key, check_key)}."
                if n else
                "Nothing changed — those products are no longer in the report."
            )

    if verdict in _VERDICT_BOTH:
        c1, c2 = st.columns(2)
        _button("Approved", "Approve", "Keep these live.", c1, "secondary")
        _button("Rejected", "Reject", "Take these down.", c2, "secondary")
    elif verdict in _VERDICT_ACTION:
        status, label, help_text = _VERDICT_ACTION[verdict]
        c1, _c2 = st.columns([1, 1])
        _button(status, label, help_text, c1, "primary")


def _on_audit_dismissed():
    """Clear the open-flag when the dialog is closed by X, Esc or backdrop.

    Without this the modal reopened by itself. show_targeted_audit_modal is
    what render_manual_review_buttons checks to decide whether to call this
    dialog, and only the in-dialog "Close" button ever cleared it — dismissing
    any other way left it True, so the very next rerun (any click anywhere in
    the app) put the dialog straight back on screen.

    on_dismiss runs the callback before the rest of the script, so the flag is
    already False by the time the reopen check happens.
    """
    st.session_state.show_targeted_audit_modal = False


@st.dialog(
    "Targeted Audit", width="large", dismissible=True, on_dismiss=_on_audit_dismissed
)
def targeted_audit_modal(support_files):
    _inject_css()

    # The dialog's own title bar already reads "Targeted Audit"; an H3 saying it
    # again cost ~45px of a screen where the findings were below the fold.

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
            #
            # The AI check *discovers* findings, it does not action them, so it
            # is allowed to update the snapshot — unlike an approve/reject,
            # which must never touch it.
            st.session_state.pop("_audit_docx_bytes", None)
            st.session_state.pop("_audit_docx_error", None)
            with st.spinner("Rebuilding Word report with AI category results..."):
                try:
                    st.session_state["_audit_docx_bytes"] = build_docx_report(
                        fr, st.session_state["_audit_results"], country_label=country_code,
                    )
                except Exception as e:
                    st.session_state["_audit_docx_error"] = f"{type(e).__name__}: {e}"
            st.session_state["_audit_snapshot"] = {
                "results": st.session_state["_audit_results"].copy(),
                "at": pd.Timestamp.now(),
                "replaced": bool(
                    (st.session_state.get("_audit_snapshot") or {}).get("replaced")
                ),
                "with_ai": True,
            }

    if run_clicked:
        st.session_state.pop("_category_ai_results", None)
        with st.spinner("Re-validating every check against the file's own rules..."):
            st.session_state["_audit_results"] = evaluate_all_checks(data, country_code)

        # The first completed run is kept as the audit of record.
        #
        # Re-running after approving or rejecting things re-derives the checks
        # against a report that now carries those decisions, so the second run
        # legitimately finds fewer errors — and the Word report and CSV would
        # quietly lose the original findings. Both downloads therefore serve
        # this snapshot, not the live results, and it is only replaced when
        # explicitly asked for.
        _snap_exists = bool(st.session_state.get("_audit_snapshot"))
        _replace = st.session_state.pop("_audit_replace_snapshot", False)

        if not _snap_exists or _replace:
            st.session_state.pop("_audit_docx_bytes", None)
            st.session_state.pop("_audit_docx_error", None)
            # Built once here, not on every rerun — with 1000+ rows it takes a
            # few seconds, and rebuilding on every keystroke in the search box
            # made it look broken.
            with st.spinner("Building Word report... this can take a moment for large files."):
                try:
                    st.session_state["_audit_docx_bytes"] = build_docx_report(
                        fr, st.session_state["_audit_results"], country_label=country_code,
                    )
                except Exception as e:
                    st.session_state["_audit_docx_error"] = f"{type(e).__name__}: {e}"
            st.session_state["_audit_snapshot"] = {
                "results": st.session_state["_audit_results"].copy(),
                "at": pd.Timestamp.now(),
                "replaced": bool(_replace),
            }

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
        counts = results["Verdict"].value_counts() if not results.empty else pd.Series(dtype=int)

        # Six metric cards. I replaced these with a pill row to buy vertical
        # space and it was the wrong trade — this is the summary people look
        # for, and shrinking it made it read as missing rather than compact.
        # The space came from the duplicate heading and the merged caption
        # instead, which cost nothing to look at.
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("❌ False Approvals", int(counts.get("False Approval", 0)))
        s2.metric("⚠️ False Rejections", int(counts.get("False Rejection", 0)))
        s3.metric("✅ True Rejections", int(counts.get("True Rejection", 0)))
        s4.metric("👁️ Needs Review", int(counts.get("Needs Manual Review", 0)))
        s5.metric("🚨 AI Errors", int(counts.get("AI Error", 0)))
        s6.metric("📑 Duplicates", int(counts.get("Duplicate", 0)))

        # Result of the last approve/reject, shown here rather than as a toast:
        # acting on a row reruns the dialog, and a toast fired during that run
        # is easy to miss behind the modal.
        _flash = st.session_state.pop("_audit_flash", None)
        if _flash:
            st.success(_flash, icon=":material/task_alt:")

        _ledger_now = _actioned()
        if _ledger_now:
            _n_app = sum(1 for v in _ledger_now.values() if v.endswith("pproved"))
            _n_rej = sum(1 for v in _ledger_now.values() if v.endswith("ejected"))
            _n_over = sum(1 for v in _ledger_now.values() if v.startswith("Overwritten"))
            _bar, _reset = st.columns([4, 1], vertical_alignment="center")
            with _bar:
                st.caption(
                    f"Applied from this audit: **{_n_app:,} approved**, "
                    f"**{_n_rej:,} rejected**"
                    + (f", of which **{_n_over:,} overwrote** an existing decision"
                       if _n_over else "")
                    + ". These are already in the report and the Excel exports."
                )
            with _reset:
                if st.button("Clear marks", key="btn_clear_audit_marks", width="stretch",
                             help="Clears the actioned markers here. Does not undo the "
                                  "status changes — those live in the report."):
                    st.session_state.pop(_ACTIONED_KEY, None)
                    st.rerun()

        # Title and caption merged onto one line — the caption was its own
        # block under a heading that said much the same thing.
        st.markdown(
            '<div class="audit-section-title">Issues by check'
            '<span style="font-weight:400;font-size:0.8rem;opacity:.65;"> — only items '
            'needing a decision or a fix; confirmed rejections are counted above</span></div>',
            unsafe_allow_html=True,
        )
        # A fixed-height scrolling container was tried here and made things
        # worse: it reserves its full height whether or not there is anything
        # in it, so a run with two findings still showed a tall, mostly empty
        # box, and a run with many put them in a 340px window to scroll
        # through. Letting the sections size to their content is better on
        # both counts — the space saved above them is what actually helped.
        any_visible = False
        # Every check, with its count — including the ones that found nothing.
        #
        # Only checks with findings get an expander, so a check that ran and
        # came back clean is indistinguishable from one that never ran at all.
        # On a real batch that meant no way to tell that the DVD and Small
        # Appliances rules had genuinely found nothing, versus being broken.
        _counts = (
            results_issues["Check"].value_counts().to_dict()
            if not results_issues.empty else {}
        )
        with st.expander(
            f"Checks run — {sum(1 for k in CHECK_ORDER if _counts.get(k))} of "
            f"{len(CHECK_ORDER)} found something",
            expanded=False, icon=":material/checklist:",
        ):
            st.dataframe(
                pd.DataFrame([
                    {"Check": CHECK_LABELS[k],
                     "Findings": int(_counts.get(k, 0)),
                     "Result": "needs attention" if _counts.get(k) else "clean"}
                    for k in CHECK_ORDER
                ]),
                hide_index=True, width="stretch",
            )

        # Fewest findings first. The order used to follow CHECK_ORDER, which put
        # Duplicates and Category — over two thousand rows between them on a
        # real batch — ahead of an eight-row Color finding. The short lists are
        # the ones a person can actually work through, and they were the ones
        # buried.
        _ordered = sorted(
            CHECK_ORDER,
            key=lambda k: (_counts.get(k, 0) or 10**9),
        )
        for check_key in _ordered:
            label = CHECK_LABELS[check_key]
            icon = _CHECK_ICONS.get(check_key, "🔹")

            check_slice = results_issues[results_issues["Check"] == check_key] if not results_issues.empty else pd.DataFrame()
            if check_slice.empty:
                continue
            any_visible = True

            _ledger = _actioned()
            _done = (
                int(check_slice["ProductSetSid"].astype(str).isin(_ledger).sum())
                if "ProductSetSid" in check_slice.columns else 0
            )
            _exp_label = f"{icon}  **{label}**   —   {len(check_slice)} item(s) flagged"
            if _done:
                _exp_label += f"   ·   {_done} actioned"

            with st.expander(_exp_label, expanded=False):
                for _ri, reason_type in enumerate(check_slice["Reason Type"].unique()):
                    reason_slice = check_slice[check_slice["Reason Type"] == reason_type]
                    st.markdown(f'<div class="audit-reason-label">{reason_type} '
                                f'({len(reason_slice)})</div>', unsafe_allow_html=True)

                    for verdict in _VERDICT_ORDER:
                        sub = reason_slice[reason_slice["Verdict"] == verdict]
                        if sub.empty:
                            continue
                        fg, bg, emoji = _VERDICT_STYLE[verdict]
                        st.markdown(_pill(f"{emoji} {verdict} ({len(sub)})", fg, bg), unsafe_allow_html=True)

                        # Positional selection needs a key that is stable across
                        # reruns and unique per table: reason types are free text,
                        # so index them rather than using the string itself.
                        _key_base = f"audit_{check_key}_{_ri}_{verdict.replace(' ', '_')}"
                        _actionable = verdict not in _VERDICT_NO_ACTION
                        _selected = _render_table(
                            sub, key=f"{_key_base}_tbl", selectable=_actionable,
                            check_key=check_key,
                        )
                        if _actionable:
                            _render_verdict_actions(
                                sub, _selected, _key_base, check_key, reason_type, verdict
                            )

        if not any_visible and search_term.strip():
            st.info(f"No results match '{search_term}'.")

        # ── Export ───────────────────────────────────────────────────────────
        # Both downloads serve the snapshot from the first completed run, not
        # the live results — see the comment where _audit_snapshot is set.
        _snap = st.session_state.get("_audit_snapshot") or {}
        _snap_results = _snap.get("results")
        _export_df = _snap_results if isinstance(_snap_results, pd.DataFrame) and not _snap_results.empty else results

        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            if not _export_df.empty:
                st.download_button(
                    "⬇️ Download CSV",
                    data=_export_df.to_csv(index=False).encode("utf-8"),
                    file_name="targeted_audit_results.csv",
                    mime="text/csv",
                    width='stretch',
                    help="The audit as first run, matching the Word report.",
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

        # Two outputs, two jobs — and the difference is easy to lose track of
        # once you start approving and rejecting from this screen:
        #
        #   Word  = the audit record. Built once, when Run Full Audit ran, and
        #           never rebuilt by an approve/reject here. It keeps the
        #           errors as found, which is the point of having it.
        #   Excel = the actionable output. render_exports_section reads
        #           final_report live, so it reflects every decision made here
        #           and in the flag expanders.
        if _snap.get("at") is not None:
            _c1, _c2 = st.columns([3, 1], vertical_alignment="center")
            with _c1:
                st.caption(
                    f":material/lock: **Saved audit — {_snap['at']:%d %b %Y, %H:%M}"
                    f"{' (incl. AI check)' if _snap.get('with_ai') else ''}.** "
                    "The Word report and the CSV above both come from this run "
                    "and do not change when you approve or reject. Re-running "
                    "the audit updates the tables below but leaves these two "
                    "alone, so the original findings survive. The Excel exports "
                    "on the main page read the live report, so they include "
                    "every decision made here and in the flag expanders."
                )
            with _c2:
                if st.button(
                    "Replace saved audit", key="btn_replace_audit_snapshot",
                    width="stretch",
                    help="Discards the saved findings and re-captures them on the "
                         "next Run Full Audit. The current Word report and CSV "
                         "are lost — download them first if you need them.",
                ):
                    st.session_state["_audit_replace_snapshot"] = True
                    st.info(
                        "The next Run Full Audit will replace the saved report.",
                        icon=":material/refresh:",
                    )

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