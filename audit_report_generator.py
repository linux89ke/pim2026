"""
audit_report_generator.py
Generates a branded DOCX QC Comprehensive Audit Report that matches the
Jumia house-style (navy header, blue section banners, D9E1F2 table headers).
"""

from __future__ import annotations

import io
from collections import defaultdict
from datetime import datetime
from typing import Optional

import pandas as pd

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt, RGBColor, Inches

_NAVY    = "1F4E79"
_BLUE    = "2E75B6"
_TBL_HDR = "D9E1F2"
_WHITE   = "FFFFFF"
_TEXT_WHT = RGBColor(0xFF, 0xFF, 0xFF)
_TEXT_NAV = RGBColor(0x1F, 0x4E, 0x79)


def _set_para_shading(para, fill_hex: str) -> None:
    ppr = para._element.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill_hex.upper())
    ppr.append(shd)


def _set_cell_shading(cell, fill_hex: str) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill_hex.upper())
    tc_pr.append(shd)


def _set_col_widths(table, widths_inches):
    for i, col in enumerate(table.columns):
        if i < len(widths_inches):
            for cell in col.cells:
                cell.width = Inches(widths_inches[i])


def _add_title(doc, text):
    p = doc.add_paragraph()
    _set_para_shading(p, _NAVY)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run(text)
    run.bold = True
    run.font.size = Pt(18)
    run.font.color.rgb = _TEXT_WHT


def _add_subtitle(doc, text):
    p = doc.add_paragraph()
    _set_para_shading(p, _NAVY)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run(text)
    run.font.size = Pt(12)
    run.font.color.rgb = RGBColor(0xA9, 0xC4, 0xE4)


def _add_section(doc, text):
    p = doc.add_paragraph()
    _set_para_shading(p, _BLUE)
    run = p.add_run(text)
    run.bold = True
    run.font.size = Pt(13)
    run.font.color.rgb = _TEXT_WHT


def _add_subsection(doc, text):
    p = doc.add_paragraph()
    run = p.add_run(text)
    run.bold = True
    run.font.size = Pt(11)
    run.font.color.rgb = _TEXT_NAV


def _add_body(doc, text):
    p = doc.add_paragraph(text)
    if p.runs:
        p.runs[0].font.size = Pt(10)


def _add_table(doc, headers, rows, col_widths=None):
    if not rows:
        _add_body(doc, "(No items in this category)")
        return
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = "Table Grid"
    hdr = table.rows[0]
    for ci, h in enumerate(headers):
        cell = hdr.cells[ci]
        _set_cell_shading(cell, _TBL_HDR)
        p = cell.paragraphs[0]
        run = p.add_run(h)
        run.bold = True
        run.font.size = Pt(9)
        run.font.color.rgb = _TEXT_NAV
    for ri, row_data in enumerate(rows):
        row = table.rows[ri + 1]
        for ci, val in enumerate(row_data):
            cell = row.cells[ci]
            _set_cell_shading(cell, _WHITE)
            p = cell.paragraphs[0]
            run = p.add_run(str(val)[:300] if val else "")
            run.font.size = Pt(9)
    if col_widths:
        _set_col_widths(table, col_widths)
    doc.add_paragraph()


def _flag_description(flag: str) -> str:
    _MAP = {
        "Wrong Category": "The AI incorrectly classified these products under the wrong category based on flawed category-matching logic.",
        "Missing COLOR": "The AI blocked these items citing a missing color field. However, color information is either present in the product name or not required for this category.",
        "Color Check": "The AI blocked these items citing a missing color field. However, color information is either present in the product name or not required for this category.",
        "Product Warranty": "These items were rejected due to missing warranty information. However, the category does not mandate a warranty or the data is present.",
        "Warranty Check": "These items were rejected due to missing warranty information. However, the category does not mandate a warranty or the data is present.",
        "Wrong Variation": "The AI threw a false 'Variation is mandatory' rejection. The master rules do not require variation for the detected category.",
        "Variation Check": "The AI threw a false 'Variation is mandatory' rejection. The master rules do not require variation for the detected category.",
        "FDA": "These items were incorrectly hit with an FDA Document rejection — false positives caused by a flawed category-FDA matrix or misclassification.",
        "Prohibited": "These items were flagged as prohibited. However, an AI review confirms they are legitimate products incorrectly matched to a prohibited-goods pattern.",
        "Poor images": "The AI rejected these items for poor image quality. A visual audit indicates the images meet the required standards.",
        "BRAND name repeated": "The AI rejected these items because the brand name appeared in the product title. In these cases the inclusion is grammatically acceptable.",
        "Missing Weight": "These items were rejected for missing weight or volume. The category does not require weight in the title for these product types.",
    }
    for key, desc in _MAP.items():
        if key.lower() in flag.lower():
            return desc
    return f"The AI generated a rejection under the '{flag}' rule. The items below were confirmed as false positives by the QC team."


def generate_audit_docx(
    final_report: pd.DataFrame,
    data: pd.DataFrame,
    override_log: list,
    dismissed_by_flag: dict,
    country: str = "Unknown",
    date_str: Optional[str] = None,
) -> bytes:
    if date_str is None:
        date_str = datetime.now().strftime("%-d %b %Y")

    doc = Document()
    for section in doc.sections:
        section.top_margin    = Inches(0.6)
        section.bottom_margin = Inches(0.6)
        section.left_margin   = Inches(0.8)
        section.right_margin  = Inches(0.8)

    # Cover
    _add_title(doc, "QC VALIDATION AUDIT REPORT")
    _add_subtitle(doc, f"Jumia {country}  \u00b7  {date_str}")
    doc.add_paragraph()

    # ── Derive issue counts ────────────────────────────────────────────────
    total     = len(final_report)
    approved  = int((final_report["Status"] == "Approved").sum()) if not final_report.empty else 0
    rejected  = int((final_report["Status"] == "Rejected").sum()) if not final_report.empty else 0
    overrides = len(override_log)
    total_filtered = sum(len(v) for v in dismissed_by_flag.values())

    # False approvals: items the AI approved but were actually wrong
    false_app_df = pd.DataFrame()
    if not final_report.empty and "FLAG" in final_report.columns:
        fa_mask = final_report["FLAG"].astype(str).str.contains(r"\[False Approval\]", na=False, case=False)
        false_app_df = final_report[fa_mask].copy()

    n_false_approvals = len(false_app_df)
    n_wrong_rejections = overrides  # items user overrode (AI rejected wrongly)
    total_issues = n_false_approvals + n_wrong_rejections

    # ── Section 1 — Executive Summary ─────────────────────────────────────
    _add_section(doc, "1. Executive Summary")
    _add_body(doc, (
        f"On {date_str}, the automated QC system processed {total:,} catalog entries "
        f"for Jumia {country}.\n\n"
        f"AI Mistakes Identified: {total_issues:,} total issues\n"
        f"  \u2022  {n_wrong_rejections:,} wrongly rejected items (AI false positives — overridden by QC team)\n"
        f"  \u2022  {n_false_approvals:,} wrongly approved items (AI false negatives — caught by rule engine)\n\n"
        f"Additionally, {total_filtered:,} rejections were confirmed as correct by the AI cross-validator.\n\n"
        f"Final outcome: {approved:,} Approved  |  {rejected:,} Rejected  |  "
        f"{overrides:,} Manual Overrides logged."
    ))
    doc.add_paragraph()

    # ── Section 2 — Wrongly Rejected Items (AI False Positives / Overrides) ──
    _add_section(doc, "2. Wrongly Rejected Items  \u2014  AI False Positives (User Overrides)")
    _add_body(doc, (
        "The following products were incorrectly rejected by the AI. "
        "A QC team member reviewed and approved them, logging an override."
    ))

    if override_log:
        by_flag: dict = defaultdict(list)
        for entry in override_log:
            by_flag[entry.get("Rejection Flag", "Unknown")].append(entry)
        for flag, entries in sorted(by_flag.items()):
            _add_subsection(doc, f"{flag}  \u2014  {len(entries)} SKUs")
            _add_body(doc, _flag_description(flag))
            rows = [
                [
                    e.get("SID", ""),
                    e.get("Product Name", ""),
                    e.get("Seller", ""),
                    e.get("AI Reason", ""),
                ]
                for e in entries
            ]
            _add_table(
                doc,
                ["SKU", "Product Name", "Seller", "AI Rejection Statement"],
                rows,
                col_widths=[1.5, 2.3, 1.2, 2.2],
            )
    else:
        _add_body(doc, "No manual overrides were logged in this session.")
    doc.add_paragraph()

    # ── Section 3 — Wrongly Approved Items (AI False Negatives) ────────────
    _add_section(doc, "3. Wrongly Approved Items  \u2014  AI False Negatives (Caught by Rule Engine)")
    _add_body(doc, (
        "The following products were incorrectly approved by the AI. "
        "The rule-based cross-validator detected they violated mandatory QC rules "
        "and flagged them for rejection."
    ))

    if not false_app_df.empty:
        sid_to_name: dict = {}
        sid_to_cat: dict  = {}
        if not data.empty and "PRODUCT_SET_SID" in data.columns:
            sid_to_name = data.set_index("PRODUCT_SET_SID")["NAME"].to_dict() if "NAME" in data.columns else {}
            sid_to_cat  = data.set_index("PRODUCT_SET_SID")["CATEGORY"].to_dict() if "CATEGORY" in data.columns else {}

        sid_col = "ProductSetSid" if "ProductSetSid" in false_app_df.columns else "PRODUCT_SET_SID"
        comment_col = "Comment" if "Comment" in false_app_df.columns else None
        flag_col = "FLAG" if "FLAG" in false_app_df.columns else None

        # Group by flag type
        by_flag_fa: dict = defaultdict(list)
        for _, row in false_app_df.iterrows():
            flag_lbl = str(row.get(flag_col, "Unknown")).replace("[False Approval]", "").strip() if flag_col else "Unknown"
            by_flag_fa[flag_lbl].append(row)

        for flag_lbl, fa_rows in sorted(by_flag_fa.items()):
            _add_subsection(doc, f"{flag_lbl}  \u2014  {len(fa_rows)} SKUs")
            rows = [
                [
                    str(r.get(sid_col, "")),
                    str(sid_to_name.get(str(r.get(sid_col, "")), r.get("NAME", "")))[:120],
                    str(sid_to_cat.get(str(r.get(sid_col, "")), r.get("CATEGORY", "")))[:80],
                    str(r.get(comment_col, ""))[:150] if comment_col else "",
                ]
                for r in fa_rows
            ]
            _add_table(
                doc,
                ["SKU", "Product Name", "Category", "Reason"],
                rows,
                col_widths=[1.5, 2.3, 1.4, 2.0],
            )
    else:
        _add_body(doc, "No wrongly approved items detected in this session.")
    doc.add_paragraph()

    # ── Section 4 — Confirmed Correct Rejections (auto-filtered true positives) ──
    _add_section(doc, "4. Confirmed Correct Rejections  \u2014  True Positives (Auto-Filtered)")
    _add_body(doc, (
        "The following products were correctly rejected by the AI and were automatically "
        "removed from the audit queue after cross-validation. They remain Rejected."
    ))

    sid_to_name2: dict   = {}
    sid_to_cat2: dict    = {}
    sid_to_comment2: dict = {}
    if not data.empty and "PRODUCT_SET_SID" in data.columns:
        data_idx = data.set_index("PRODUCT_SET_SID")
        sid_to_name2 = data_idx["NAME"].to_dict() if "NAME" in data.columns else {}
        sid_to_cat2  = data_idx["CATEGORY"].to_dict() if "CATEGORY" in data.columns else {}
    if not final_report.empty and "ProductSetSid" in final_report.columns:
        sid_to_comment2 = final_report.set_index("ProductSetSid")["Comment"].to_dict() if "Comment" in final_report.columns else {}

    any_dismissed = False
    for flag, sids in sorted(dismissed_by_flag.items()):
        if not sids:
            continue
        any_dismissed = True
        _add_subsection(doc, f"{flag}  \u2014  {len(sids)} SKUs")
        _add_body(doc, _flag_description(flag))
        rows = [
            [
                sid,
                str(sid_to_name2.get(sid, ""))[:120],
                str(sid_to_cat2.get(sid, ""))[:80],
                str(sid_to_comment2.get(sid, ""))[:150],
            ]
            for sid in sorted(sids)
        ]
        _add_table(
            doc,
            ["SKU", "Product Name", "Category", "AI Rejection Reason"],
            rows,
            col_widths=[1.5, 2.3, 1.4, 2.0],
        )
    if not any_dismissed:
        _add_body(doc, "No auto-filtered items recorded yet.")
    doc.add_paragraph()

    # Footer
    _add_section(doc, "End of Report")
    _add_body(doc, (
        f"Generated by Jumia QC Validation Tool on {datetime.now().strftime('%d %b %Y at %H:%M')}. "
        "This report reflects the state of the audit at the time of download. "
        "Manual overrides take precedence over all automated decisions."
    ))

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.getvalue()
