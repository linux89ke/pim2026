"""General QC rules — the file to edit when you meet a new broken pattern.

Each rule is one entry in RULES below. Add one, delete one, or set
``active=False`` to park it without losing the wording. Every rule becomes its
own flag: its own expander, its own line in the export, and it takes part in
the approval re-check and the audit exactly like the built-in checks.

Rules are matched against the CATEGORY the product is filed in, resolved from
the category map, so a rule written against a branch covers everything beneath
it. That matters — a seller who picks the "Shaving Gels" leaf instead of its
parent should not escape a rule written for the parent.

Keyword matching is whole-word by default. Substring matching is available but
has to be asked for, because it is the reliable way to produce nonsense: "ear"
inside search, clear, wear, earth; "air" inside hair, chair, repair. A previous
version of the counterfeit check matched "taser" inside "betaserc" for exactly
this reason.

A rule that raises is caught by the validator dispatch and logged rather than
taking the run down — but it then silently never fires, so check the rules
panel in the sidebar after editing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from data_utils import clean_category_code

logger = logging.getLogger(__name__)

WRONG_CATEGORY_REASON = "1000007 - Wrong Category"


@dataclass
class CategoryRule:
    """A product matching `keyword` is in the wrong place when filed under
    `wrong_in`. `belongs` is documentation for the reviewer — it is written
    into the comment, never used to move anything automatically.

    Leave `keyword` empty to reject *everything* filed under `wrong_in`, for a
    branch that should never receive listings at all. That is a much broader
    rule than a keyword one — it cannot be wrong about which products it hits,
    only about whether the branch really is off-limits — so the category paths
    want checking before it goes live."""

    id: str
    keyword: str
    wrong_in: List[str]                 # category paths; a prefix covers the subtree
    comment: str
    flag: str = ""                      # defaults to a title derived from id
    belongs: str = ""
    match: str = "word"                 # "word" | "substring"
    countries: Optional[List[str]] = None   # None = every market
    reason: str = WRONG_CATEGORY_REASON
    active: bool = True
    columns: List[str] = field(default_factory=lambda: ["NAME", "CATEGORY_CODE"])


# ── The rules ──────────────────────────────────────────────────────────────
RULES: List[CategoryRule] = [

    CategoryRule(
        id="iphone-under-android",
        flag="iPhone in Android Phones",
        keyword="iphone",
        wrong_in=["Phones & Tablets / Mobile Phones / Smartphones / Android Phones"],
        belongs="Phones & Tablets / Mobile Phones / Smartphones / iOS Phones",
        comment="iPhone filed under Android Phones — belongs in iOS Phones",
    ),

    # Both category trees, and the whole Shave & Hair Removal branch rather
    # than the single leaf that was reported. The catalogue carries two
    # parallel spellings of this path — one with a "Beauty & Personal Care"
    # level and one without — and the branch has four leaves under Shaving
    # Creams, Lotions & Gels alone. Naming one code would catch one filing and
    # miss the rest.
    CategoryRule(
        id="titan-gel-in-shaving",
        flag="Titan Gel in shaving category",
        keyword="titan gel",
        wrong_in=[
            "Health & Beauty / Beauty & Personal Care / Personal Care / Shave & Hair Removal",
            "Health & Beauty / Personal Care / Shave & Hair Removal",
        ],
        comment="Titan Gel filed as a shaving product — not a shaving cream, lotion or gel",
    ),

    # ── From the weekly search-quality reports ─────────────────────────────
    # Only findings that describe a product-data defect are here. That report
    # also carries search-relevance findings — "ear" returning grooming
    # trimmers ahead of earphones, "air" returning air fresheners — where the
    # products are filed correctly and the complaint is about ranking. A rule
    # for those would reject correct listings, so they are deliberately absent.

    CategoryRule(
        id="fridge-in-tools",
        flag="Refrigerator in Tools & Home Improvement",
        keyword="fridge",
        wrong_in=["Home & Office / Tools & Home Improvement"],
        belongs="Electronics / Home Appliances",
        comment="Refrigerator filed under Tools & Home Improvement — belongs in Home Appliances",
    ),

    # "water" and "dispenser" were separate rows of the report describing the
    # same products; the report says so itself. One rule, or every dispenser
    # is flagged twice under two names.
    CategoryRule(
        id="dispenser-in-tools",
        flag="Water dispenser in Tools & Home Improvement",
        keyword="dispenser",
        wrong_in=["Home & Office / Tools & Home Improvement"],
        belongs="Electronics / Home Appliances",
        comment="Water dispenser filed under Tools & Home Improvement — belongs in Home Appliances",
    ),

    # No keyword: nothing belongs here. Sellers filing books under the DVD
    # branch is the pattern this catches, so it rejects the whole subtree
    # rather than trying to tell a book from a film by its title — which would
    # be guesswork, and would miss exactly the listings that are dressed up to
    # look like films.
    #
    # Scoped to DVDs alone. Its siblings under Books, Movies and Music —
    # Fiction, Magazines, Stationery, Bestselling Books and the rest — are
    # legitimate destinations and are untouched.
    CategoryRule(
        id="anything-in-dvds",
        flag="Listed under DVDs",
        keyword="",                       # every product in the branch
        wrong_in=["Books, Movies and Music / DVDs"],
        comment="Filed under DVDs — books and other products do not belong in the DVD categories",
        columns=["CATEGORY_CODE"],        # no NAME dependency: the category is the offence
    ),

    CategoryRule(
        id="phone-mic-in-instruments",
        flag="Phone microphone in Musical Instruments",
        keyword="lavalier",
        wrong_in=["Musical Instruments / Microphones & Accessories"],
        belongs="Phones & Tablets / Accessories",
        comment="Phone-accessory microphone filed under Musical Instruments",
    ),
]


# ── Engine ─────────────────────────────────────────────────────────────────

def _keyword_pattern(keyword: str, match: str) -> re.Pattern:
    kw = re.escape(str(keyword).strip())
    # Multi-word keywords tolerate any run of whitespace between the words, so
    # "titan  gel" and "titan gel" both match.
    kw = kw.replace(r"\ ", r"\s+")
    if match == "substring":
        return re.compile(kw, re.IGNORECASE)
    # Lookarounds rather than \b: \b treats "-" as a boundary, so "titan-gel"
    # would match on \b and not here. Both readings are defensible; this one
    # is consistent with the rest of the app.
    return re.compile(rf"(?<!\w){kw}(?!\w)", re.IGNORECASE)


def _codes_under(paths: List[str], code_to_path: Dict) -> set:
    """Every category code at or beneath any of `paths`."""
    out = set()
    for code, path in (code_to_path or {}).items():
        p = str(path).strip()
        for want in paths:
            w = str(want).strip()
            if p == w or p.startswith(w + " / "):
                out.add(clean_category_code(str(code)))
                break
    return out


def _make_check(rule: CategoryRule, codes: set):
    def _check(data: pd.DataFrame, **_kwargs) -> pd.DataFrame:
        if data is None or data.empty or not codes:
            return pd.DataFrame(columns=getattr(data, "columns", []))
        if not {"NAME", "CATEGORY_CODE"}.issubset(data.columns):
            return pd.DataFrame(columns=data.columns)

        cat = (
            data["_cat_clean"]
            if "_cat_clean" in data.columns
            else data["CATEGORY_CODE"].fillna("").astype(str).map(clean_category_code)
        )
        in_scope = cat.isin(codes)
        if not in_scope.any():
            return pd.DataFrame(columns=data.columns)

        if str(rule.keyword).strip():
            pat = _keyword_pattern(rule.keyword, rule.match)
            names = data["NAME"].fillna("").astype(str)
            hit = in_scope & names.str.contains(pat, na=False)
        else:
            # No keyword: the category itself is the offence.
            hit = in_scope
        if not hit.any():
            return pd.DataFrame(columns=data.columns)

        flagged = data[hit].copy()
        detail = rule.comment
        if rule.belongs:
            detail = f"{detail} (should be: {rule.belongs})"
        flagged["Comment_Detail"] = detail
        flagged["Reason"] = rule.reason
        return flagged.drop_duplicates(subset=["PRODUCT_SET_SID"])

    return _check


def _flag_name(rule: CategoryRule) -> str:
    return rule.flag or rule.id.replace("-", " ").title()


def build_validators(support_files: Dict, country_code: str = "") -> List[tuple]:
    """Return (flag_name, fn, kwargs) tuples to append to the validator list."""
    code_to_path = (support_files or {}).get("code_to_path") or {}
    out = []
    for rule in RULES:
        if not rule.active:
            continue
        if rule.countries and country_code and country_code not in rule.countries:
            continue
        codes = _codes_under(rule.wrong_in, code_to_path)
        if not codes:
            # The category map did not resolve any of the paths. Skipping is
            # right — a rule with no scope would match nothing anyway — but it
            # is reported in the panel so a typo in a path is visible rather
            # than looking like a rule that simply never fires.
            logger.warning("general_rules: %s matched no categories for %s",
                           rule.id, rule.wrong_in)
            continue
        out.append((_flag_name(rule), _make_check(rule, codes), {}))
    return out


def relevant_columns() -> Dict[str, List[str]]:
    """Column dependencies, for the flag cache. Without these an unmapped flag
    falls back to hashing the whole frame on every run."""
    return {_flag_name(r): list(r.columns) for r in RULES if r.active}


def rule_health(support_files: Dict) -> pd.DataFrame:
    """One row per rule, for the sidebar panel."""
    code_to_path = (support_files or {}).get("code_to_path") or {}
    rows = []
    for rule in RULES:
        codes = _codes_under(rule.wrong_in, code_to_path) if rule.active else set()
        rows.append({
            "Rule": _flag_name(rule),
            "Keyword": rule.keyword,
            "Match": rule.match,
            "Categories": len(codes),
            "Active": rule.active,
            "Status": ("off" if not rule.active
                       else "no categories matched" if not codes
                       else "ok"),
        })
    return pd.DataFrame(rows)
