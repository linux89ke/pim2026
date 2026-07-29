"""
design_tokens.py - The single source of truth for how this tool looks.

Before this module every colour was an inlined hex literal: ~200 of them
spread across seven <style> blocks in streamlit_app.py and ui_components.py,
in four unrelated palettes (Jumia brand orange, one Tailwind set in the KPI
cards, a different Tailwind set in flag_pill_header, and plotly's Pastel in
the donut). Changing the brand colour meant grep-and-pray.

Three rules the tokens encode:

1. Orange is a signal, not a surface. The tool's whole job is flagging
   things; orange cannot mean "look here" while it is also the wallpaper.
   Neutral surfaces, orange reserved for the active state and the primary
   action.

2. Orange never carries white text. #F68B1E against white is 2.43:1 — it
   fails WCAG AA (4.5:1) both as text on white and as a fill under white
   text. So: INK_ON_ACCENT (dark) sits on orange fills at 7.2:1, and
   ACCENT_TEXT (#A85400, 4.7:1) is the only orange allowed to be text.

3. Severity is ordinal, not categorical. One 4-stop ramp replaces the five
   unrelated hues, so colour tells a reviewer how much trouble a flag is
   rather than which check produced it.
"""

from __future__ import annotations

import re

# ─── Colour ────────────────────────────────────────────────────────────────

COLORS = {
    # Surfaces
    "surface":        "#FBFAF9",   # warm neutral page background
    "panel":          "#FFFFFF",
    "panel_sunken":   "#F4F2F0",
    "hairline":       "#E8E5E1",
    "hairline_soft":  "#F0EDEA",

    # Ink
    "ink":            "#1A1917",
    "ink_muted":      "#6B6560",
    "ink_faint":      "#948D86",

    # Brand orange, split by role
    "accent":         "#F68B1E",   # fills only — see INK_ON_ACCENT
    "accent_hover":   "#E07C12",
    "accent_text":    "#A85400",   # 4.7:1 on white — the only orange for text
    "accent_wash":    "#FEF3E7",   # tinted background for active rows
    "ink_on_accent":  "#1A1917",   # 7.2:1 on #F68B1E

    # Status (used by KPI + report, not by severity)
    "positive":       "#2F6B45",
    "positive_wash":  "#E9F2EC",
    "negative":       "#B3261E",
    "negative_wash":  "#FBEAE9",
}

# ─── Severity ramp ─────────────────────────────────────────────────────────
#
# Ordinal, four stops. `order` drives the sort of the whole flags list, so a
# compliance blocker can never sit below a cosmetic flag on the page.

# `mark` exists because a Streamlit expander summary is plain markdown — no
# HTML, so the CSS spine cannot reach it. A coloured glyph is the only way to
# carry severity into the collapsed row, which is exactly where a reviewer
# scans. It never carries the meaning alone: the group header states it in
# words, and the label is readable on its own.
SEVERITY = {
    "blocker": {
        "order": 0,
        "label": "Blockers",
        "blurb": "Compliance and counterfeit risk. These cannot go live.",
        "color": "#B3261E",
        "wash":  "#FBEAE9",
        "spine": "#B3261E",
        "mark":  "🟥",
    },
    "judgment": {
        "order": 1,
        "label": "Needs judgment",
        "blurb": "A check fired but a human decides. Expect false positives.",
        "color": "#8A5200",
        "wash":  "#FBF1E3",
        "spine": "#B26A00",
        "mark":  "🟧",
    },
    "advisory": {
        "order": 2,
        "label": "Advisory",
        "blurb": "Data hygiene. Worth fixing, rarely worth rejecting over.",
        "color": "#3D5A75",
        "wash":  "#EDF2F6",
        "spine": "#4A6B8A",
        "mark":  "🟦",
    },
    "resolved": {
        "order": 3,
        "label": "Resolved",
        "blurb": "Already actioned.",
        "color": "#3F6B4F",
        "wash":  "#EAF1EC",
        "spine": "#3F6B4F",
        "mark":  "🟩",
    },
}

SEVERITY_ORDER = ["blocker", "judgment", "advisory", "resolved"]

# Exact FLAG strings, keyed to severity. These are the values that appear in
# final_report["FLAG"] (see REASON_MAP in constants.py) — matched after
# stripping the "(Prefetched)" suffix and any " - sub bucket" tail.
_FLAG_SEVERITY = {
    # ── Blockers: legal, counterfeit, seller-authorisation ──
    "Restricted brands":                          "blocker",
    "Prohibited products":                        "blocker",
    # A blocker, and one that cascades: adult content is a property of the
    # product, so if another seller lists the identical photo it is the same
    # problem regardless of who posted it.
    "Adult / NSFW content":                       "blocker",
    "Suspected Fake product":                     "blocker",
    "Suspected Fake Perfume":                     "blocker",
    "Counterfeit Sneakers":                       "blocker",
    "Suspected counterfeit Jerseys":              "blocker",
    "Brand Image Mismatch":                       "blocker",
    "Image Infringing":                           "blocker",
    "FDA":                                        "blocker",
    "Off-Platform Contact":                       "blocker",
    "Seller Not approved to sell Refurb":         "blocker",
    "Seller Approve to sell books":               "blocker",
    "Seller Approved to Sell Perfume":            "blocker",
    "Perfume Tester":                             "blocker",

    # ── Needs judgment: a check fired, a human decides ──
    "Wrong Category":                             "judgment",
    "Category Check":                             "judgment",
    "Miscellaneous Category":                     "judgment",
    "Category Max Price Exceeded":                "judgment",
    "Generic branded products with genuine brands": "judgment",
    "Generic BRAND Issues":                       "judgment",
    "Fashion brand issues":                       "judgment",
    "BRAND name repeated in NAME":                "judgment",
    "Product Name Brand Name":                    "judgment",
    "Specs Inconsistency":                        "judgment",
    "Incomplete Smartphone Name":                 "judgment",
    "Wrong Variation":                            "judgment",
    "Discount too high":                          "judgment",
    "Suspicious Discount":                        "judgment",
    "Duplicate product":                          "judgment",
    "Image Mismatch":                             "judgment",
    "Title Language Check":                       "judgment",

    # ── Advisory: data hygiene ──
    "Missing COLOR":                              "advisory",
    "Missing Weight/Volume":                      "advisory",
    "Unnecessary words in NAME":                  "advisory",
    "Single-word NAME":                           "advisory",
    "Product Warranty":                           "advisory",
    "Image Stretched":                            "advisory",
    "Image Blurry":                               "advisory",
    "Poor images":                                "advisory",
    "Image Too Many things displayed":            "advisory",
    "Poor Images Aspect Ratio":                   "advisory",
    "All Caps NAME":                              "advisory",
    "NAME too short":                             "advisory",
}

# Display labels. The FLAG string is a data key — it indexes session state,
# exports, apply_status_change and the generated report — so it is never
# renamed. This map is applied at render time only.
_FLAG_LABELS = {
    "Prohibited products":                        "Prohibited product",
    "Adult / NSFW content":                       "Adult or explicit content",
    "Suspected Fake product":                     "Suspected counterfeit",
    "Suspected Fake Perfume":                     "Suspected counterfeit perfume",
    "Suspected counterfeit Jerseys":              "Suspected counterfeit jersey",
    "Counterfeit Sneakers":                       "Suspected counterfeit sneakers",
    "Brand Image Mismatch":                       "Image doesn't match the brand",
    "Image Infringing":                           "Image infringes a trademark",
    "FDA":                                        "FDA restricted",
    "Off-Platform Contact":                       "Contact details in the listing",
    "Seller Not approved to sell Refurb":         "Seller not approved for refurbished",
    "Seller Approve to sell books":               "Seller not approved for books",
    "Seller Approved to Sell Perfume":            "Seller not approved for perfume",
    "Perfume Tester":                             "Tester listed for sale",
    "Wrong Category":                             "Wrong category",
    "Category Check":                             "Category check",
    "Title Language Check":                       "Title language check",
    "Other Reason (Custom)":                      "Other — manual reason",
    "Miscellaneous Category":                     "Filed under Miscellaneous",
    "Category Max Price Exceeded":                "Price above category cap",
    "Generic branded products with genuine brands": "Generic listing using a real brand",
    # check_generic_brand_issues matches brand == "generic" within the fashion
    # category codes (valid_category_codes_fas), so the category is half the
    # finding — "Generic brand name" left that out.
    "Generic BRAND Issues":                       "Generic listing in Fashion category",
    "Fashion brand issues":                       "Fashion brand needs checking",
    "BRAND name repeated in NAME":                "Brand repeated in title",
    "Specs Inconsistency":                        "Specs don't match the title",
    "Incomplete Smartphone Name":                 "Phone title missing specs",
    "Wrong Variation":                            "Wrong variation",
    "Discount too high":                          "Discount too high",
    "Suspicious Discount":                        "Discount looks wrong",
    "Duplicate product":                          "Duplicate listing",
    "Image Mismatch":                             "Image doesn't match the title",
    "Missing COLOR":                              "Missing colour",
    "Missing Weight/Volume":                      "Missing weight or volume",
    "Unnecessary words in NAME":                  "Filler words in title",
    "Single-word NAME":                           "Single-word title",
    "Product Warranty":                           "Warranty details missing",
    "Image Stretched":                            "Image stretched",
    "Image Blurry":                               "Image too low-resolution",
    "Poor images":                                "Poor image quality",
    "Image Too Many things displayed":            "Too many items in the image",
    "Poor Images Aspect Ratio":                   "Image aspect ratio is off",
    "All Caps NAME":                              "Title in all caps",
    "NAME too short":                             "Title too short",
    "Title Language Check - Not In English":      "Title not in English",
    # The "Other" bucket is everything the language check catches that is not
    # a non-English title — in practice grocery listings with no quantity in
    # the title. Named for what it actually finds rather than for being the
    # leftover branch of _classify_title_language_sub_bucket.
    "Title Language Check - Other":               "Grocery items missing Quantity in title",
    "Product Name Brand Name – Brand Repeated In Title":
        "Brand repeated in title",
    "Product Name Brand Name – Inspired/Alternative Perfume Brand":
        "Inspired-by perfume brand",
    "Product Name Brand Name – Generic/Placeholder Brand":
        "Placeholder brand name",
    "Product Name Brand Name – High-End Brand Counterfeit Suspected":
        "High-end brand, counterfeit suspected",
    "Product Name Brand Name – Other":            "Brand name needs checking",
}

# One sub-bucket outranks its parent: a high-end counterfeit suspicion is a
# blocker even though the rest of the "Product Name Brand Name" family is a
# judgment call.
_FLAG_SEVERITY_OVERRIDES = {
    "Product Name Brand Name – High-End Brand Counterfeit Suspected": "blocker",
}

_PREFETCH_RE = re.compile(r"\s*\(Prefetched\)\s*$", re.IGNORECASE)

# Sub-bucket flags are built with an en dash (–) in streamlit_app — see
# _classify_title_language_sub_bucket — while several keys below were written
# with a plain hyphen. Those lookups silently missed and fell through to the
# parent label, which is why "Title Language Check – Other" rendered as
# "Title language check — other" instead of its mapped name. Normalising the
# dash on both sides fixes the whole class of near-miss rather than the two
# keys that happened to be noticed.
_DASH_RE = re.compile(r"[–—]")


def _norm_key(text: str) -> str:
    return _DASH_RE.sub("-", str(text or "")).strip()


def base_flag(flag: str) -> str:
    """Strip the '(Prefetched)' suffix that the ZIP path appends to a FLAG."""
    return _PREFETCH_RE.sub("", str(flag or "")).strip()


def flag_severity(flag: str) -> str:
    """
    Severity bucket for a FLAG string. Falls back to 'judgment' — an unknown
    check is something a human should look at, not something to bury under
    Advisory or panic about under Blockers.
    """
    name = _norm_key(base_flag(flag))
    if not name:
        return "judgment"

    _sev = {_norm_key(k): v for k, v in _FLAG_SEVERITY.items()}
    _ovr = {_norm_key(k): v for k, v in _FLAG_SEVERITY_OVERRIDES.items()}

    if name in _ovr:
        return _ovr[name]
    if name in _sev:
        return _sev[name]

    # Sub-buckets arrive as "Parent - Detail" (dashes already normalised).
    if " - " in name:
        parent = name.split(" - ", 1)[0].strip()
        if parent in _sev:
            return _sev[parent]

    # Manual and bulk actions the reviewer initiated themselves.
    lowered = name.lower()
    if lowered.startswith(("manual", "bulk")):
        return "resolved"

    return "judgment"


def flag_label(flag: str) -> str:
    """Human-readable name for a FLAG. Never used as a key — display only."""
    name = base_flag(flag)
    key = _norm_key(name)
    _labels = {_norm_key(k): v for k, v in _FLAG_LABELS.items()}
    label = _labels.get(key)

    if label is None and " - " in key:
        parent, detail = key.split(" - ", 1)
        parent_label = _labels.get(parent.strip())
        if parent_label:
            label = f"{parent_label} — {detail.strip().lower()}"

    return label or name


def severity_sort_key(flag: str):
    """Sort flags by severity first, then alphabetically inside a bucket."""
    sev = flag_severity(flag)
    return (SEVERITY[sev]["order"], flag_label(flag).lower())


# ─── Type ──────────────────────────────────────────────────────────────────
#
# IBM Plex Sans for UI, IBM Plex Mono for every numeric. The mono is the
# functional choice, not a decorative one: reviewers compare and copy 12-digit
# SIDs all day, and tabular figures make a column of them scannable. It is
# also the tool's signature — the one where the numbers line up.

FONT_UI = "'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
FONT_MONO = "'IBM Plex Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace"

# A real scale, replacing the ad-hoc 9/10/11/12/13/14/16/26/28/36px sizes.
# 11px is the floor — the old 9px badge text was unreadable.
TYPE = {
    "micro":   "11px",   # badges, captions
    "small":   "12px",   # table cells, meta
    "body":    "13px",
    "lead":    "15px",
    "title":   "18px",
    "display": "24px",   # KPI values
}

FONT_IMPORT = (
    "@import url('https://fonts.googleapis.com/css2"
    "?family=IBM+Plex+Sans:wght@400;500;600;700"
    "&family=IBM+Plex+Mono:wght@500;600;700&display=swap');"
)


# ─── Stylesheet ────────────────────────────────────────────────────────────

def css_variables() -> str:
    """The token block, for injecting into any <style> or iframe document."""
    decls = "\n".join(f"      --{k.replace('_', '-')}: {v};" for k, v in COLORS.items())
    sev = "\n".join(
        f"      --sev-{name}: {cfg['spine']};\n"
        f"      --sev-{name}-wash: {cfg['wash']};\n"
        f"      --sev-{name}-text: {cfg['color']};"
        for name, cfg in SEVERITY.items()
    )
    sizes = "\n".join(f"      --text-{k}: {v};" for k, v in TYPE.items())
    return (
        ":root {\n"
        f"{decls}\n{sev}\n{sizes}\n"
        f"      --font-ui: {FONT_UI};\n"
        f"      --font-mono: {FONT_MONO};\n"
        "    }"
    )


def app_css() -> str:
    """
    The global stylesheet. Replaces the ad-hoc <style> block that used to sit
    inline in streamlit_app.py, plus the scattered per-component blocks.
    """
    C = COLORS
    return f"""
    {FONT_IMPORT}
    {css_variables()}

    html, body, [class*="css"] {{ font-family: var(--font-ui); }}
    .stApp {{ background: {C['surface']}; }}
    header[data-testid="stHeader"] {{ background: transparent !important; }}
    div[data-testid="stStatusWidget"] {{ z-index: 9999999 !important; }}

    h1, h2, h3, h4 {{
      color: {C['ink']} !important;
      font-family: var(--font-ui);
      letter-spacing: -0.011em;
    }}

    /* Every numeric in the app: SIDs, counts, prices, percentages.
       tnum keeps digit columns aligned; zero is slashed for legibility. */
    .num, .kpi-value, .flag-count, .rail-num, code, pre {{
      font-family: var(--font-mono);
      font-feature-settings: "tnum" 1, "zero" 1;
      font-variant-numeric: tabular-nums slashed-zero;
    }}

    /* ── Buttons ───────────────────────────────────────────────────────
       Orange fills carry dark ink at 7.2:1. White-on-orange was 2.43:1
       and failed AA in every button in the app. */
    .stButton > button {{
      border-radius: 6px;
      font-weight: 600;
      font-size: var(--text-body);
      transition: background .15s ease, border-color .15s ease;
    }}
    .stButton > button[kind="primary"] {{
      background-color: {C['accent']} !important;
      border: 1px solid {C['accent_hover']} !important;
      color: {C['ink_on_accent']} !important;
    }}
    .stButton > button[kind="primary"]:hover {{
      background-color: {C['accent_hover']} !important;
      color: {C['ink_on_accent']} !important;
    }}
    .stButton > button[kind="secondary"] {{
      background-color: {C['panel']} !important;
      border: 1px solid {C['hairline']} !important;
      color: {C['ink']} !important;
    }}
    .stButton > button[kind="secondary"]:hover {{
      background-color: {C['accent_wash']} !important;
      border-color: {C['accent']} !important;
      color: {C['accent_text']} !important;
    }}
    .stButton > button:focus-visible {{
      outline: 2px solid {C['accent_text']} !important;
      outline-offset: 2px !important;
    }}

    /* ── Metrics ── */
    div[data-testid="stMetric"] {{
      background: {C['panel']};
      border: 1px solid {C['hairline']};
      border-radius: 10px;
      padding: 12px 16px;
    }}
    div[data-testid="stMetricValue"] {{
      color: {C['ink']};
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums slashed-zero;
      font-weight: 600;
      font-size: var(--text-display) !important;
    }}
    div[data-testid="stMetricLabel"] {{
      color: {C['ink_muted']};
      font-size: var(--text-micro);
      text-transform: uppercase;
      letter-spacing: .07em;
      font-weight: 600;
    }}

    /* ── Expanders: the flag rows ── */
    div[data-testid="stExpander"] {{
      border: 1px solid {C['hairline']};
      border-radius: 10px;
      background: {C['panel']};
      margin-bottom: 6px;
    }}
    div[data-testid="stExpander"] summary {{
      background-color: {C['panel']};
      padding: 10px 14px;
      border-radius: 10px;
      font-size: var(--text-body);
    }}
    div[data-testid="stExpander"] summary:hover {{
      background-color: {C['panel_sunken']};
    }}

    /* ── Scrollbars ── */
    ::-webkit-scrollbar {{ width: 14px; height: 14px; }}
    ::-webkit-scrollbar-track {{ background: {C['panel_sunken']}; }}
    ::-webkit-scrollbar-thumb {{
      background: #C9C3BC; border-radius: 8px;
      border: 3px solid {C['panel_sunken']};
    }}
    ::-webkit-scrollbar-thumb:hover {{ background: {C['ink_faint']}; }}
    * {{ scrollbar-width: thin; scrollbar-color: #C9C3BC {C['panel_sunken']}; }}

    div[data-baseweb="segmented-control"] button {{ border-radius: 6px; }}
    div[data-baseweb="segmented-control"] button[aria-pressed="true"] {{
      background-color: {C['accent']} !important;
      color: {C['ink_on_accent']} !important;
    }}

    /* ── Sticky filter toolbar ─────────────────────────────────────────
       Consolidating the per-flag filters into one bar was right, but it
       left the controls at the top of a very long page: changing a filter
       meant scrolling all the way back. Pinned just under the context rail
       (40px + its margin) it keeps the single-toolbar win without the
       round trip.

       It never worked. Streamlit wraps every element in a tight-fitting
       block, so a sticky child's containing block is exactly its own height
       and it has no room to travel — it scrolls away like a static element.
       Measured against three DOM arrangements, including a tall wrapper with
       overflow:visible; all three failed. Computed style still reports
       "sticky" in that state, so it cannot be used to check the behaviour.

       Filters moved to the sidebar, which is always on screen without
       fighting the framework, and the search went back inside each expander
       next to the rows it filters. */

    /* ── ZIP / prefetched flag rows ────────────────────────────────────
       Restores the orange differentiation these rows always had. The old
       version was a MutationObserver that matched on the label text and
       wrote a hardcoded tan with !important; this is a keyed container, so
       it is themeable, costs nothing at runtime, and leaves the severity
       spine inside the row intact. */
    [class*="st-key-flagrow_zip_"] div[data-testid="stExpander"] {{
      border-color: {C['accent']};
      border-left: 3px solid {C['accent']};
      background: {C['accent_wash']};
    }}
    [class*="st-key-flagrow_zip_"] div[data-testid="stExpander"] summary {{
      background: {C['accent_wash']};
      font-weight: 600;
    }}
    [class*="st-key-flagrow_zip_"] div[data-testid="stExpander"] summary:hover {{
      background: #FDE9D2;
    }}
    [class*="st-key-flagrow_zip_"] div[data-testid="stExpander"]:hover {{
      box-shadow: 0 4px 14px rgba(246,139,30,.20);
    }}

    /* ── Motion ────────────────────────────────────────────────────────
       Two rules keep this from getting in a reviewer's way: nothing that
       delays reading a value animates for longer than 320ms, and nothing
       loops. Everything below is disabled wholesale under
       prefers-reduced-motion at the end of this block. */

    @keyframes dtFadeUp {{
      from {{ opacity: 0; transform: translateY(8px); }}
      to   {{ opacity: 1; transform: none; }}
    }}
    @keyframes dtFadeIn {{ from {{ opacity: 0; }} to {{ opacity: 1; }} }}

    /* Four elements, no stagger, no delay — short enough that a rerun does
       not visibly re-deal them, unlike the staggered version which gated the
       numbers behind half a second every time anything on the page changed. */
    .kpi-card {{ animation: dtFadeIn .18s ease both; }}
    .kpi-card {{ transition: transform .18s cubic-bezier(.2,.8,.3,1), box-shadow .18s; }}
    .kpi-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 8px 22px rgba(26,25,23,.09);
    }}
    /* The staged reveal that used to be here delayed each figure by up to
       .34s. On first load that read as the numbers arriving; on every
       subsequent rerun it just meant the KPIs a reviewer was reading blinked
       and came back. The number is the point — it renders immediately. */
    .kpi-bar {{ position: relative; overflow: hidden; }}
    /* The sweep that used to run here was a 1.1s animation on a .5s delay,
       replayed on every rerun. Removed for the same reason as the rest: it
       fired on interactions, not on arrival. */

    .ctx-rail {{ animation: dtFadeIn .16s ease both; }}

    /* No entry animation on expanders. Streamlit re-creates elements on every
       rerun, so `animation` here did not play once on arrival — it replayed
       for all fifteen-plus flag rows on every single interaction, which read
       as the whole page lagging. Transitions are safe: they only run on the
       element actually being hovered or focused. */
    div[data-testid="stExpander"] {{
      transition: border-color .18s ease, box-shadow .18s ease,
                  transform .18s cubic-bezier(.2,.8,.3,1);
    }}
    div[data-testid="stExpander"]:hover {{
      border-color: {C['accent']};
      box-shadow: 0 4px 14px rgba(26,25,23,.07);
    }}
    div[data-testid="stExpander"] summary {{ transition: background-color .15s ease; }}
    div[data-testid="stExpander"] summary:focus-visible {{
      outline: 2px solid {C['accent_text']};
      outline-offset: -2px;
      border-radius: 10px;
    }}
    /* The opened body used to fade in over .22s. That fade sat directly in
       front of the thing you clicked to see, on top of the rerun Streamlit
       already costs — so opening a flag felt slower than it was. Gone. */

    .stButton > button {{
      transform: translateZ(0);
      transition: background-color .15s ease, border-color .15s ease,
                  box-shadow .18s ease, transform .12s cubic-bezier(.2,.8,.3,1);
    }}
    .stButton > button:hover {{ transform: translateY(-1px); }}
    .stButton > button:active {{ transform: translateY(0) scale(.98); }}
    .stButton > button[kind="primary"]:hover {{
      box-shadow: 0 4px 14px rgba(246,139,30,.38);
    }}

    div[data-testid="stMetric"] {{
      transition: border-color .18s ease;
    }}
    div[data-testid="stMetric"]:hover {{ border-color: {C['accent']}; }}

    div[data-testid="stAlert"] {{ animation: dtFadeUp .28s cubic-bezier(.2,.8,.3,1) both; }}

    @media (prefers-reduced-motion: reduce) {{
      *, *::before, *::after {{
        animation-duration: .01ms !important;
        animation-iteration-count: 1 !important;
        animation-delay: 0ms !important;
        transition-duration: .01ms !important;
      }}
      .kpi-bar::after {{ display: none; }}
      .kpi-card:hover, .stButton > button:hover,
      div[data-testid="stExpander"]:hover {{ transform: none; }}
    }}
    """


# ─── Back-compat ───────────────────────────────────────────────────────────
#
# Existing modules import JUMIA_COLORS from constants and index it by these
# keys in a few hundred places. Rather than rewrite every call site at once,
# the old keys keep working and now resolve to the corrected values — so the
# contrast fix reaches code that has not been migrated yet.

JUMIA_COLORS_OVERRIDE = {
    "primary_orange":   COLORS["accent"],
    "secondary_orange": COLORS["accent_hover"],
    "jumia_red":        COLORS["negative"],
    "dark_gray":        COLORS["ink"],
    "medium_gray":      COLORS["ink_muted"],
    "light_gray":       COLORS["panel_sunken"],
    "border_gray":      COLORS["hairline"],
    "success_green":    COLORS["positive"],
    "warning_yellow":   "#B26A00",
    "white":            COLORS["panel"],
    "black":            COLORS["ink"],
}
