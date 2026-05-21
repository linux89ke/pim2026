"""
pages/QCResultsReview.py
Imports · constants · helpers · sidebar · upload · metrics · dashboard · flags breakdown · visual review · exports
"""
import os, zipfile, base64, hashlib, json
from pathlib import Path
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, List, Tuple
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

try:
    from constants import JUMIA_COLORS, NEW_FILE_MAPPING, GRID_COLS
except ImportError:
    JUMIA_COLORS = {
        "primary_orange":"#F68B1E","secondary_orange":"#FF9933","jumia_red":"#E73C17",
        "dark_gray":"#313133","medium_gray":"#5A5A5C","light_gray":"#F5F5F5",
        "border_gray":"#E0E0E0","success_green":"#4CAF50","warning_yellow":"#FFC107",
        "white":"#FFFFFF","black":"#000000",
    }
    NEW_FILE_MAPPING = {
        "cod_productset_sid":"PRODUCT_SET_SID","dsc_name":"NAME","dsc_brand_name":"BRAND",
        "cod_category_code":"CATEGORY_CODE","dsc_category_name":"CATEGORY",
        "dsc_shop_seller_name":"SELLER_NAME","dsc_shop_active_country":"ACTIVE_STATUS_COUNTRY",
        "cod_parent_sku":"PARENTSKU","color":"COLOR","color_family":"COLOR_FAMILY",
        "image_1":"MAIN_IMAGE","dsc_status":"LISTING_STATUS","product_warranty":"PRODUCT_WARRANTY",
        "warranty_duration":"WARRANTY_DURATION","warranty_address":"WARRANTY_ADDRESS",
        "warranty_type":"WARRANTY_TYPE","count_variations":"COUNT_VARIATIONS",
        "list_variations":"LIST_VARIATIONS","list_seller_skus":"SELLER_SKU",
        "global_sale_price":"GLOBAL_SALE_PRICE","global_price":"GLOBAL_PRICE",
    }
    GRID_COLS = [
        "PRODUCT_SET_SID","NAME","BRAND","CATEGORY","SELLER_NAME",
        "MAIN_IMAGE","GLOBAL_SALE_PRICE","GLOBAL_PRICE","COLOR","PARENTSKU",
    ]

try:
    from ui_components import build_fast_grid_html
    _GRID_OK = True
except Exception:
    _GRID_OK = False

try:
    from data_utils import format_local_price
except ImportError:
    def format_local_price(v, c="Kenya"):
        try: return f"${float(v):,.0f}"
        except: return str(v) if v else ""

try:
    from translations import LANGUAGES, get_translation as _gt
    def _t(k): return _gt(st.session_state.get("ui_lang","en"), k)
except ImportError:
    LANGUAGES = {"English":"en","Français":"fr","العربية":"ar"}
    _FB = {
        "upload_files":"Upload Files","val_results":"Validation Results",
        "flags_breakdown":"Flags Breakdown","system_status":"System Status",
        "clear_cache":"Clear Cache","display_settings":"Display Settings",
        "manual_review":"Manual Image & Category Review","download_reports":"Download Reports",
        "search_grid":"Search","approve_btn":"✔ Approve Selected","reject_as":"✘ Reject As…",
        "approved":"Approved","rejected":"Rejected","rej_rate":"Rejection Rate",
        "common_skus":"Common SKUs","poor_img":"Poor images","wrong_cat":"Wrong Category",
        "fake_prod":"Suspected Fake product","restr_brand":"Restricted brands",
        "wrong_brand":"Generic branded products with genuine brands",
        "prohibited":"Prohibited products","missing_color":"Missing COLOR",
        "more_options":"More options","undo":"Undo","clear_sel":"Clear Selection",
        "items_pending":"items pending review","batch_reject":"Batch Reject Selected",
        "select_all":"Select All","deselect_all":"Deselect All",
    }
    def _t(k): return _FB.get(k, k.replace("_"," ").title())

try:
    import plotly.graph_objects as go
    import plotly.express as px
    _PLOTLY_OK = True
except ImportError:
    _PLOTLY_OK = False

if "layout_mode" not in st.session_state:
    st.session_state.layout_mode = "wide"
try:
    st.set_page_config(page_title="QC Results Review", page_icon="✅",
                       layout=st.session_state.layout_mode)
except: pass

O  = JUMIA_COLORS["primary_orange"]
G  = JUMIA_COLORS["success_green"]
R  = JUMIA_COLORS["jumia_red"]
DG = JUMIA_COLORS["dark_gray"]
MG = JUMIA_COLORS["medium_gray"]

FLAG_MAP: List[Tuple] = [
    ("Wrong Category",     "category_check_status",          "category_check_rejection_reason",          "1000007 - Wrong Category",   "Wrong Category"),
    ("Product Warranty",   "warranty_check_status",          "warranty_rejection_reason",                "1000007 - Product Warranty", "Product Warranty"),
    ("FDA Check",          "fda_check_status",               "fda_rejection_reason",                     "1000007 - FDA",              "FDA Check"),
    ("Missing COLOR",      "color_check_status",             "color_rejection_reason",                   "1000007 - Missing Color",    "Missing COLOR"),
    ("Wrong Variation",    "variation_check_status",         "variation_rejection_reason",               "1000007 - Wrong Variation",  "Wrong Variation"),
    ("BRAND name in NAME", "product_name_brand_name_status", "product_name_brand_name_rejection_reason", "1000007 - Brand in Name",    "BRAND name in NAME"),
    ("Title Language",     "title_language_check_status",    "title_language_check_reason",              "1000007 - Title Language",   "Title Language"),
    ("Image Quality",      "image_quality_check_status",     "image_quality_check_reason",               "1000007 - Image Quality",    "Poor images"),
    ("Brand on Image",     "brand_image_check_status",       "brand_image_check_reason",                 "1000007 - Brand on Image",   "Brand on Image"),
]

FLAG_COLS: Dict[str, List[str]] = {
    "Wrong Category":     ["PRODUCT_SET_SID","NAME","BRAND","CATEGORY","SELLER_NAME","PARENTSKU","GLOBAL_SALE_PRICE","category_check_rejection_reason"],
    "Product Warranty":   ["PRODUCT_SET_SID","NAME","BRAND","SELLER_NAME","PARENTSKU","PRODUCT_WARRANTY","WARRANTY_TYPE","WARRANTY_DURATION","WARRANTY_ADDRESS","warranty_rejection_reason"],
    "FDA Check":          ["PRODUCT_SET_SID","NAME","BRAND","CATEGORY","SELLER_NAME","fda_rejection_reason"],
    "Missing COLOR":      ["PRODUCT_SET_SID","NAME","BRAND","COLOR","COLOR_FAMILY","SELLER_NAME","PARENTSKU","color_rejection_reason"],
    "Wrong Variation":    ["PRODUCT_SET_SID","NAME","BRAND","SELLER_NAME","PARENTSKU","COUNT_VARIATIONS","LIST_VARIATIONS","variation_rejection_reason"],
    "BRAND name in NAME": ["PRODUCT_SET_SID","NAME","BRAND","SELLER_NAME","product_name_brand_name_rejection_reason"],
    "Title Language":     ["PRODUCT_SET_SID","NAME","BRAND","CATEGORY","SELLER_NAME","title_language_check_reason"],
    "Image Quality":      ["PRODUCT_SET_SID","NAME","BRAND","SELLER_NAME","PARENTSKU","image_quality_check_reason"],
    "Brand on Image":     ["PRODUCT_SET_SID","NAME","BRAND","SELLER_NAME","brand_image_check_reason"],
}

COL_LABELS = {
    "PRODUCT_SET_SID":"Product Set SID","NAME":"Product Name","BRAND":"Brand",
    "CATEGORY":"Category","SELLER_NAME":"Seller","PARENTSKU":"Parent SKU",
    "COLOR":"Color","COLOR_FAMILY":"Color Family","GLOBAL_SALE_PRICE":"Sale Price (USD)",
    "GLOBAL_PRICE":"Price (USD)","PRODUCT_WARRANTY":"Warranty","WARRANTY_TYPE":"Warranty Type",
    "WARRANTY_DURATION":"Duration","WARRANTY_ADDRESS":"Address",
    "COUNT_VARIATIONS":"# Variations","LIST_VARIATIONS":"Variations List",
    **{rc:"Rejection Reason" for _,_,rc,_,_ in FLAG_MAP},
}


# ── load Rejection Reasons from reasons.xlsx (project file) ──────────────────
@st.cache_data(show_spinner=False)
def _load_reasons_df() -> pd.DataFrame:
    """Load reasons.xlsx — columns: flag, reason, comment, French, Arabic"""
    _root = os.path.join(os.path.dirname(__file__), "..")
    _candidates = [
        os.path.normpath(os.path.join(_root, "reasons.xlsx")),
        os.path.normpath(os.path.join(_root, "reason.xlsx")),
        "reasons.xlsx",
        "reason.xlsx",
    ]
    for _path in _candidates:
        if os.path.exists(_path):
            try:
                return pd.read_excel(_path, dtype=str).fillna("")
            except Exception as _e:
                st.warning(f"Could not read {_path}: {_e}")
    return pd.DataFrame(columns=["flag","reason","comment","French","Arabic"])

@st.cache_data(show_spinner=False)
def _build_flag_lookup() -> dict:
    """Build {flag_lower: {reason, comment, French, Arabic}} from reasons.xlsx"""
    _df = _load_reasons_df()
    if _df.empty:
        return {}
    _lkup = {}
    for _, _row in _df.iterrows():
        _flag = str(_row.get("flag","")).strip()
        if _flag:
            _lkup[_flag.lower()] = {
                "reason":  str(_row.get("reason","")).strip(),
                "comment": str(_row.get("comment","")).strip(),
                "French":  str(_row.get("French","")).strip(),
                "Arabic":  str(_row.get("Arabic","")).strip(),
            }
    return _lkup

def _load_rejection_reasons() -> pd.DataFrame:
    """Return full reasons.xlsx DataFrame for the RejectionReasons export sheet."""
    return _load_reasons_df()

# ✅ dropdown options come from the flag column in reasons.xlsx
def _load_rlist() -> list:
    _df = _load_reasons_df()
    if not _df.empty and "flag" in _df.columns:
        return [v for v in _df["flag"].tolist() if str(v).strip()]
    return ["Other Reason"]

_RLIST = _load_rlist()

_FLAGS_MAPPING = {r:{"reason":f"1000007 - {r}","en":r,"fr":r} for r in _RLIST}

_GRID_REASON_MAP: Dict[str,str] = {
    "REJECT_POOR_IMAGE":    "Poor images",
    "REJECT_IMG_STRETCHED": "Image Stretched",
    "REJECT_IMG_BLURRY":    "Image Blurry",
    "REJECT_IMG_MISMATCH":  "Image Mismatch",
    "REJECT_IMG_INFRINGING":"Image Infringing",
    "REJECT_IMG_TOO_MANY":  "Image Too Many things displayed",
    "REJECT_WRONG_CAT":     "Wrong Category",
    "REJECT_FAKE":          "Suspected Fake product",
    "REJECT_BRAND":         "Restricted brands",
    "REJECT_WRONG_BRAND":   "Generic branded products with genuine brands",
    "REJECT_PROHIBITED":    "Prohibited products",
    "REJECT_COLOR":         "Missing COLOR",
}

_IPP_OPTIONS     = [20,50,100,200]
COUNTRIES        = ["Kenya","Uganda","Nigeria","Ghana","Morocco"]
FLAG_EMOJIS      = {"Kenya":"🇰🇪","Uganda":"🇺🇬","Nigeria":"🇳🇬","Ghana":"🇬🇭","Morocco":"🇲🇦"}
COUNTRY_CODE_MAP = {
    "jumia-ke":"Kenya","jumia-ug":"Uganda","jumia-ng":"Nigeria",
    "jumia-gh":"Ghana","jumia-ma":"Morocco",
}
IMAGE_EXTS = {".jpg",".jpeg",".png",".gif",".webp"}

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(f"""<style>
div[data-testid="column"]:nth-child(1) [data-testid="stMetric"]{{border-top:3px solid {DG}!important;}}
div[data-testid="column"]:nth-child(2) [data-testid="stMetric"]{{border-top:3px solid {G}!important;}}
div[data-testid="column"]:nth-child(3) [data-testid="stMetric"]{{border-top:3px solid {R}!important;}}
div[data-testid="column"]:nth-child(4) [data-testid="stMetric"]{{border-top:3px solid {O}!important;}}
div[data-testid="column"]:nth-child(5) [data-testid="stMetric"]{{border-top:3px solid {MG}!important;}}
[data-testid="stMetric"]{{padding-top:10px!important;border-radius:6px;}}
.vr-name{{font-size:.80rem;font-weight:700;color:#111;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;}}
.vr-meta{{font-size:.70rem;color:#888;margin-top:3px;}}
.badge-rej{{background:{R};color:#fff;font-size:.64rem;padding:2px 8px;border-radius:10px;font-weight:700;}}
.badge-app{{background:{G};color:#fff;font-size:.64rem;padding:2px 8px;border-radius:10px;font-weight:700;}}
.status-done{{background:#f0faf0;border:1px solid #c3e6cb;border-radius:6px;padding:10px 16px;color:#155724;font-size:.9rem;margin:10px 0;}}
.sel-badge{{display:inline-block;background:{O}22;color:{O};border:1px solid {O}66;border-radius:12px;padding:2px 12px;font-size:.80rem;font-weight:600;}}
.img-card-name{{font-size:.78rem;font-weight:700;color:#111;margin-top:6px;line-height:1.35;word-break:break-word;}}
.img-card-sid{{font-size:.68rem;color:#999;margin-top:2px;}}
.img-card-reason{{background:#fff2f2;border-left:3px solid {R};border-radius:4px;padding:5px 8px;margin-top:5px;font-size:.72rem;color:{R};font-weight:600;line-height:1.4;word-break:break-word;}}
</style>""", unsafe_allow_html=True)

# ── session state defaults ────────────────────────────────────────────────────
_DEFS = dict(
    ui_lang="en", layout_mode="wide", qcr_df=None, qcr_img_store={},
    qcr_final_report=pd.DataFrame(), qcr_all_data=pd.DataFrame(), qcr_zip_hash="",
    selected_country="Uganda", qcr_uploader_key=0, qcr_show_review=False,
    grid_page=0, grid_items_per_page=50, qcr_exports={}, qcr_display_cache={},
    qcr_bridge_counter=0, qcr_toasts=[], qcr_flags_init=False,
    qcr_bridge_key=0, do_scroll_top=False,
)
for _k, _v in _DEFS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

if st.session_state.grid_items_per_page not in _IPP_OPTIONS:
    st.session_state.grid_items_per_page = min(
        _IPP_OPTIONS, key=lambda x: abs(x - st.session_state.grid_items_per_page)
    )

for _msg in st.session_state.qcr_toasts:
    st.toast(_msg, icon="✅")
st.session_state.qcr_toasts = []

# ── helpers ───────────────────────────────────────────────────────────────────
def load_zip(raw: bytes):
    imgs: Dict[str,bytes] = {}
    df = None
    try:
        with zipfile.ZipFile(BytesIO(raw)) as zf:
            ns   = zf.namelist()
            csvs = [n for n in ns if Path(n).name.lower()=="qc_results.csv" and "__MACOSX" not in n]
            if not csvs: csvs = [n for n in ns if n.lower().endswith(".csv")  and "__MACOSX" not in n]
            if not csvs: csvs = [n for n in ns if n.lower().endswith(".xlsx") and "__MACOSX" not in n]
            if csvs:
                rf = zf.read(csvs[0])
                if csvs[0].lower().endswith(".xlsx"):
                    df = pd.read_excel(BytesIO(rf), dtype=str)
                else:
                    for enc in ("utf-8","latin-1","cp1252"):
                        try: df = pd.read_csv(BytesIO(rf), dtype=str, encoding=enc); break
                        except UnicodeDecodeError: continue
            for n in ns:
                if "__MACOSX" in n or Path(n).suffix.lower() not in IMAGE_EXTS:
                    continue
                b = zf.read(n)
                imgs[n.strip().lower()]            = b
                imgs[Path(n).name.strip().lower()] = b
    except Exception as e:
        st.error(f"ZIP error: {e}")
    return df, imgs


def find_image(fn: str, imgs: dict) -> Optional[bytes]:
    if not fn or str(fn).strip().lower() in ("","nan","none"):
        return None
    fn = str(fn).strip()
    for c in [fn.lower(), Path(fn).name.lower(), fn.lstrip("/").lower()]:
        if c in imgs:
            return imgs[c]
    stem = Path(fn).stem.lower()[:40]
    if stem:
        for k, v in imgs.items():
            if stem in k:
                return v
    return None


def _pil_resample():
    try:
        from PIL import Image as _I; return _I.Resampling.LANCZOS
    except AttributeError: pass
    try:
        from PIL import Image as _I; return _I.LANCZOS
    except AttributeError: pass
    try:
        from PIL import Image as _I; return _I.ANTIALIAS
    except AttributeError:
        return 1

_RESAMPLE = _pil_resample()


def img_to_uri(fn: str, imgs: dict, max_px: int = 160) -> str:
    raw = find_image(fn, imgs)
    if not raw:
        return ""
    try:
        from PIL import Image as _PIL
        pil = _PIL.open(BytesIO(raw))
        pil.load()
        pil.thumbnail((max_px, max_px), _RESAMPLE)
        buf = BytesIO()
        if pil.mode != "RGB":
            pil = pil.convert("RGB")
        pil.save(buf, format="JPEG", quality=72, optimize=True)
        return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()
    except Exception:
        pass
    ext  = Path(str(fn)).suffix.lower().lstrip(".") or "jpg"
    mime = {"jpg":"jpeg","jpeg":"jpeg","png":"png","gif":"gif","webp":"webp"}.get(ext,"jpeg")
    return f"data:image/{mime};base64," + base64.b64encode(raw).decode()


def img_to_raw(fn: str, imgs: dict) -> Optional[bytes]:
    return find_image(fn, imgs)


def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={s.lower():t for s,t in NEW_FILE_MAPPING.items() if s.lower() in df.columns})
    for alt in ["image_filename","image1","image_1"]:
        if "MAIN_IMAGE" not in df.columns and alt in df.columns:
            df["MAIN_IMAGE"] = df[alt]
    if "MAIN_IMAGE" not in df.columns:
        df["MAIN_IMAGE"] = ""
    if "PRODUCT_SET_SID" not in df.columns:
        for alt in ["cod_productset_sid","productset_sid","sid"]:
            if alt in df.columns:
                df["PRODUCT_SET_SID"] = df[alt]
                break
    return df


def derive_report(df: pd.DataFrame) -> pd.DataFrame:
    _lkup = _build_flag_lookup()
    rows  = []
    for _, r in df.iterrows():
        sid = str(r.get("PRODUCT_SET_SID","")).strip()
        ff = fr = fc = None
        for _, sc, rc, _rcode, alias in FLAG_MAP:     # _rcode no longer used
            if sc not in df.columns:
                continue
            if str(r.get(sc,"")).strip().lower() == "rejected":
                if ff is None:
                    ff          = alias
                    _entry      = _lkup.get(alias.lower(), {})
                    fr          = _entry.get("reason",  f"1000007 - {alias}")
                    fc          = _entry.get("comment", "")
        rows.append({
            "ProductSetSid": sid,
            "ParentSKU":     str(r.get("PARENTSKU","")).strip(),
            "Status":        "Rejected" if ff else "Approved",
            "Reason":        fr or "",
            "Comment":       fc or "",
            "FLAG":          ff or "",
            "SellerName":    str(r.get("SELLER_NAME","")).strip(),
        })
    return pd.DataFrame(rows)


def df_to_excel(sheets: Dict[str, pd.DataFrame]) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for name, frame in sheets.items():
            frame.to_excel(w, sheet_name=name[:31], index=False)
    return buf.getvalue()


def _apply_bridge_payload(action: str, payload: dict):
    if not isinstance(payload, dict) or not payload:
        return
    fr    = st.session_state.qcr_final_report
    count = 0
    if action == "reject":
        _lkup = _build_flag_lookup()
        for sid, code in payload.items():
            sid  = str(sid)
            code = str(code)
            if code.startswith("Other Reason (Custom):"):
                flag     = "Other Reason (Custom)"
                _entry   = _lkup.get(flag.lower(), {})
                rsn      = code.replace("Other Reason (Custom):","").strip() or "Other Reason"
                rcode    = _entry.get("reason", "1000007 - Other Reason")
                cmt      = rsn
            else:
                flag   = _GRID_REASON_MAP.get(code, code.replace("_"," ").title())
                _entry = _lkup.get(flag.lower(), {})
                rcode  = _entry.get("reason",  f"1000007 - {flag}")
                cmt    = _entry.get("comment", flag)
            fr.loc[fr["ProductSetSid"]==sid,
                   ["Status","Reason","Comment","FLAG"]] = ["Rejected", rcode, cmt, flag]
            count += 1
        if count:
            st.session_state.qcr_exports = {}
            st.session_state.qcr_toasts.append(f"{count} item(s) rejected via visual review.")
    elif action == "undo":
        for sid in payload.keys():
            sid = str(sid)
            fr.loc[fr["ProductSetSid"]==sid, ["Status","Reason","Comment","FLAG"]] = ["Approved","","",""]
            count += 1
        if count:
            st.session_state.qcr_exports = {}
            st.session_state.qcr_toasts.append(f"{count} item(s) reverted to Approved.")

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    _ln  = list(LANGUAGES.keys())
    _cc2 = st.session_state.ui_lang
    _cn  = next((k for k,v in LANGUAGES.items() if v==_cc2), "English")
    _sl  = st.selectbox("Language / Langue / اللغة", _ln, index=_ln.index(_cn))
    if LANGUAGES[_sl] != _cc2:
        st.session_state.ui_lang = LANGUAGES[_sl]; st.rerun()
    st.markdown("---")
    st.header(_t("system_status"))
    _frsb = st.session_state.qcr_final_report
    if not _frsb.empty:
        st.success(f"✓ {int((_frsb['Status']=='Approved').sum()):,} approved", icon="✅")
        st.error(f"✗ {int((_frsb['Status']=='Rejected').sum()):,} rejected", icon="❌")
    else:
        st.info("No file loaded yet.")
    if st.button(_t("clear_cache"), width='stretch', type="secondary"):
        for _k in ["qcr_df","qcr_img_store","qcr_final_report","qcr_all_data",
                   "qcr_zip_hash","qcr_exports","qcr_display_cache"]:
            st.session_state[_k] = (
                {} if "store" in _k or "exports" in _k or "cache" in _k
                else (pd.DataFrame() if "report" in _k or "data" in _k
                      else ("" if "hash" in _k else None))
            )
        st.session_state.qcr_uploader_key  += 1
        st.session_state.qcr_show_review    = False
        st.session_state.qcr_flags_init     = False
        st.rerun()
    st.markdown("---")
    st.header(_t("display_settings"))
    _lo = st.radio("Layout Mode", ["Wide","Centered"],
                   index=0 if st.session_state.layout_mode=="wide" else 1)
    _nm = "wide" if _lo=="Wide" else "centered"
    if _nm != st.session_state.layout_mode:
        st.session_state.layout_mode = _nm; st.rerun()

# ── header ────────────────────────────────────────────────────────────────────
_lh = ""
for _lf in ("jumia logo.png","jumia_logo.png","assets/jumia_logo.png"):
    if os.path.exists(_lf):
        with open(_lf,"rb") as _f:
            _lh = (f'<img src="data:image/png;base64,'
                   f'{base64.b64encode(_f.read()).decode()}" '
                   f'style="height:30px;vertical-align:middle;margin-right:8px;">')
        break

st.markdown(
    f'<h2 style="margin-bottom:4px;">{_lh}QC Results Review</h2>'
    f'<p style="color:#888;font-size:.85rem;margin-top:0;">'
    f'Pre-computed QC — upload ZIP with QC_Results.csv + images/</p>',
    unsafe_allow_html=True,
)

# ── upload + country buttons ──────────────────────────────────────────────────
st.header(f":material/upload_file: {_t('upload_files')}", anchor=False)
_cur = st.session_state.selected_country
_bh  = "".join([
    f'<button onclick="var inp=document.querySelectorAll(\'[data-testid=stTextInput] input\');'
    f'var b=Array.from(inp).find(function(e){{return e.placeholder===\'__QCR_COUNTRY__\';}});'
    f'if(b){{b.value=\'{c}\';b.dispatchEvent(new Event(\'input\',{{bubbles:true}}));'
    f'b.dispatchEvent(new Event(\'change\',{{bubbles:true}}));}}" '
    f'style="padding:6px 16px;margin-right:6px;border-radius:20px;cursor:pointer;font-size:.85rem;'
    f'border:{"2px solid "+O if c==_cur else "1px solid #ddd"};'
    f'background:{"#fff7ee" if c==_cur else "#fff"};color:#000;'
    f'font-weight:{"700" if c==_cur else "400"};">'
    f'{FLAG_EMOJIS[c]} {c}</button>'
    for c in COUNTRIES
])
components.html(f"<div style='margin-bottom:4px;'>{_bh}</div>", height=52, scrolling=False)

_cbk = f"qcr_cb_{st.session_state.qcr_bridge_counter}"
_cbv = st.text_input("b", value="", placeholder="__QCR_COUNTRY__",
                     key=_cbk, label_visibility="collapsed")
if _cbv.strip() in COUNTRIES and _cbv.strip() != _cur:
    st.session_state.selected_country   = _cbv.strip()
    st.session_state.qcr_bridge_counter += 1
    st.rerun()

if st.session_state.qcr_df is not None:
    if st.button("✕  Clear all files", type="secondary"):
        for _k in ["qcr_df","qcr_img_store","qcr_final_report","qcr_all_data",
                   "qcr_zip_hash","qcr_exports","qcr_display_cache"]:
            st.session_state[_k] = (
                {} if "store" in _k or "exports" in _k or "cache" in _k
                else (pd.DataFrame() if "report" in _k or "data" in _k
                      else ("" if "hash" in _k else None))
            )
        st.session_state.qcr_uploader_key  += 1
        st.session_state.qcr_show_review    = False
        st.session_state.qcr_flags_init     = False
        st.rerun()

_up = st.file_uploader(
    "Upload ZIP", type=["zip"], accept_multiple_files=False,
    key=f"qcr_zip_{st.session_state.qcr_uploader_key}",
    label_visibility="collapsed",
    help="ZIP must contain QC_Results.csv + images/",
)
if _up is not None:
    _raw = _up.read()
    _h   = hashlib.md5(_raw).hexdigest()
    if _h != st.session_state.qcr_zip_hash:
        with st.status("Extracting ZIP…", expanded=True) as _s:
            st.write("📂 Reading CSV and images…")
            _dfr, _ims = load_zip(_raw)
            if _dfr is not None and not _dfr.empty:
                st.write("🔄 Mapping columns…")
                _dfm = map_columns(_dfr)
                st.write("📊 Building report…")
                _rep = derive_report(_dfm)
                st.session_state.qcr_df            = _dfm
                st.session_state.qcr_img_store     = _ims
                st.session_state.qcr_final_report  = _rep
                st.session_state.qcr_all_data      = _dfm
                st.session_state.qcr_zip_hash      = _h
                st.session_state.qcr_exports       = {}
                st.session_state.qcr_display_cache = {}
                st.session_state.qcr_show_review   = False
                st.session_state.qcr_flags_init    = False
                st.session_state.grid_page         = 0
                if "ACTIVE_STATUS_COUNTRY" in _dfm.columns:
                    _cr = _dfm["ACTIVE_STATUS_COUNTRY"].dropna()
                    if not _cr.empty:
                        _det = COUNTRY_CODE_MAP.get(str(_cr.iloc[0]).strip().lower())
                        if _det:
                            st.session_state.selected_country   = _det
                            st.session_state.qcr_bridge_counter += 1
                _an = int((_rep["Status"]=="Approved").sum())
                _rn = int((_rep["Status"]=="Rejected").sum())
                _s.update(label=f"Done — {_an:,} approved, {_rn:,} rejected",
                           state="complete", expanded=False)
            else:
                _s.update(label="Could not find QC_Results.csv in ZIP", state="error")
                st.error("Ensure QC_Results.csv is at the ZIP root.")

_fr   = st.session_state.qcr_final_report
_df   = st.session_state.qcr_df
_imgs = st.session_state.qcr_img_store

if not _fr.empty:
    _a = int((_fr["Status"]=="Approved").sum())
    _r = int((_fr["Status"]=="Rejected").sum())
    st.markdown(
        f'<div class="status-done">✓ Done — '
        f'<strong>{_a:,} approved</strong>, <strong>{_r:,} rejected</strong></div>',
        unsafe_allow_html=True,
    )

if _fr.empty or _df is None:
    st.info("👆 Upload a ZIP file containing **QC_Results.csv** and an **images/** folder.", icon="📦")
    st.stop()

_adf = _fr[_fr["Status"]=="Approved"]
_rdf = _fr[_fr["Status"]=="Rejected"]
_tot = len(_fr)

# ── metrics ───────────────────────────────────────────────────────────────────
st.header(f":material/bar_chart: {_t('val_results')}", anchor=False)
with st.container(border=True):
    _c1,_c2,_c3,_c4,_c5 = st.columns(5)
    for _col,_lbl,_val,_clr in [
        (_c1,"Total Products",f"{_tot:,}",                                    DG),
        (_c2,_t("approved"),  f"{len(_adf):,}",                               G),
        (_c3,_t("rejected"),  f"{len(_rdf):,}",                               R),
        (_c4,_t("rej_rate"),  f"{len(_rdf)/_tot*100:.1f}%" if _tot else "0%", O),
        (_c5,_t("common_skus"),"0",                                           MG),
    ]:
        with _col:
            st.markdown(f'<div style="border-top:3px solid {_clr};border-radius:6px;padding-top:4px;"></div>', unsafe_allow_html=True)
            st.metric(_lbl, _val)


# ── dashboard ─────────────────────────────────────────────────────────────────
def _render_dashboard(fr: pd.DataFrame, df: pd.DataFrame):
    _tot_d  = len(fr)
    _app_d  = int((fr["Status"]=="Approved").sum())
    _rej_d  = int((fr["Status"]=="Rejected").sum())
    _rate_d = _rej_d / max(_tot_d,1) * 100
    _rej_df = fr[fr["Status"]=="Rejected"].copy()

    # KPIs
    st.markdown("##### 📊 Session Overview")
    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Total Products", f"{_tot_d:,}")
    k2.metric("Approved",       f"{_app_d:,}",  delta=f"{_app_d/_tot_d*100:.1f}%")
    k3.metric("Rejected",       f"{_rej_d:,}",  delta=f"-{_rate_d:.1f}%", delta_color="inverse")
    k4.metric("Rejection Rate", f"{_rate_d:.1f}%")
    k5.metric("Unique Flags",   str(fr[fr["Status"]=="Rejected"]["FLAG"].nunique()))
    st.markdown("---")

    if not _PLOTLY_OK:
        st.warning("Install plotly: `pip install plotly`")
        return

    # row 1 — donut + flag bar
    col1,col2 = st.columns([1,1.6], gap="large")
    with col1:
        st.markdown("**Outcome Distribution**")
        _fig_d = go.Figure(go.Pie(
            labels=["Approved","Rejected"], values=[_app_d,_rej_d],
            hole=0.62, marker_colors=[G,R], textinfo="percent",
            hovertemplate="%{label}: %{value} (%{percent})<extra></extra>",
        ))
        _fig_d.update_layout(
            margin=dict(t=10,b=30,l=10,r=10), height=280,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            showlegend=True,
            legend=dict(orientation="h",yanchor="bottom",y=-0.2,xanchor="center",x=0.5),
            annotations=[dict(text=f"<b>{_rate_d:.1f}%</b><br>rejected",
                              x=0.5,y=0.5,font_size=15,showarrow=False,font_color=R)],
        )
        st.plotly_chart(_fig_d, width='stretch', config={"displayModeBar":False})

    with col2:
        st.markdown("**Rejections by Flag Type**")
        _fvc = _rej_df["FLAG"].value_counts().reset_index()
        _fvc.columns = ["Flag","Count"]
        _fvc = _fvc.sort_values("Count",ascending=True).tail(10)
        _fig_b = go.Figure(go.Bar(
            x=_fvc["Count"],y=_fvc["Flag"],orientation="h",
            marker_color=O,marker_line_width=0,
            text=_fvc["Count"],textposition="outside",
            hovertemplate="%{y}: %{x}<extra></extra>",
        ))
        _fig_b.update_layout(
            margin=dict(t=10,b=10,l=10,r=60), height=280,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=True,gridcolor="#f0f0f0",zeroline=False),
            yaxis=dict(showgrid=False), font=dict(size=11),
        )
        st.plotly_chart(_fig_b, width='stretch', config={"displayModeBar":False})

    # row 2 — seller stacked bar + category bar
    col3,col4 = st.columns(2, gap="large")
    with col3:
        st.markdown("**Top Sellers — Approved vs Rejected**")
        if "SELLER_NAME" in df.columns:
            _ss = df[["PRODUCT_SET_SID","SELLER_NAME"]].drop_duplicates()
            _mg = fr.merge(_ss,left_on="ProductSetSid",right_on="PRODUCT_SET_SID",how="left")
            _sv_r = _mg[_mg["Status"]=="Rejected"]["SELLER_NAME"].value_counts().head(10)
            _sv_a = _mg[_mg["Status"]=="Approved"]["SELLER_NAME"].value_counts()
            _sv   = _sv_r.reset_index(); _sv.columns = ["Seller","Rejected"]
            _sv["Approved"] = _sv["Seller"].map(_sv_a).fillna(0).astype(int)
            _sv = _sv.sort_values("Rejected",ascending=True)
            _fig_s = go.Figure()
            _fig_s.add_trace(go.Bar(y=_sv["Seller"],x=_sv["Rejected"],name="Rejected",orientation="h",marker_color=R,marker_line_width=0))
            _fig_s.add_trace(go.Bar(y=_sv["Seller"],x=_sv["Approved"],name="Approved",orientation="h",marker_color=G,marker_line_width=0))
            _fig_s.update_layout(
                barmode="stack",margin=dict(t=10,b=30,l=10,r=10),height=340,
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True,gridcolor="#f0f0f0"),yaxis=dict(showgrid=False),
                legend=dict(orientation="h",yanchor="bottom",y=-0.18,xanchor="center",x=0.5),font=dict(size=10),
            )
            st.plotly_chart(_fig_s,width='stretch',config={"displayModeBar":False})
        else:
            st.info("No SELLER_NAME column.")

    with col4:
        st.markdown("**Top Rejected Categories**")
        if "CATEGORY" in df.columns:
            _sc2 = df[["PRODUCT_SET_SID","CATEGORY"]].drop_duplicates()
            _mc2 = _rej_df.merge(_sc2,left_on="ProductSetSid",right_on="PRODUCT_SET_SID",how="left")
            _cv  = _mc2["CATEGORY"].value_counts().head(12).reset_index()
            _cv.columns = ["Category","Count"]
            _cv["Category"] = _cv["Category"].apply(lambda c:(c.split("/")[-1].strip() if "/" in str(c) else str(c))[:32])
            _cv = _cv.sort_values("Count",ascending=True)
            _pal = ["#F68B1E","#FF9933","#FFB347","#FFD700","#FFA07A","#FF8C00","#FF6347","#FF4500","#E73C17","#CC3300","#B22222","#8B0000"]
            _fig_c = go.Figure(go.Bar(
                y=_cv["Category"],x=_cv["Count"],orientation="h",
                marker_color=_pal[:len(_cv)],marker_line_width=0,
                text=_cv["Count"],textposition="outside",
                hovertemplate="%{y}: %{x}<extra></extra>",
            ))
            _fig_c.update_layout(
                margin=dict(t=10,b=10,l=10,r=50),height=340,
                paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True,gridcolor="#f0f0f0",zeroline=False),
                yaxis=dict(showgrid=False),font=dict(size=10),
            )
            st.plotly_chart(_fig_c,width='stretch',config={"displayModeBar":False})
        else:
            st.info("No CATEGORY column.")

    st.markdown("---")

    # row 3 — rejection reasons table
    st.markdown("**Top Rejection Reasons**")
    _rr = (
        _rej_df.groupby(["FLAG","Comment"]).size()
        .reset_index(name="Count").sort_values("Count",ascending=False)
        .head(15).reset_index(drop=True)
    )
    _rr.index += 1
    _rr["% of Rejected"] = (_rr["Count"]/max(_rej_d,1)*100).round(1).astype(str)+"%"
    _rr.columns = ["Check / Flag","Rejection Reason","Count","% of Rejected"]
    st.dataframe(_rr, width='stretch', hide_index=False,
                 column_config={"Count":st.column_config.NumberColumn(format="%d"),
                                "Check / Flag":st.column_config.TextColumn(width="medium"),
                                "Rejection Reason":st.column_config.TextColumn(width="large")})

    # row 4 — heatmap
    if "SELLER_NAME" in df.columns and not _rej_df.empty:
        st.markdown("---")
        st.markdown("**Rejection Heatmap — Top Sellers × Flag**")
        _hm = _rej_df.merge(df[["PRODUCT_SET_SID","SELLER_NAME"]].drop_duplicates(),
                            left_on="ProductSetSid",right_on="PRODUCT_SET_SID",how="left")
        _ts2 = _hm["SELLER_NAME"].value_counts().head(12).index.tolist()
        _tf  = _hm["FLAG"].value_counts().head(8).index.tolist()
        _hp = (_hm[_hm["SELLER_NAME"].isin(_ts2) & _hm["FLAG"].isin(_tf)]
                .pivot_table(index="SELLER_NAME", columns="FLAG", aggfunc="size", fill_value=0)
                .reindex(index=_ts2, columns=_tf, fill_value=0))
        _fig_h = go.Figure(go.Heatmap(
            z=_hp.values,
            x=[f[:24] for f in _hp.columns.tolist()],
            y=[s[:30] for s in _hp.index.tolist()],
            colorscale=[[0,"#fff7ee"],[0.5,O],[1,"#7a2800"]],
            text=_hp.values, texttemplate="%{text}",
            hovertemplate="Seller: %{y}<br>Flag: %{x}<br>Count: %{z}<extra></extra>",
            showscale=True,
        ))
        _fig_h.update_layout(
            margin=dict(t=20,b=60,l=10,r=10),height=400,
            paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(tickangle=-30,tickfont=dict(size=10)),
            yaxis=dict(tickfont=dict(size=10)),
        )
        st.plotly_chart(_fig_h,width='stretch',config={"displayModeBar":False})

    # row 5 — treemap
    if not _rej_df.empty:
        st.markdown("---")
        st.markdown("**Flag Distribution Treemap**")
        _td = _rej_df["FLAG"].value_counts().reset_index()
        _td.columns = ["Flag","Count"]
        _td["Pct"] = (_td["Count"]/max(_rej_d,1)*100).round(1)
        _fig_t = px.treemap(_td,path=["Flag"],values="Count",color="Count",
                            color_continuous_scale=[[0,"#fff7ee"],[0.5,O],[1,R]],
                            custom_data=["Pct"])
        _fig_t.update_traces(
            texttemplate="<b>%{label}</b><br>%{value} (%{customdata[0]:.1f}%)",
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Share: %{customdata[0]:.1f}%<extra></extra>",
        )
        _fig_t.update_layout(margin=dict(t=10,b=10,l=10,r=10),height=340,
                              paper_bgcolor="rgba(0,0,0,0)",coloraxis_showscale=False)
        st.plotly_chart(_fig_t,width='stretch',config={"displayModeBar":False})


with st.expander(
    f":material/dashboard: **QC Dashboard** — "
    f"{int((_fr['Status']=='Rejected').sum()):,} rejected · "
    f"{int((_fr['Status']=='Approved').sum()):,} approved · "
    f"{len(_fr):,} total",
    expanded=False,
):
    _render_dashboard(_fr, _df)

# ── flags breakdown ───────────────────────────────────────────────────────────
st.subheader(f":material/flag: {_t('flags_breakdown')}", anchor=False)
_rej_live = set(
    st.session_state.qcr_final_report[
        st.session_state.qcr_final_report["Status"]=="Rejected"
    ]["ProductSetSid"].astype(str)
)

if _rdf.empty:
    st.success("🎉 All products passed — no rejections found.")
else:
    if not st.session_state.qcr_flags_init and not _rdf.empty:
        st.session_state[f"qcr_exp_{_rdf['FLAG'].value_counts().index[0]}"] = True
        st.session_state.qcr_flags_init = True

    for _fn, _sc, _rc, _rcode, _alias in FLAG_MAP:
        if _sc not in _df.columns: continue
        _fl = _df[
            (_df[_sc].astype(str).str.strip().str.lower()=="rejected") &
            (_df["PRODUCT_SET_SID"].astype(str).isin(_rej_live))
        ]
        if _fl.empty: continue

        with st.expander(f"**{_fn}** ({len(_fl)})", expanded=False, key=f"qcr_exp_{_fn}"):
            _s1,_s2 = st.columns([2,2], gap="medium")
            with _s1:
                _srch = st.text_input(
                    "Search", placeholder="Name, Brand…", icon=":material/search:",
                    key=f"qcrs_{_fn}", label_visibility="collapsed",
                )
            with _s2:
                _so = sorted(_fl["SELLER_NAME"].dropna().astype(str).unique()) if "SELLER_NAME" in _fl.columns else []
                _ss = st.multiselect("Filter by Seller", _so, key=f"qcrf_{_fn}",
                                     label_visibility="collapsed")

            _wc = FLAG_COLS.get(_fn, ["PRODUCT_SET_SID","NAME","BRAND","CATEGORY","SELLER_NAME","PARENTSKU",_rc])
            _av = [c for c in _wc if c in _fl.columns]
            if _rc in _fl.columns and _rc not in _av: _av.append(_rc)
            _sh = _fl[_av].copy().reset_index(drop=True)
            _sh.columns = [COL_LABELS.get(c,c) for c in _sh.columns]

            if _srch.strip():
                _mk = pd.Series(False, index=_sh.index)
                for _c in ["Product Name","Brand","Seller"]:
                    if _c in _sh.columns:
                        _mk |= _sh[_c].astype(str).str.contains(_srch.strip(), case=False, na=False)
                _sh = _sh[_mk]
            if _ss and "Seller" in _sh.columns:
                _sh = _sh[_sh["Seller"].isin(_ss)]

            _ev = st.dataframe(
                _sh, hide_index=True, width='stretch',
                selection_mode="multi-row", on_select="rerun",
                column_config={
                    "Product Set SID":  st.column_config.TextColumn(pinned=True),
                    "Product Name":     st.column_config.TextColumn(pinned=True),
                    "Sale Price (USD)":  st.column_config.NumberColumn(format="$%.2f"),
                    "Full Category":    st.column_config.TextColumn(width="large"),
                },
                key=f"qcrd_{_fn}",
            )
            _si = [i for i in _ev.selection.rows if i < len(_sh)]
            _hs = len(_si) > 0
            st.markdown(
                f'<span class="sel-badge">✔ {len(_si)} / {len(_sh)} selected</span>',
                unsafe_allow_html=True,
            )
            _b1,_b2 = st.columns(2)
            with _b1:
                if st.button(_t("approve_btn"), key=f"qcra_{_fn}", type="primary",
                             width='stretch', disabled=not _hs):
                    if "Product Set SID" in _sh.columns:
                        _ids = _sh.iloc[_si]["Product Set SID"].tolist()
                        st.session_state.qcr_final_report.loc[
                            st.session_state.qcr_final_report["ProductSetSid"].isin(_ids),
                            ["Status","Reason","Comment","FLAG"]
                        ] = ["Approved","","","Approved by User"]
                        st.session_state.qcr_exports = {}
                        st.session_state.qcr_toasts.append(f"{len(_ids)} items approved.")
                        st.rerun()
            with _b2:
                with st.popover(_t("reject_as"), width='stretch',
                                disabled=not _hs, key=f"qcrp_{_fn}"):
                    _cr = st.selectbox("Reason", _RLIST, key=f"qcrr_{_fn}",
                                       label_visibility="collapsed")
                    if st.button("Apply", key=f"qcrap_{_fn}", type="primary", width='stretch'):
                        if "Product Set SID" in _sh.columns:
                            _ids = _sh.iloc[_si]["Product Set SID"].tolist()
                            _lkup_e  = _build_flag_lookup().get(_cr.lower(), {})
                            _rsn_c   = _lkup_e.get("reason",  f"1000007 - {_cr}")
                            _cmt_c   = _lkup_e.get("comment", _cr)
                            st.session_state.qcr_final_report.loc[
                                st.session_state.qcr_final_report["ProductSetSid"].isin(_ids),
                                ["Status","Reason","Comment","FLAG"]
                            ] = ["Rejected", _rsn_c, _cmt_c, _cr]
                            st.session_state.qcr_exports = {}
                            st.session_state.qcr_toasts.append(f"{len(_ids)} rejected as '{_cr}'.")
                            st.rerun()

            st.markdown("<br>", unsafe_allow_html=True)
            if st.toggle("🖼️  Show Images", value=False, key=f"qcri_{_fn}"):
                _img_count = min(len(_fl), 48)
                for _cs in range(0, _img_count, 5):
                    _chunk = _fl.iloc[_cs:_cs+5]
                    _gcols = st.columns(5)
                    for _gc, (_, _row) in zip(_gcols, _chunk.iterrows()):
                        with _gc:
                            with st.container(border=True):
                                _rfn  = str(_row.get("image_filename", _row.get("MAIN_IMAGE",""))).strip()
                                _rraw = img_to_raw(_rfn, _imgs)
                                if _rraw:
                                    st.image(_rraw, width='stretch')
                                else:
                                    st.markdown(
                                        '<div style="height:130px;background:#f5f5f5;border-radius:6px;'
                                        'display:flex;align-items:center;justify-content:center;'
                                        'color:#ccc;font-size:2rem;">🖼️</div>',
                                        unsafe_allow_html=True,
                                    )
                                _rnm  = str(_row.get("NAME",""))
                                _rsid = str(_row.get("PRODUCT_SET_SID",""))
                                _rrs  = str(_row.get(_rc,"")) if _rc in _row.index else ""
                                _rrs  = "" if _rrs.strip().lower() in ("nan","none","") else _rrs.strip()
                                st.markdown(
                                    f'<div style="font-size:0.85rem; font-weight:700; height:40px; overflow:hidden; text-overflow:ellipsis;" title="{_rnm}">{_rnm}</div>'
                                    f'<div style="font-size:0.75rem; color:#666; margin-bottom:10px;">SID: {_rsid}</div>',
                                    unsafe_allow_html=True,
                                )
                                if _rrs:
                                    st.error(f"{_rrs}", icon=":material/error:")
                if len(_fl) > 48:
                    st.info(f"Showing first 48 of {len(_fl)} images.")


# ── visual review ─────────────────────────────────────────────────────────────
st.markdown("---")
_vc1,_vc2 = st.columns([3,1], gap="medium")
with _vc1:
    st.header(f":material/pageview: {_t('manual_review')}", anchor=False)
    st.caption("Open Focus Mode to visually review Approved products and reject as needed.")
with _vc2:
    if st.button("Start Visual Review", type="primary",
                 icon=":material/pageview:", width='stretch'):
        st.session_state.qcr_show_review = True
        st.session_state.grid_page       = 0


@st.dialog("Visual Review Mode", width="large", icon=":material/pageview:", dismissible=False)
def _vr_dialog():
    _lfr = st.session_state.qcr_final_report
    _ldf = st.session_state.qcr_df
    _lim = st.session_state.qcr_img_store

    _app_sids = set(_lfr[_lfr["Status"]=="Approved"]["ProductSetSid"].astype(str))
    _ag  = [c for c in GRID_COLS if c in _ldf.columns]
    _rd  = _ldf[_ldf["PRODUCT_SET_SID"].astype(str).isin(_app_sids)][_ag].copy()

    # ── NaN-safe filename resolver ────────────────────────────────────────
    def _safe_fn(row: pd.Series) -> str:
        sid = str(row.get("PRODUCT_SET_SID","")).strip()
        for col in ("image_filename","MAIN_IMAGE"):
            v = row.get(col)
            try:
                if v is not None and pd.notna(v):
                    s = str(v).strip()
                    if s.lower() not in ("","nan","none","null"):
                        return s
            except (TypeError, ValueError):
                pass
        # SID-based fallback for local ZIP images
        if sid and _lim:
            sid_l = sid.lower()
            for k in _lim:
                if Path(k).stem.lower() == sid_l:
                    return Path(k).name
        return ""

    # ── HTTPS URL pass-through; local → data URI ──────────────────────────
    def _resolve_img(fn: str) -> str:
        if not fn:
            return ""
        fn_clean = fn.replace("http://","https://",1)
        if fn_clean.startswith("https://"):
            return fn_clean          # let browser fetch directly
        return img_to_uri(fn, _lim, max_px=160)

    # ── top controls ──────────────────────────────────────────────────────
    c1,c2,c3,c4 = st.columns([1.5,1.5,1.5,0.8], gap="large", vertical_alignment="bottom")
    with c1:
        _sn  = st.text_input("Search by Name", placeholder="Product name…", icon=":material/search:")
    with c2:
        _ssc = st.text_input("Search by Seller/Category", placeholder="Seller or Category…", icon=":material/store:")
    with c3:
        _cur_ipp = st.session_state.get("grid_items_per_page", 50)
        if _cur_ipp not in _IPP_OPTIONS:
            _cur_ipp = min(_IPP_OPTIONS, key=lambda x: abs(x - _cur_ipp))
        st.session_state.grid_items_per_page = st.select_slider(
            "Items per page", options=_IPP_OPTIONS, value=_cur_ipp,
            help="Lower = faster image loading",
        )
    with c4:
        if st.button("Close", width='stretch', type="secondary"):
            st.session_state.qcr_show_review = False
            st.rerun()

    # ── filters ───────────────────────────────────────────────────────────
    if _sn:
        _rd = _rd[_rd["NAME"].astype(str).str.contains(_sn, case=False, na=False)]
    if _ssc:
        _mc = (
            _rd["CATEGORY"].astype(str).str.contains(_ssc, case=False, na=False)
            if "CATEGORY" in _rd.columns
            else pd.Series(False, index=_rd.index)
        )
        _ms = _rd["SELLER_NAME"].astype(str).str.contains(_ssc, case=False, na=False)
        _rd = _rd[_mc | _ms]

    # ── pagination ────────────────────────────────────────────────────────
    _ipp  = st.session_state.grid_items_per_page
    _tpgs = max(1,(len(_rd)+_ipp-1)//_ipp)
    _pg   = min(st.session_state.grid_page, _tpgs-1)
    st.session_state.grid_page = _pg

    pg1,pg2,pg3 = st.columns([1,2,1], vertical_alignment="center", gap="small")
    with pg1:
        if st.button("Prev Page", key="qvr_prev", width='stretch',
                     icon=":material/arrow_back:", disabled=(_pg==0)):
            st.session_state.grid_page = _pg-1
            st.session_state.do_scroll_top = True; st.rerun()
    with pg2:
        _npg = st.number_input(
            f"Jump to Page (Total: {_tpgs} | {len(_rd)} items)",
            min_value=1, max_value=max(1,_tpgs), value=_pg+1, step=1, key="qvr_jump",
        )
        if _npg-1 != _pg:
            st.session_state.grid_page = _npg-1
            st.session_state.do_scroll_top = True; st.rerun()
    with pg3:
        if st.button("Next Page", key="qvr_next", width='stretch',
                     icon=":material/arrow_forward:", disabled=(_pg>=_tpgs-1)):
            st.session_state.grid_page = _pg+1
            st.session_state.do_scroll_top = True; st.rerun()

    # ── page slice ────────────────────────────────────────────────────────
    _ps    = _pg * _ipp
    _pd    = _rd.iloc[_ps:_ps+_ipp].copy()
    _total = len(_pd)

    # ── diagnostic strip ─────────────────────────────────────────────────
    if _total > 0:
        _, _dbr  = next(iter(_pd.iterrows()))
        _db_fn   = _safe_fn(_dbr)
        _db_res  = _resolve_img(_db_fn)
        _db_ok   = bool(_db_res)
        _db_col  = "green" if _db_ok else "#c0392b"
        _db_type = ("URL" if (_db_res or "").startswith("https")
                    else "data URI" if (_db_res or "").startswith("data:")
                    else "none")
        st.markdown(
            f"<div style='background:#f8f8f8;border:1px solid #ddd;border-radius:6px;"
            f"padding:6px 12px;font-size:.76rem;margin-bottom:6px;'>"
            f"🔍 <b>Debug</b> &nbsp;|&nbsp; "
            f"MAIN_IMAGE: <code>{str(_dbr.get('MAIN_IMAGE',''))[:80] or '(empty)'}</code>"
            f" &nbsp;|&nbsp; Type: <b style='color:{_db_col}'>{_db_type}</b>"
            f" &nbsp;|&nbsp; Resolved: <b style='color:{_db_col}'>{'✓' if _db_ok else '✗'}</b>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # ── resolve images ────────────────────────────────────────────────────
    _uris  = []
    _prog  = st.progress(0, text=f"🖼️ Resolving images… 0 / {_total}")
    for _i, (_, _row) in enumerate(_pd.iterrows()):
        _uris.append(_resolve_img(_safe_fn(_row)))
        _prog.progress((_i+1)/max(_total,1), text=f"🖼️ Resolving images… {_i+1} / {_total}")
    _prog.empty()

    _loaded = sum(1 for u in _uris if u)
    st.caption(f"✅ {_loaded} / {_total} images resolved for this page")

    _pd = _pd.copy()
    _pd["MAIN_IMAGE"] = _uris

    # ── rejected-state & warnings ─────────────────────────────────────────
    _rst = {}
    _pwr = {}
    for _sid in _pd["PRODUCT_SET_SID"].astype(str):
        _rrow = _lfr[_lfr["ProductSetSid"]==_sid]
        if not _rrow.empty:
            _status = _rrow["Status"].iloc[0]
            _flag = str(_rrow["FLAG"].iloc[0])
            
            if _status == "Rejected":
                _comment = str(_rrow["Comment"].iloc[0])
                # Special handling for Brand Image Check to show clean reason
                if "Brand Image Check" in _flag or _comment == "Brand Image Check":
                    _rst[_sid] = "Brand Image Check"
                else:
                    _rst[_sid] = _flag or "Poor images"
            
            if _flag == "Manual review":
                _pwr[_sid] = ["Manual review"]

    _cpr = 3 if st.session_state.get("layout_mode")=="centered" else 4

    # ── fast HTML grid ────────────────────────────────────────────────────
    _go = False
    if _GRID_OK:
        try:
            _gh = build_fast_grid_html(
                page_data=_pd,
                flags_mapping=_FLAGS_MAPPING,
                country=st.session_state.get("selected_country","Kenya"),
                page_warnings=_pwr,
                rejected_state=_rst,
                cols_per_row=_cpr,
                poor_img_sids=set(),
                prefetch_urls=[],
                scroll_to_top=st.session_state.get("do_scroll_top",False),
            )
            st.session_state.do_scroll_top = False
            components.html(_gh, height=750, scrolling=True)
            _go = True
        except Exception as _ge:
            st.error(f"Grid render error: {_ge}")

    # ── native fallback ───────────────────────────────────────────────────
    if not _go:
        for _gcs in range(0, len(_pd), _cpr):
            _gch = _pd.iloc[_gcs:_gcs+_cpr]
            _gcl = st.columns(_cpr)
            for _gcc, (_, _gr) in zip(_gcl, _gch.iterrows()):
                with _gcc:
                    _gs   = str(_gr.get("PRODUCT_SET_SID",""))
                    _gfn  = _safe_fn(_gr)
                    _gres = _resolve_img(_gfn)

                    # HTTPS → HTML img tag
                    if _gres.startswith("https://"):
                        st.markdown(
                            f'<img src="{_gres}" referrerpolicy="no-referrer" '
                            f'style="width:100%;border-radius:6px;max-height:180px;object-fit:contain;">',
                            unsafe_allow_html=True,
                        )
                    # data URI → st.image
                    elif _gres.startswith("data:"):
                        st.image(_gres, width='stretch')
                    # placeholder
                    else:
                        st.markdown(
                            '<div style="height:120px;background:#f5f5f5;border-radius:6px;'
                            'display:flex;align-items:center;justify-content:center;'
                            'color:#ccc;font-size:2rem;">🖼️</div>',
                            unsafe_allow_html=True,
                        )

                    _gst2 = _rst.get(_gs)
                    st.markdown(
                        f'<div class="vr-name">{str(_gr.get("NAME",""))[:65]}</div>'
                        f'<div class="vr-meta">{str(_gr.get("BRAND",""))} · '
                        f'<span class="{"badge-rej" if _gst2 else "badge-app"}">'
                        f'{"Rejected" if _gst2 else "Approved"}</span></div>',
                        unsafe_allow_html=True,
                    )
                    if _gst2:
                        if st.button("↺ Undo", key=f"qvr_u_{_gs}",
                                     width='stretch', type="secondary"):
                            st.session_state.qcr_final_report.loc[
                                st.session_state.qcr_final_report["ProductSetSid"]==_gs,
                                ["Status","Reason","Comment","FLAG"]
                            ] = ["Approved","","",""]
                            st.session_state.qcr_exports = {}
                            st.session_state.qcr_toasts.append("Reverted to Approved.")
                            st.rerun()
                    else:
                        with st.popover("✘ Reject", width='stretch',
                                        key=f"qvr_rp_{_gs}"):
                            _grr = st.selectbox("Reason", _RLIST,
                                                key=f"qvr_rs_{_gs}",
                                                label_visibility="collapsed")
                            if st.button("Apply", key=f"qvr_ra_{_gs}", type="primary", width='stretch'):
                                _lkup_e  = _build_flag_lookup().get(_grr.lower(), {})
                                _rsn_g   = _lkup_e.get("reason",  f"1000007 - {_grr}")
                                _cmt_g   = _lkup_e.get("comment", _grr)
                                st.session_state.qcr_final_report.loc[
                                    st.session_state.qcr_final_report["ProductSetSid"]==_gs,
                                    ["Status","Reason","Comment","FLAG"]
                                ] = ["Rejected", _rsn_g, _cmt_g, _grr]
                                st.session_state.qcr_exports = {}
                                st.session_state.qcr_toasts.append(f"Rejected as '{_grr}'.")
                                st.rerun()

    # ── JS→Python bridge ─────────────────────────────────────────────────
    st.markdown("---")
    _bk = f"qcr_jtb_{st.session_state.qcr_bridge_key}"
    _bv = st.text_input("jtb", value="", placeholder="JTBRIDGE_UNIQUE_DO_NOT_USE",
                         key=_bk, label_visibility="collapsed")
    if _bv.strip():
        try:
            _p   = json.loads(_bv.strip())
            _act = str(_p.get("action",""))
            _pay = _p.get("payload",{})
            if isinstance(_pay, dict) and _pay:
                _apply_bridge_payload(_act, _pay)
            elif _p.get("sid"):
                _sid_l = str(_p.get("sid",""))
                if _act == "reject":
                    _apply_bridge_payload("reject", {_sid_l: str(_p.get("reason","Poor images"))})
                elif _act == "undo":
                    _apply_bridge_payload("undo", {_sid_l: True})
        except Exception:
            pass
        st.session_state.qcr_bridge_key += 1
        st.rerun()

    # ── bottom pagination ─────────────────────────────────────────────────
    pb1,pb2,pb3,pb4 = st.columns([1,2,1,1], vertical_alignment="center", gap="small")
    with pb1:
        if st.button("Prev Page", key="qvr_prev_b", width='stretch',
                     icon=":material/arrow_back:", disabled=(_pg==0)):
            st.session_state.grid_page = _pg-1; st.rerun()
    with pb2:
        _nb = st.number_input(
            f"Jump to Page (Total: {_tpgs} | {len(_rd)} items)",
            min_value=1, max_value=max(1,_tpgs), value=_pg+1, step=1, key="qvr_jump_b",
        )
        if _nb-1 != _pg:
            st.session_state.grid_page = _nb-1; st.rerun()
    with pb3:
        if st.button("Next Page", key="qvr_next_b", width='stretch',
                     icon=":material/arrow_forward:", disabled=(_pg>=_tpgs-1)):
            st.session_state.grid_page = _pg+1; st.rerun()
    with pb4:
        if st.button("Close Review", key="qvr_close_b",
                     width='stretch', type="secondary"):
            st.session_state.qcr_show_review = False; st.rerun()


if st.session_state.qcr_show_review:
    _vr_dialog()


# ── exports ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.header(f":material/download: {_t('download_reports')}", anchor=False)
st.caption("Export validation results in Excel format")

_lfr2 = st.session_state.qcr_final_report
_app3 = _lfr2[_lfr2["Status"]=="Approved"]
_rej3 = _lfr2[_lfr2["Status"]=="Rejected"]
_ts   = datetime.now().strftime("%Y%m%d_%H%M")
_cc3  = st.session_state.selected_country[:2].upper()

def _bex(name: str) -> bytes:
    if name in st.session_state.qcr_exports:
        return st.session_state.qcr_exports[name]

    _live = st.session_state.qcr_final_report.copy()
    _full = st.session_state.qcr_all_data.copy()
    
    # ── ProductSets sheet — exact column names matching real PIM export ───
    _product_sets = pd.DataFrame({
        "productsetsid": _live["ProductSetSid"],
        "parentsku":     _live["ParentSKU"],
        "status":        _live["Status"],
        "reason":        _live["Reason"],
        "comment":       _live["Comment"],
        "flag":          _live["FLAG"],
        "sellername":    _live["SellerName"],
    })
    _ps_rej = _product_sets[_product_sets["status"]=="Rejected"].reset_index(drop=True)
    _ps_app = _product_sets[_product_sets["status"]=="Approved"].reset_index(drop=True)
    
    # ── Rejection Reasons sheet — live from reasons.xlsx ─────────────────
    _rej_reasons = _load_rejection_reasons()
    
    # ── Full Data sheet — original CSV with QC columns appended ──────────
    _idx = _live.set_index("ProductSetSid")
    _full["QC_Status"] = _idx["Status"].reindex(_full["PRODUCT_SET_SID"].astype(str)).values
    _full["QC_Flag"]   = _idx["FLAG"].reindex(_full["PRODUCT_SET_SID"].astype(str)).values
    _full["QC_Reason"] = _idx["Reason"].reindex(_full["PRODUCT_SET_SID"].astype(str)).values
    
    # ── Summary + Flags sheets ────────────────────────────────────────────
    _tot_s = len(_live)
    _rej_s = int((_live["Status"]=="Rejected").sum())
    _app_s = int((_live["Status"]=="Approved").sum())
    
    _flag_series = _live[_live["Status"]=="Rejected"]["FLAG"].value_counts()
    _flag_counts = pd.DataFrame({
        "Flag / Check":   _flag_series.index.tolist(),
        "Rejected Count": _flag_series.values.tolist(),
    })
    _flag_counts["% of Rejected"] = (
        (_flag_counts["Rejected Count"] / max(_rej_s, 1) * 100)
        .round(1).astype(str) + "%"
    )

    _summary = pd.DataFrame({
        "Metric": [
            "Total Products", "Approved", "Rejected",
            "Rejection Rate", "Country", "Generated At",
        ],
        "Value": [
            _tot_s, _app_s, _rej_s,
            f"{_rej_s / max(_tot_s, 1) * 100:.1f}%",
            st.session_state.get("selected_country", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M"),
        ],
    })

    _sheet_map = {
        "PIM Export": {
            "ProductSets":       _product_sets,
            "Rejection Reasons": _rej_reasons,
        },
        "Rejected Only": {
            "ProductSets":       _ps_rej,
            "Rejection Reasons": _rej_reasons,
        },
        "Approved Only": {
            "ProductSets":       _ps_app,
            "Rejection Reasons": _rej_reasons,
        },
        "Full Data": {
            "Summary":           _summary,
            "Flags Breakdown":   _flag_counts,
            "ProductSets":       _product_sets,
            "Rejection Reasons": _rej_reasons,
            "Full Data":         _full,
        },
    }
    
    data = df_to_excel(_sheet_map.get(name, {"ProductSets": _product_sets}))
    st.session_state.qcr_exports[name] = data
    return data

_CARDS = [
    ("PIM Export",    "5 sheets: Summary · Flags · All · Rejected · Approved", len(_lfr2)),
    ("Rejected Only", "Products that failed validation",                        len(_rej3)),
    ("Approved Only", "Products that passed validation",                        len(_app3)),
    ("Full Data",     "Full dataset with QC flags appended",                    len(_lfr2)),
]

_ar = all(n in st.session_state.qcr_exports for n,*_ in _CARDS)
if _ar:
    st.success("All reports ready to download.", icon=":material/check_circle:")
else:
    if st.button(":material/download: Generate All Reports", type="primary",
                 width='stretch', key="qcr_gen_all"):
        with st.spinner("Generating all reports…"):
            for _n, *_ in _CARDS:
                _bex(_n)
        st.rerun()

_ecols = st.columns(4)
for _ecol, (_cn, _cd, _cr) in zip(_ecols, _CARDS):
    with _ecol:
        with st.container(border=True):
            st.markdown(f"**{_cn}**")
            st.caption(_cd)
            st.metric("Rows", f"{_cr:,}")
            if _cn not in st.session_state.qcr_exports:
                if st.button(":material/download: Generate", key=f"qcr_gen_{_cn}",
                             type="primary", width='stretch'):
                    with st.spinner(f"Building {_cn}…"):
                        _bex(_cn)
                    st.rerun()
            else:
                st.download_button(
                    ":material/file_download: Download",
                    data=st.session_state.qcr_exports[_cn],
                    file_name=f"{_cc3}_{_cn.replace(' ','_')}_{_ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"qcr_dl_{_cn}",
                    width='stretch',
                    type="primary",
                )
                if st.button("Clear", key=f"qcr_clr_{_cn}", width='stretch'):
                    del st.session_state.qcr_exports[_cn]
                    st.rerun()