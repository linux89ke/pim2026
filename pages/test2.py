import sys
import json
import gzip
import re
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title = "Jumia Product Viewer",
    page_icon  = "🛍️",
    layout     = "wide",
)

# ══════════════════════════════════════════════════════════════════════════════
#  INLINE HELPERS  (no external imports needed)
# ══════════════════════════════════════════════════════════════════════════════

def safe_str(val, default="N/A") -> str:
    if val is None:
        return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return default
    except Exception:
        pass
    s = str(val).strip()
    return s if s and s.lower() not in ("nan","none","null") else default


def safe_float(val, default=None):
    if val is None:
        return default
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return default
    except Exception:
        pass
    try:
        return float(str(val).replace(",","").strip())
    except Exception:
        return default


def safe_int(val, default=None):
    f = safe_float(val)
    if f is None:
        return default
    try:
        return int(f)
    except Exception:
        return default


def has_ai_summary(val) -> bool:
    if not val:
        return False
    s = str(val).strip()
    if s in ("N/A","","nan","None"):
        return False
    if s.startswith("_Summary unavailable"):
        return False
    if len(s) < 20:
        return False
    try:
        data = json.loads(s)
        return isinstance(data, dict) and len(data) > 0
    except Exception:
        return False


def load_export(raw_bytes: bytes) -> pd.DataFrame:
    try:
        data = json.loads(gzip.decompress(raw_bytes))
    except Exception:
        try:
            data = json.loads(raw_bytes)
        except Exception:
            return pd.DataFrame()
    products = data.get("products", data if isinstance(data, list) else [])
    return pd.DataFrame(products)


def get_ai(row) -> dict:
    try:
        return json.loads(safe_str(row.get("AI Summary","")))
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  RENDERING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def render_html_iframe(html: str, uid: str = "0", min_height: int = 300) -> None:
    wrapped = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
    <style>
      body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
            background:#0e1117;color:#fafafa;padding:12px 16px;margin:0;
            line-height:1.7;font-size:14px}}
      img{{max-width:100%;border-radius:8px;
           box-shadow:0 2px 12px rgba(0,0,0,.4);margin:12px 0;display:block}}
      h1,h2,h3,h4{{color:#ff6900;margin-top:18px}}
      ul,ol{{padding-left:20px}} li{{margin:4px 0}}
      p{{margin:8px 0}} strong,b{{color:#fff}}
      table{{border-collapse:collapse;width:100%}}
      td,th{{border:1px solid #333;padding:6px 10px;text-align:left}}
      th{{background:#1a1a1a}}
    </style></head><body>{html}</body></html>"""
    img_count  = html.count("<img")
    est_height = min_height + img_count * 300 + len(html) // 10
    components.html(wrapped, height=min(est_height, 6000), scrolling=True)


def render_image_lightbox(image_urls: list, name: str, uid: str) -> None:
    if not image_urls:
        st.info("No gallery images.")
        return
    thumbs = "".join(
        f'<div class="th" onclick="olb_{uid}({i})">'
        f'<img src="{u}" loading="lazy"/>'
        f'<div class="n">{i+1}</div></div>'
        for i, u in enumerate(image_urls)
    )
    imgs_js = json.dumps(image_urls)
    html = f"""<style>
      *{{box-sizing:border-box;margin:0;padding:0}}
      body{{background:#0e1117}}
      .g{{display:flex;flex-wrap:wrap;gap:6px;padding:6px}}
      .th{{width:88px;height:88px;overflow:hidden;border-radius:6px;
           cursor:pointer;border:2px solid transparent;transition:.2s;position:relative}}
      .th:hover{{border-color:#ff6900;transform:scale(1.05)}}
      .th img{{width:100%;height:100%;object-fit:cover}}
      .th .n{{position:absolute;bottom:3px;right:5px;background:rgba(0,0,0,.6);
              color:#fff;font-size:10px;border-radius:3px;padding:1px 4px}}
      .lb{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.93);
           z-index:9999;flex-direction:column;align-items:center;justify-content:center}}
      .lb.on{{display:flex}}
      .lb img{{max-width:88vw;max-height:72vh;border-radius:8px}}
      .cl{{position:absolute;top:14px;right:20px;color:#fff;font-size:22px;
           cursor:pointer;background:rgba(255,105,0,.3);border:none;
           border-radius:50%;width:36px;height:36px;line-height:36px;text-align:center}}
      .cl:hover{{background:#ff6900}}
      .cap{{color:#ccc;margin-top:10px;font-size:13px}}
      .nav{{display:flex;gap:12px;margin-top:14px}}
      .nav button{{background:rgba(255,105,0,.2);color:#fff;border:1px solid #ff6900;
                  padding:7px 22px;border-radius:5px;cursor:pointer}}
      .nav button:hover{{background:#ff6900}}
    </style>
    <div class="g">{thumbs}</div>
    <div class="lb" id="lb_{uid}" onclick="if(event.target===this)clb_{uid}()">
      <button class="cl" onclick="clb_{uid}()">X</button>
      <img id="lbi_{uid}" src="" alt=""/>
      <div class="cap" id="lbc_{uid}"></div>
      <div class="nav">
        <button onclick="mv_{uid}(-1)">Prev</button>
        <button onclick="mv_{uid}(1)">Next</button>
      </div>
    </div>
    <script>
      const I_{uid}={imgs_js},N_{uid}={json.dumps(name)};
      let c_{uid}=0;
      function olb_{uid}(i){{c_{uid}=i;upd_{uid}();
        document.getElementById('lb_{uid}').classList.add('on')}}
      function clb_{uid}(){{document.getElementById('lb_{uid}').classList.remove('on')}}
      function mv_{uid}(d){{c_{uid}=(c_{uid}+d+I_{uid}.length)%I_{uid}.length;upd_{uid}()}}
      function upd_{uid}(){{
        document.getElementById('lbi_{uid}').src=I_{uid}[c_{uid}];
        document.getElementById('lbc_{uid}').innerText=
          N_{uid}+' — '+(c_{uid}+1)+'/'+I_{uid}.length}}
      document.addEventListener('keydown',e=>{{
        if(document.getElementById('lb_{uid}').classList.contains('on')){{
          if(e.key==='ArrowLeft')mv_{uid}(-1);
          if(e.key==='ArrowRight')mv_{uid}(1);
          if(e.key==='Escape')clb_{uid}()
        }}
      }})
    </script>"""
    rows = (len(image_urls) // 5) + 1
    components.html(html, height=max(110, rows * 102 + 12), scrolling=False)


# ══════════════════════════════════════════════════════════════════════════════
#  CARD GRID
# ══════════════════════════════════════════════════════════════════════════════

def render_card_grid(df: pd.DataFrame, cols_per_row: int = 4) -> None:
    grade_color = {"A":"#00c853","B":"#8bc34a","C":"#ffc107","D":"#ff9800","F":"#f44336"}

    for i in range(0, len(df), cols_per_row):
        chunk = df.iloc[i: i + cols_per_row]
        cols  = st.columns(cols_per_row)

        for j, (_, row) in enumerate(chunk.iterrows()):
            with cols[j]:
                with st.container(border=True):

                    # Image
                    img_raw = safe_str(row.get("Image URLs",""))
                    if img_raw != "N/A":
                        first = img_raw.split(" | ")[0].strip()
                        try:    st.image(first, use_container_width=True)
                        except: pass
                    else:
                        st.markdown("*No image*")

                    # Title
                    st.markdown(f"**{safe_str(row.get('Product Name',''))[:55]}**")

                    # Price
                    price = safe_float(row.get("Price (KSh)"))
                    orig  = safe_float(row.get("Original Price (KSh)"))
                    disc  = safe_str(row.get("Discount",""))
                    if price is not None:
                        if orig is not None and orig > price:
                            st.markdown(
                                f"<s style='color:#888'>KSh {int(orig):,}</s> "
                                f"**KSh {int(price):,}** {disc}",
                                unsafe_allow_html=True,
                            )
                        else:
                            st.markdown(f"**KSh {int(price):,}**")

                    # Rating
                    rating  = safe_float(row.get("Rating"))
                    reviews = safe_int(row.get("Reviews"), 0)
                    if rating is not None:
                        st.caption(f"{rating:.1f}/5 ({reviews} reviews)")

                    # Jumia link
                    url = safe_str(row.get("URL",""))
                    if url != "N/A":
                        st.markdown(f"[View on Jumia]({url})")

                    # AI summary below link
                    ai = get_ai(row)
                    if ai:
                        grade = ai.get("grade","")
                        score = ai.get("overall_score","")
                        recs  = ai.get("seller_recommendations",[])
                        rules = ai.get("jumia_rules",{})
                        color = grade_color.get(grade,"#888")

                        st.markdown(
                            f"<div style='margin:6px 0 4px'>"
                            f"<span style='background:{color};color:#fff;"
                            f"font-size:11px;font-weight:700;padding:2px 10px;"
                            f"border-radius:10px'>Grade {grade} — {score}/100</span>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )

                        # Failing rules
                        failing = [
                            lbl for key, lbl in {
                                "gallery_ok":"Gallery","description_ok":"Description",
                                "features_ok":"Features","warranty_ok":"Warranty",
                                "desc_images_ok":"Desc Images",
                            }.items() if rules.get(key) is False
                        ]
                        if failing:
                            pills = " ".join(
                                f"<span style='background:#f44336;color:#fff;"
                                f"font-size:10px;padding:1px 6px;border-radius:8px'>"
                                f"{f}</span>" for f in failing
                            )
                            st.markdown(
                                f"<div style='margin:3px 0'>{pills}</div>",
                                unsafe_allow_html=True,
                            )

                        # Recommendations
                        if recs:
                            rec_html = "".join(
                                f"<div style='font-size:11px;color:#ddd;padding:3px 0;"
                                f"border-bottom:1px solid #2a2a2a;line-height:1.4'>"
                                f"<span style='color:#ff6900;font-weight:700'>{k}.</span> "
                                f"{r}</div>"
                                for k, r in enumerate(recs[:3], 1)
                            )
                            st.markdown(
                                f"<div style='background:#111;border-radius:6px;"
                                f"padding:6px 8px;margin:5px 0'>"
                                f"<div style='font-size:10px;color:#888;"
                                f"text-transform:uppercase;letter-spacing:.5px;"
                                f"margin-bottom:4px'>What to fix</div>"
                                f"{rec_html}</div>",
                                unsafe_allow_html=True,
                            )


# ══════════════════════════════════════════════════════════════════════════════
#  AI SUMMARY PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_ai_summary(ai: dict, uid: str = "0") -> None:
    if not ai:
        st.info("No AI analysis in this export.")
        return

    score = ai.get("overall_score", 0)
    grade = ai.get("grade","F")
    grade_color = {"A":"#00c853","B":"#8bc34a","C":"#ffc107","D":"#ff9800","F":"#f44336"}
    color = grade_color.get(grade,"#888")

    st.markdown(
        f"<div style='display:inline-block;background:{color};color:#fff;"
        f"padding:6px 20px;border-radius:16px;font-weight:700;font-size:20px;"
        f"margin-bottom:12px'>Content Grade: {grade} — {score}/100</div>",
        unsafe_allow_html=True,
    )

    summary = ai.get("summary","")
    if summary:
        st.markdown(f"> {summary}")

    enrich    = ai.get("enrichment",{})
    brand_url = enrich.get("brand_page_url","")
    img_count = enrich.get("infographic_count", 0)
    if brand_url:
        st.caption(f"Reference: [{brand_url}]({brand_url})")
    if img_count:
        st.caption(f"{img_count} product images embedded in description")

    st.markdown("---")

    # Rules
    rules = ai.get("jumia_rules",{})
    if rules:
        st.markdown("#### Jumia Content Rules")
        rule_labels = {
            "gallery_ok":"Gallery (min 5)","description_ok":"Description (min 300)",
            "features_ok":"Key Features (min 5)","warranty_ok":"Warranty present",
            "desc_images_ok":"Description images",
        }
        rcols = st.columns(len(rule_labels))
        for i, (key, label) in enumerate(rule_labels.items()):
            ok    = rules.get(key)
            c     = "#00c853" if ok else "#f44336" if ok is False else "#888"
            icon  = "PASS" if ok else "FAIL" if ok is False else "N/A"
            with rcols[i]:
                st.markdown(
                    f"<div style='background:#1a1a1a;border:2px solid {c};"
                    f"border-radius:8px;padding:8px;text-align:center'>"
                    f"<div style='color:{c};font-weight:700;font-size:14px'>{icon}</div>"
                    f"<div style='font-size:11px;color:#aaa;margin-top:4px'>{label}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Infographics
    infographics = ai.get("infographic_suggestions",[])
    if infographics:
        st.markdown("#### Infographics to Create")
        pc = {"High":"#f44336","Medium":"#ffc107","Low":"#8bc34a"}
        for ig in infographics:
            p = ig.get("priority","Medium")
            st.markdown(
                f"<div style='background:#1a1a1a;border-left:4px solid {pc.get(p,'#888')};"
                f"border-radius:0 8px 8px 0;padding:10px 14px;margin:6px 0'>"
                f"<span style='color:{pc.get(p,'#888')};font-size:11px;font-weight:700;"
                f"text-transform:uppercase'>{p} PRIORITY</span>"
                f" &nbsp;<b style='font-size:14px'>{ig.get('type','')}</b><br>"
                f"<span style='color:#bbb;font-size:13px'>{ig.get('description','')}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Improved description
    desc = ai.get("improved_description","")
    if desc:
        st.markdown("#### AI-Improved Description")
        st.caption(f"{desc.count('<img')} product images embedded")
        with st.expander("Show description", expanded=True):
            render_html_iframe(desc, uid=f"desc_{uid}")
        st.download_button(
            "Download as HTML",
            desc.encode(),
            "improved_description.html",
            "text/html",
            key=f"dl_desc_{uid}",
            use_container_width=True,
        )

    st.markdown("---")

    # Features
    feats = ai.get("improved_key_features",[])
    if feats:
        st.markdown(f"#### Key Features ({len(feats)})")
        fc = st.columns(2)
        for i, f in enumerate(feats):
            with fc[i % 2]:
                st.markdown(
                    f"<div style='background:#1a1a1a;border:1px solid #333;"
                    f"border-radius:6px;padding:8px 12px;margin:4px 0;font-size:13px'>"
                    f"{i+1}. {f}</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Specs table
    specs = ai.get("complete_specs",{})
    if specs:
        st.markdown(f"#### Specifications ({len(specs)})")
        st.dataframe(
            pd.DataFrame(specs.items(), columns=["Specification","Value"]),
            use_container_width=True, hide_index=True,
            height=min(len(specs)*35+42, 450),
        )

    st.markdown("---")

    # Platform research
    platform = ai.get("platform_research",{})
    if platform:
        st.markdown("#### What Competing Listings Include (You Are Missing)")
        p1, p2 = st.columns(2)
        with p1:
            for g in platform.get("amazon_gaps",[]):
                st.markdown(f"- **Amazon:** {g}")
        with p2:
            for g in platform.get("aliexpress_gaps",[]):
                st.markdown(f"- **AliExpress:** {g}")
        fs = platform.get("specs_buyers_filter_by",[])
        if fs:
            st.markdown("**Specs buyers filter by:**")
            pills = "  ".join(
                f"<code style='background:#2a2a2a;padding:2px 8px;"
                f"border-radius:4px;font-size:12px'>{s}</code>" for s in fs
            )
            st.markdown(pills, unsafe_allow_html=True)

    st.markdown("---")

    # Title
    st.markdown("#### Title")
    t1, t2 = st.columns(2)
    with t1:
        for iss in (ai.get("title_issues",[]) or ["No issues found"]):
            st.markdown(f"- {iss}")
    with t2:
        it = ai.get("improved_title","")
        if it:
            st.markdown("**Suggested:**")
            st.code(it)

    st.markdown("---")

    # Recommendations
    recs = ai.get("seller_recommendations",[])
    if recs:
        st.markdown("#### Top Actions to Take Now")
        for i, r in enumerate(recs, 1):
            st.markdown(
                f"<div style='background:#1a1a1a;border:1px solid #333;"
                f"border-radius:8px;padding:10px 14px;margin:6px 0'>"
                f"<b style='color:#ff6900'>{i}.</b> {r}</div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Keywords
    kws = ai.get("keywords",[])
    if kws:
        st.markdown("#### Search Keywords")
        pills = "  ".join(
            f"<code style='background:#2a2a2a;padding:3px 10px;"
            f"border-radius:4px;font-size:13px'>{k}</code>" for k in kws
        )
        st.markdown(pills, unsafe_allow_html=True)

    st.markdown("---")

    # Marketplace links
    links      = dict(ai.get("marketplace_links",{}))
    brand_site = ai.get("official_brand_site","")
    if brand_site:
        links["Official Site"] = brand_site
    if links:
        st.markdown("#### Search This Product On")
        items = list(links.items())
        for start in range(0, len(items), 5):
            chunk = items[start:start+5]
            lc    = st.columns(5)
            for i, (lname, lurl) in enumerate(chunk):
                with lc[i]:
                    st.markdown(
                        f"<a href='{lurl}' target='_blank' "
                        f"style='display:block;text-align:center;background:#2a2a2a;"
                        f"border:1px solid #444;border-radius:8px;padding:8px 4px;"
                        f"color:#4fc3f7;text-decoration:none;font-size:12px;margin:2px'>"
                        f"{lname}</a>",
                        unsafe_allow_html=True,
                    )


# ══════════════════════════════════════════════════════════════════════════════
#  PDP CARD
# ══════════════════════════════════════════════════════════════════════════════

def display_pdp_card(row, card_idx: int = 0) -> None:
    uid  = str(card_idx)
    name = safe_str(row.get("Title", row.get("Product Name","")))

    price  = safe_float(row.get("Price (KSh)"))
    orig   = safe_float(row.get("Original Price (KSh)"))
    disc   = safe_str(row.get("Discount",""))
    rating = safe_float(row.get("Rating"))
    reviews= safe_int(row.get("Reviews"), 0)
    sku    = safe_str(row.get("SKU","N/A"))
    cat    = safe_str(row.get("Category","N/A"))
    brand  = safe_str(row.get("Brand","N/A"))
    seller = safe_str(row.get("Seller Name","N/A"))
    badges = safe_str(row.get("Badges",""))
    url    = safe_str(row.get("URL",""))

    img_raw  = safe_str(row.get("Image URLs",""))
    img_urls = [u.strip() for u in img_raw.split(" | ")
                if u.strip() and img_raw != "N/A"]

    ai = get_ai(row)

    with st.container(border=True):
        h1, h2 = st.columns([4,1])
        with h1:
            st.markdown(f"### {name}")
            meta = []
            if sku   != "N/A": meta.append(f"SKU: `{sku}`")
            if cat   != "N/A": meta.append(f"Category: {cat}")
            if brand != "N/A": meta.append(f"Brand: {brand}")
            if seller!= "N/A": meta.append(f"Seller: {seller}")
            if meta:
                st.caption("  |  ".join(meta))
            if badges and badges != "N/A":
                pills = "  ".join(
                    f"<span style='background:#ff6900;color:#fff;font-size:11px;"
                    f"padding:2px 8px;border-radius:10px'>{b.strip()}</span>"
                    for b in badges.split("|") if b.strip()
                )
                st.markdown(pills, unsafe_allow_html=True)

        with h2:
            if price is not None:
                if orig is not None and orig > price:
                    st.markdown(
                        f"<s style='color:#888;font-size:13px'>KSh {int(orig):,}</s><br>"
                        f"<b style='font-size:22px'>KSh {int(price):,}</b><br>"
                        f"<span style='color:#ff6900'>{disc}</span>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"<b style='font-size:22px'>KSh {int(price):,}</b>",
                        unsafe_allow_html=True,
                    )
            if rating is not None:
                st.caption(f"{rating:.1f}/5  ({reviews} reviews)")
            if url != "N/A":
                st.markdown(f"[View on Jumia]({url})")

        st.markdown("---")

        tab_gallery, tab_desc, tab_specs, tab_ai = st.tabs([
            f"Gallery ({len(img_urls)})", "Description", "Specifications", "AI Analysis"
        ])

        # Gallery
        with tab_gallery:
            if img_urls:
                render_image_lightbox(img_urls, name, f"pdp_{uid}")
            else:
                st.info("No gallery images.")

        # Description
        with tab_desc:
            improved = ai.get("improved_description","")
            if improved:
                st.markdown(f"**AI-Improved Description ({improved.count('<img')} images embedded)**")
                render_html_iframe(improved, uid=f"pdesc_{uid}")
                st.markdown("---")
                st.markdown("**Original Description**")

            desc_html = safe_str(row.get("Description HTML",""))
            desc_text = safe_str(row.get("Description",""))
            if desc_html != "N/A":
                render_html_iframe(desc_html, uid=f"orig_{uid}", min_height=200)
            elif desc_text != "N/A":
                st.markdown(desc_text)
            else:
                st.info("No description available.")

            witb = safe_str(row.get("What's in the Box",""))
            if witb != "N/A":
                st.markdown("**What's in the Box**")
                for item in witb.split("|"):
                    if item.strip():
                        st.markdown(f"- {item.strip()}")

            ai_feats = ai.get("improved_key_features",[])
            kf       = safe_str(row.get("Key Features",""))
            if ai_feats:
                st.markdown(f"**Key Features — AI completed ({len(ai_feats)})**")
                for feat in ai_feats:
                    st.markdown(f"- {feat}")
            elif kf != "N/A":
                st.markdown("**Key Features**")
                for feat in kf.split("|"):
                    if feat.strip():
                        st.markdown(f"- {feat.strip()}")

        # Specs
        with tab_specs:
            ai_specs  = ai.get("complete_specs",{})
            spec_dict = {
                k.replace("Spec: ",""): v
                for k, v in row.items()
                if str(k).startswith("Spec: ") and safe_str(v) not in ("N/A","")
            }
            merged = {**spec_dict, **ai_specs}
            if merged:
                st.dataframe(
                    pd.DataFrame(merged.items(), columns=["Specification","Value"]),
                    use_container_width=True, hide_index=True,
                    height=min(len(merged)*35+42, 450),
                )
            else:
                specs_raw = safe_str(row.get("Specifications (Raw)",""))
                if specs_raw != "N/A":
                    for pair in specs_raw.split("|"):
                        if ":" in pair:
                            k, _, v = pair.partition(":")
                            st.markdown(f"**{k.strip()}:** {v.strip()}")
                else:
                    st.info("No specifications found.")

            warrant = safe_str(row.get("Warranty Info",""))
            if warrant != "N/A":
                st.markdown(f"**Warranty:** {warrant}")

        # AI Analysis
        with tab_ai:
            if ai:
                render_ai_summary(ai, uid=uid)
            else:
                st.info("No AI analysis in this export.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ══════════════════════════════════════════════════════════════════════════════

st.title("🛍️ Jumia Product Viewer")
st.caption("Upload an export file — browse products with full AI analysis, images, and descriptions.")

uploaded = st.file_uploader(
    "Upload export file (.json.gz or .json)",
    type=["gz","json"],
    help="Export from the Jumia Scraper app using the 'Export for Viewer' button",
)

if not uploaded:
    st.info("Upload an export file above to get started.")
    st.stop()

with st.spinner("Loading..."):
    df = load_export(uploaded.read())

if df.empty:
    st.error("No products found in this file.")
    st.stop()

st.success(f"Loaded {len(df)} products")

# ─── Sidebar filters ──────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    search = st.text_input("Search", placeholder="Name, brand, SKU...")

    cats = sorted(df["Category"].dropna().unique().tolist()) if "Category" in df.columns else []
    sel_cats = st.multiselect("Category", cats)

    brands = sorted(df["Brand"].dropna().unique().tolist()) if "Brand" in df.columns else []
    sel_brands = st.multiselect("Brand", brands)

    if "AI Summary" in df.columns:
        sel_grades = st.multiselect("AI Grade", ["A","B","C","D","F"])
    else:
        sel_grades = []

    if "Price (KSh)" in df.columns:
        prices = pd.to_numeric(df["Price (KSh)"], errors="coerce").dropna()
        if len(prices) > 1 and prices.min() < prices.max():
            price_range = st.slider(
                "Price (KSh)",
                int(prices.min()), int(prices.max()),
                (int(prices.min()), int(prices.max())),
                step=100,
            )
        else:
            price_range = None
    else:
        price_range = None

    sort_by = st.selectbox(
        "Sort by",
        ["Default","Price: Low to High","Price: High to Low",
         "Rating","AI Score","Name A-Z"],
    )
    st.markdown("---")
    st.caption(f"{len(df)} products in file")

# ─── Apply filters ────────────────────────────────────────────────────────────
filt = df.copy()

if search:
    mask = filt.apply(
        lambda r: any(
            search.lower() in str(r.get(c,"")).lower()
            for c in ["Product Name","Title","Brand","SKU"]
        ), axis=1,
    )
    filt = filt[mask]

if sel_cats   and "Category" in filt.columns: filt = filt[filt["Category"].isin(sel_cats)]
if sel_brands and "Brand"    in filt.columns: filt = filt[filt["Brand"].isin(sel_brands)]

if sel_grades and "AI Summary" in filt.columns:
    filt = filt[filt["AI Summary"].apply(
        lambda x: get_ai({"AI Summary":x}).get("grade","")
    ).isin(sel_grades)]

if price_range and "Price (KSh)" in filt.columns:
    filt = filt.copy()
    filt["_p"] = pd.to_numeric(filt["Price (KSh)"], errors="coerce")
    filt = filt[(filt["_p"] >= price_range[0]) & (filt["_p"] <= price_range[1])].drop(columns=["_p"])

# Sort
if sort_by == "Price: Low to High"  and "Price (KSh)" in filt.columns:
    filt = filt.copy(); filt["_s"]=pd.to_numeric(filt["Price (KSh)"],errors="coerce"); filt=filt.sort_values("_s").drop(columns=["_s"])
elif sort_by == "Price: High to Low" and "Price (KSh)" in filt.columns:
    filt = filt.copy(); filt["_s"]=pd.to_numeric(filt["Price (KSh)"],errors="coerce"); filt=filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "Rating" and "Rating" in filt.columns:
    filt = filt.copy(); filt["_s"]=pd.to_numeric(filt["Rating"],errors="coerce"); filt=filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "AI Score" and "AI Summary" in filt.columns:
    filt = filt.copy(); filt["_s"]=filt["AI Summary"].apply(lambda x:get_ai({"AI Summary":x}).get("overall_score",0)); filt=filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "Name A-Z":
    nc = "Product Name" if "Product Name" in filt.columns else "Title"
    if nc in filt.columns: filt = filt.sort_values(nc)

# ─── Stats ────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
ai_done    = filt["AI Summary"].apply(has_ai_summary).sum() if "AI Summary" in filt.columns else 0
prices_num = pd.to_numeric(filt.get("Price (KSh)", pd.Series()), errors="coerce").dropna()
scores     = filt["AI Summary"].apply(lambda x: get_ai({"AI Summary":x}).get("overall_score",0)).replace(0, pd.NA).dropna() if "AI Summary" in filt.columns else pd.Series()

c1.metric("Products",    len(filt))
c2.metric("AI Analysed", ai_done)
c3.metric("Avg Price",   f"KSh {int(prices_num.mean()):,}" if len(prices_num) else "N/A")
c4.metric("Avg AI Score",f"{scores.mean():.0f}/100"        if len(scores)     else "N/A")

st.markdown("---")

if filt.empty:
    st.warning("No products match your filters.")
    st.stop()

# ─── View mode ────────────────────────────────────────────────────────────────
v1, v2 = st.columns([2,1])
with v1:
    view = st.radio("View", ["Card Grid","Detailed List"], horizontal=True, label_visibility="collapsed")
with v2:
    cpr = st.select_slider("Columns", [2,3,4,5], value=4, label_visibility="collapsed") if view == "Card Grid" else 4

filt_reset = filt.reset_index(drop=True)

if view == "Card Grid":
    render_card_grid(filt_reset, cols_per_row=cpr)
else:
    for idx, (_, row) in enumerate(filt_reset.iterrows()):
        display_pdp_card(row, card_idx=idx)
        st.markdown("---")
