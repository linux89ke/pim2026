import json
import gzip
import re
import math
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
#  INLINE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def safe_str(val, default="N/A") -> str:
    if val is None:
        return default
    try:
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
#  HTML RENDERER  (iframe so base64 images always work)
# ══════════════════════════════════════════════════════════════════════════════

def _render_html_with_images(html: str, uid: str = "0", min_height: int = 400) -> None:
    wrapped = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0e1117;
    color: #fafafa;
    padding: 12px 16px;
    margin: 0;
    line-height: 1.7;
    font-size: 14px;
  }}
  img {{
    max-width: 100%;
    border-radius: 8px;
    box-shadow: 0 2px 12px rgba(0,0,0,.4);
    margin: 12px 0;
    display: block;
  }}
  h1,h2,h3,h4 {{ color: #ff6900; margin-top: 20px; }}
  ul, ol {{ padding-left: 20px; }}
  li {{ margin: 4px 0; }}
  p  {{ margin: 8px 0; }}
  strong, b {{ color: #fff; }}
  table {{ border-collapse:collapse; width:100%; }}
  td,th {{ border:1px solid #333; padding:6px 10px; text-align:left; }}
  th {{ background:#1a1a1a; }}
  .gallery {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:12px; }}
  .gallery div {{ flex: 0 0 calc(33% - 8px); }}
  .gallery img  {{ width:100%; }}
</style>
</head>
<body>
{html}
</body>
</html>"""
    img_count  = html.count("<img")
    est_height = min_height + img_count * 320 + len(html) // 8
    components.html(wrapped, height=min(est_height, 5000), scrolling=True)


# ══════════════════════════════════════════════════════════════════════════════
#  LIGHTBOX
# ══════════════════════════════════════════════════════════════════════════════

def render_image_lightbox(image_urls: list, product_name: str, uid: str) -> None:
    if not image_urls:
        st.info("No product images found.")
        return

    thumbs_html = "".join(
        f'<div class="th" onclick="open_lb_{uid}({i})">'
        f'<img src="{url}" alt="img{i}" loading="lazy"/>'
        f'<div class="num">{i+1}</div></div>'
        for i, url in enumerate(image_urls)
    )
    imgs_js = json.dumps(image_urls)

    html = f"""
    <style>
      *{{box-sizing:border-box;margin:0;padding:0}}
      body{{background:#0e1117;font-family:sans-serif}}
      .gallery{{display:flex;flex-wrap:wrap;gap:6px;padding:6px}}
      .th{{position:relative;width:88px;height:88px;overflow:hidden;
           border-radius:6px;cursor:pointer;border:2px solid transparent;transition:.2s}}
      .th:hover{{border-color:#ff6900;transform:scale(1.05)}}
      .th img{{width:100%;height:100%;object-fit:cover}}
      .th .num{{position:absolute;bottom:3px;right:5px;background:rgba(0,0,0,.6);
                color:#fff;font-size:10px;border-radius:3px;padding:1px 4px}}
      .lb{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.93);
           z-index:9999;flex-direction:column;align-items:center;justify-content:center}}
      .lb.on{{display:flex}}
      .lb-img{{max-width:88vw;max-height:72vh;border-radius:8px;
               box-shadow:0 0 40px rgba(255,105,0,.25)}}
      .close-btn{{position:absolute;top:14px;right:20px;color:#fff;font-size:26px;
                  cursor:pointer;background:rgba(255,105,0,.3);border:none;
                  border-radius:50%;width:36px;height:36px;line-height:36px;text-align:center}}
      .close-btn:hover{{background:#ff6900}}
      .cap{{color:#ccc;margin-top:10px;font-size:13px;text-align:center;max-width:600px}}
      .cnt{{color:#888;font-size:12px;margin-top:4px}}
      .nav{{display:flex;gap:12px;margin-top:14px}}
      .nav button{{background:rgba(255,105,0,.2);color:#fff;border:1px solid #ff6900;
                  padding:7px 22px;border-radius:5px;cursor:pointer;font-size:14px;transition:.2s}}
      .nav button:hover{{background:#ff6900}}
    </style>
    <div class="gallery">{thumbs_html}</div>
    <div class="lb" id="lb_{uid}" onclick="if(event.target===this)close_lb_{uid}()">
      <button class="close-btn" onclick="close_lb_{uid}()">X</button>
      <img class="lb-img" id="lb_img_{uid}" src="" alt=""/>
      <div class="cap" id="lb_cap_{uid}"></div>
      <div class="cnt" id="lb_cnt_{uid}"></div>
      <div class="nav">
        <button onclick="move_{uid}(-1)">Prev</button>
        <button onclick="move_{uid}(1)">Next</button>
      </div>
    </div>
    <script>
      const IMGS_{uid}={imgs_js}, NAME_{uid}={json.dumps(product_name)};
      let cur_{uid}=0;
      function open_lb_{uid}(i){{
        cur_{uid}=i; upd_{uid}();
        document.getElementById('lb_{uid}').classList.add('on');
      }}
      function close_lb_{uid}(){{
        document.getElementById('lb_{uid}').classList.remove('on');
      }}
      function move_{uid}(d){{
        cur_{uid}=(cur_{uid}+d+IMGS_{uid}.length)%IMGS_{uid}.length; upd_{uid}();
      }}
      function upd_{uid}(){{
        document.getElementById('lb_img_{uid}').src=IMGS_{uid}[cur_{uid}];
        document.getElementById('lb_cap_{uid}').innerText=NAME_{uid};
        document.getElementById('lb_cnt_{uid}').innerText=
          (cur_{uid}+1)+' / '+IMGS_{uid}.length;
      }}
      document.addEventListener('keydown',e=>{{
        if(document.getElementById('lb_{uid}').classList.contains('on')){{
          if(e.key==='ArrowLeft') move_{uid}(-1);
          if(e.key==='ArrowRight') move_{uid}(1);
          if(e.key==='Escape') close_lb_{uid}();
        }}
      }});
    </script>"""

    rows = (len(image_urls) // 5) + 1
    components.html(html, height=max(110, rows * 102 + 12), scrolling=False)


# ══════════════════════════════════════════════════════════════════════════════
#  CARD GRID  (identical to ui_components.py)
# ══════════════════════════════════════════════════════════════════════════════

def render_card_grid(df: pd.DataFrame, cols_per_row: int = 4) -> None:
    for i in range(0, len(df), cols_per_row):
        chunk = df.iloc[i: i + cols_per_row]
        cols  = st.columns(cols_per_row)

        for j, (_, row) in enumerate(chunk.iterrows()):
            with cols[j]:
                with st.container(border=True):

                    # Image
                    img_urls = safe_str(row.get("Image URLs"))
                    if img_urls != "N/A":
                        first = img_urls.split(" | ")[0].strip()
                        try:    st.image(first, use_container_width=True)
                        except: st.markdown("")
                    else:
                        st.markdown("*No image*")

                    # Title
                    st.markdown(f"**{safe_str(row.get('Product Name'))[:55]}**")

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
                    url = safe_str(row.get("URL"))
                    if url != "N/A":
                        st.markdown(f"[View on Jumia]({url})")

                    # AI summary below link
                    ai_raw = safe_str(row.get("AI Summary"))
                    if has_ai_summary(ai_raw):
                        try:
                            ai_data = json.loads(ai_raw)
                            grade   = ai_data.get("grade","")
                            score   = ai_data.get("overall_score","")
                            recs    = ai_data.get("seller_recommendations",[])
                            issues  = ai_data.get("title_issues",[])
                            rules   = ai_data.get("jumia_rules",{})

                            grade_color = {
                                "A":"#00c853","B":"#8bc34a",
                                "C":"#ffc107","D":"#ff9800","F":"#f44336",
                            }.get(grade,"#888")
                            st.markdown(
                                f"<div style='margin:6px 0 4px'>"
                                f"<span style='background:{grade_color};color:#fff;"
                                f"font-size:11px;font-weight:700;padding:2px 10px;"
                                f"border-radius:10px'>Grade {grade} — {score}/100</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )

                            # Failing rules
                            failing = [
                                label for key, label in {
                                    "gallery_ok":     "Gallery",
                                    "description_ok": "Description",
                                    "features_ok":    "Features",
                                    "warranty_ok":    "Warranty",
                                    "desc_images_ok": "Desc Images",
                                }.items()
                                if rules.get(key) is False
                            ]
                            if failing:
                                pills = " ".join(
                                    f"<span style='background:#f44336;color:#fff;"
                                    f"font-size:10px;padding:1px 6px;border-radius:8px;"
                                    f"margin:1px;display:inline-block'>{f}</span>"
                                    for f in failing
                                )
                                st.markdown(
                                    f"<div style='margin:3px 0'>{pills}</div>",
                                    unsafe_allow_html=True,
                                )

                            # Recommendations
                            if recs:
                                rec_html = "".join(
                                    f"<div style='font-size:11px;color:#ddd;"
                                    f"padding:3px 0;border-bottom:1px solid #2a2a2a;"
                                    f"line-height:1.4'>"
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

                            # Title issues
                            if issues:
                                issue_lines = "".join(
                                    f"<div style='font-size:10px;color:#ffc107;"
                                    f"padding:1px 0'>• {iss}</div>"
                                    for iss in issues[:2]
                                )
                                st.markdown(
                                    f"<div style='margin:3px 0'>{issue_lines}</div>",
                                    unsafe_allow_html=True,
                                )
                        except Exception:
                            st.caption(ai_raw[:120])


# ══════════════════════════════════════════════════════════════════════════════
#  AI SUMMARY  (identical to ui_components.py render_ai_summary)
# ══════════════════════════════════════════════════════════════════════════════

def render_ai_summary(ai_raw: str, product: dict = None, uid: str = "0") -> None:
    if not ai_raw or ai_raw in ("N/A",""):
        st.info("No AI analysis in this export.")
        return
    try:
        data = json.loads(ai_raw) if isinstance(ai_raw, str) else ai_raw
    except Exception:
        st.markdown(ai_raw)
        return

    enrich    = data.get("enrichment",{})
    brand_url = enrich.get("brand_page_url","")
    img_count = enrich.get("infographic_count", 0)

    # Score badge
    score = data.get("overall_score", 0)
    grade = data.get("grade","F")
    grade_color = {
        "A":"#00c853","B":"#8bc34a",
        "C":"#ffc107","D":"#ff9800","F":"#f44336",
    }.get(grade,"#888")
    st.markdown(
        f"<div style='display:inline-block;background:{grade_color};"
        f"color:#fff;padding:6px 20px;border-radius:16px;"
        f"font-weight:700;font-size:20px;margin-bottom:12px'>"
        f"Content Grade: {grade} — {score}/100</div>",
        unsafe_allow_html=True,
    )

    summary = data.get("summary","")
    if summary:
        st.markdown(f"> {summary}")

    if brand_url:
        st.caption(f"Reference data from: [{brand_url}]({brand_url})")
    if img_count:
        st.caption(
            f"{img_count} product images downloaded and embedded in the description below"
        )

    st.markdown("---")

    # Jumia compliance rules
    rules = data.get("jumia_rules",{})
    if rules:
        st.markdown("#### Jumia Content Rules")
        rule_labels = {
            "gallery_ok":     "Gallery (min 5)",
            "description_ok": "Description (min 300)",
            "features_ok":    "Key Features (min 5)",
            "warranty_ok":    "Warranty present",
            "desc_images_ok": "Description images",
        }
        rcols = st.columns(len(rule_labels))
        for i, (key, label) in enumerate(rule_labels.items()):
            ok    = rules.get(key, None)
            color = "#00c853" if ok else "#f44336" if ok is False else "#888"
            icon  = "PASS" if ok else "FAIL" if ok is False else "N/A"
            with rcols[i]:
                st.markdown(
                    f"<div style='background:#1a1a1a;border:2px solid {color};"
                    f"border-radius:8px;padding:8px;text-align:center'>"
                    f"<div style='color:{color};font-weight:700;font-size:14px'>{icon}</div>"
                    f"<div style='font-size:11px;color:#aaa;margin-top:4px'>{label}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Infographics to create
    infographics = data.get("infographic_suggestions",[])
    if infographics:
        st.markdown("#### Infographics to Create")
        priority_color = {"High":"#f44336","Medium":"#ffc107","Low":"#8bc34a"}
        for ig in infographics:
            prio  = ig.get("priority","Medium")
            color = priority_color.get(prio,"#888")
            st.markdown(
                f"<div style='background:#1a1a1a;border-left:4px solid {color};"
                f"border-radius:0 8px 8px 0;padding:10px 14px;margin:6px 0'>"
                f"<span style='color:{color};font-size:11px;font-weight:700;"
                f"text-transform:uppercase'>{prio} PRIORITY</span>"
                f" &nbsp; <b style='font-size:14px'>{ig.get('type','')}</b><br>"
                f"<span style='color:#bbb;font-size:13px'>"
                f"{ig.get('description','')}</span></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Improved description
    improved_desc = data.get("improved_description","")
    if improved_desc:
        st.markdown("#### AI-Generated Improved Description")
        st.caption(
            f"Includes {improved_desc.count('<img')} embedded product images. "
            f"Copy the HTML into your Jumia seller portal."
        )
        with st.expander("Show full improved description", expanded=True):
            _render_html_with_images(improved_desc, uid=f"exp_{uid}")
        st.download_button(
            "Download Description as HTML",
            improved_desc.encode(),
            "improved_description.html",
            "text/html",
            key=f"dl_desc_{uid}",
            use_container_width=True,
        )

    st.markdown("---")

    # Key features
    improved_feats = data.get("improved_key_features",[])
    if improved_feats:
        st.markdown(f"#### Key Features ({len(improved_feats)} generated)")
        feat_cols = st.columns(2)
        for i, feat in enumerate(improved_feats):
            with feat_cols[i % 2]:
                st.markdown(
                    f"<div style='background:#1a1a1a;border:1px solid #333;"
                    f"border-radius:6px;padding:8px 12px;margin:4px 0;font-size:13px'>"
                    f"{i+1}. {feat}</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # Complete specs table
    complete_specs = data.get("complete_specs",{})
    if complete_specs:
        st.markdown(f"#### Complete Product Specs ({len(complete_specs)} entries)")
        st.dataframe(
            pd.DataFrame(complete_specs.items(), columns=["Specification","Value"]),
            use_container_width=True,
            hide_index=True,
            height=min(len(complete_specs) * 35 + 42, 450),
        )

    st.markdown("---")

    # Platform research
    platform = data.get("platform_research",{})
    if platform:
        st.markdown("#### What Competing Listings Include (You Are Missing)")
        p1, p2 = st.columns(2)
        with p1:
            for g in platform.get("amazon_gaps",[]):
                st.markdown(f"- **Amazon:** {g}")
        with p2:
            for g in platform.get("aliexpress_gaps",[]):
                st.markdown(f"- **AliExpress:** {g}")
        filter_specs = platform.get("specs_buyers_filter_by",[])
        if filter_specs:
            st.markdown("**Specs buyers filter by:**")
            pills = "  ".join(
                f"<code style='background:#2a2a2a;padding:2px 8px;"
                f"border-radius:4px;font-size:12px'>{s}</code>"
                for s in filter_specs
            )
            st.markdown(pills, unsafe_allow_html=True)

    st.markdown("---")

    # Title
    st.markdown("#### Title")
    t1, t2 = st.columns(2)
    with t1:
        issues = list(data.get("title_issues",[]))
        if data.get("has_color_in_title"):
            issues = ["Color/variant in title — move to product variations"] + issues
        for iss in (issues or ["No issues found"]):
            st.markdown(f"- {iss}")
    with t2:
        improved_title = data.get("improved_title","")
        if improved_title:
            st.markdown("**Suggested:**")
            st.code(improved_title)

    st.markdown("---")

    # Seller actions
    recs = data.get("seller_recommendations",[])
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
    keywords = data.get("keywords",[])
    if keywords:
        st.markdown("#### Search Keywords")
        pills = "  ".join(
            f"<code style='background:#2a2a2a;padding:3px 10px;"
            f"border-radius:4px;font-size:13px'>{k}</code>"
            for k in keywords
        )
        st.markdown(pills, unsafe_allow_html=True)

    st.markdown("---")

    # Marketplace links
    links      = dict(data.get("marketplace_links",{}))
    brand_site = data.get("official_brand_site","")
    if brand_site:
        links["Official Site"] = brand_site
    if links:
        st.markdown("#### Search This Product On")
        link_items = list(links.items())
        for start in range(0, len(link_items), 5):
            chunk = link_items[start: start + 5]
            lc    = st.columns(5)
            for i, (lname, lurl) in enumerate(chunk):
                with lc[i]:
                    st.markdown(
                        f"<a href='{lurl}' target='_blank' "
                        f"style='display:block;text-align:center;"
                        f"background:#2a2a2a;border:1px solid #444;"
                        f"border-radius:8px;padding:8px 4px;"
                        f"color:#4fc3f7;text-decoration:none;"
                        f"font-size:12px;margin:2px'>{lname}</a>",
                        unsafe_allow_html=True,
                    )


# ══════════════════════════════════════════════════════════════════════════════
#  PDP CARD  (identical to ui_components.py display_pdp_card)
# ══════════════════════════════════════════════════════════════════════════════

def display_pdp_card(row, card_idx: int = 0) -> None:
    uid     = str(card_idx)
    name    = safe_str(row.get("Title", row.get("Product Name","")))
    price   = safe_float(row.get("Price (KSh)"))
    orig    = safe_float(row.get("Original Price (KSh)"))
    disc    = safe_str(row.get("Discount",""))
    rating  = safe_float(row.get("Rating"))
    reviews = safe_int(row.get("Reviews"), 0)
    sku     = safe_str(row.get("SKU","N/A"))
    cat     = safe_str(row.get("Category","N/A"))
    brand   = safe_str(row.get("Brand","N/A"))
    seller  = safe_str(row.get("Seller Name","N/A"))
    badges  = safe_str(row.get("Badges",""))
    url     = safe_str(row.get("URL",""))
    ai_raw  = safe_str(row.get("AI Summary",""))

    img_raw  = safe_str(row.get("Image URLs",""))
    img_urls = [
        u.strip() for u in img_raw.split(" | ")
        if u.strip() and img_raw != "N/A"
    ]

    with st.container(border=True):

        # Header
        h_left, h_right = st.columns([4, 1])
        with h_left:
            st.markdown(f"### {name}")
            meta = []
            if sku    != "N/A": meta.append(f"SKU: `{sku}`")
            if cat    != "N/A": meta.append(f"Category: {cat}")
            if brand  != "N/A": meta.append(f"Brand: {brand}")
            if seller != "N/A": meta.append(f"Seller: {seller}")
            if meta:
                st.caption("  |  ".join(meta))
            if badges and badges != "N/A":
                badge_pills = "  ".join(
                    f"<span style='background:#ff6900;color:#fff;"
                    f"font-size:11px;padding:2px 8px;border-radius:10px'>"
                    f"{b.strip()}</span>"
                    for b in badges.split("|") if b.strip()
                )
                st.markdown(badge_pills, unsafe_allow_html=True)

        with h_right:
            if price is not None:
                if orig is not None and orig > price:
                    st.markdown(
                        f"<s style='color:#888;font-size:13px'>"
                        f"KSh {int(orig):,}</s><br>"
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
            if url and url != "N/A":
                st.markdown(f"[View on Jumia]({url})")

        st.markdown("---")

        # Tabs
        tab_images, tab_desc, tab_specs, tab_ai = st.tabs([
            f"Gallery ({len(img_urls)})",
            "Description",
            "Specifications",
            "AI Analysis",
        ])

        # Gallery tab
        with tab_images:
            if img_urls:
                render_image_lightbox(img_urls, name, f"pdp_{uid}")
            else:
                st.info("No gallery images found.")

        # Description tab
        with tab_desc:
            ai_improved = ""
            if has_ai_summary(ai_raw):
                try:
                    ai_improved = json.loads(ai_raw).get("improved_description","")
                except Exception:
                    pass

            if ai_improved:
                st.markdown(
                    f"**AI-Improved Description "
                    f"({ai_improved.count('<img')} images embedded)**"
                )
                _render_html_with_images(ai_improved, uid=f"desc_{uid}")
                st.markdown("---")
                st.markdown("**Original Description**")

            desc_html = safe_str(row.get("Description HTML",""))
            desc_text = safe_str(row.get("Description",""))
            if desc_html and desc_html != "N/A":
                _render_html_with_images(desc_html, uid=f"orig_{uid}", min_height=200)
            elif desc_text and desc_text != "N/A":
                st.markdown(desc_text)
            else:
                st.info("No description available.")

            witb = safe_str(row.get("What's in the Box",""))
            if witb and witb != "N/A":
                st.markdown("**What's in the Box**")
                for item in witb.split("|"):
                    if item.strip():
                        st.markdown(f"- {item.strip()}")

            ai_feats = []
            if has_ai_summary(ai_raw):
                try:
                    ai_feats = json.loads(ai_raw).get("improved_key_features",[])
                except Exception:
                    pass
            kf = safe_str(row.get("Key Features",""))
            if ai_feats:
                st.markdown(f"**Key Features (AI-completed — {len(ai_feats)})**")
                for feat in ai_feats:
                    st.markdown(f"- {feat}")
            elif kf and kf != "N/A":
                st.markdown("**Key Features**")
                for feat in kf.split("|"):
                    if feat.strip():
                        st.markdown(f"- {feat.strip()}")

        # Specifications tab
        with tab_specs:
            ai_specs = {}
            if has_ai_summary(ai_raw):
                try:
                    ai_specs = json.loads(ai_raw).get("complete_specs",{})
                except Exception:
                    pass

            spec_dict = {
                k.replace("Spec: ",""): v
                for k, v in row.items()
                if str(k).startswith("Spec: ") and safe_str(v) not in ("N/A","")
            }
            merged = {**spec_dict, **ai_specs}

            if merged:
                st.dataframe(
                    pd.DataFrame(merged.items(), columns=["Specification","Value"]),
                    use_container_width=True,
                    hide_index=True,
                    height=min(len(merged) * 35 + 42, 450),
                )
                if ai_specs:
                    st.caption(
                        f"{len(spec_dict)} from listing + "
                        f"{len(ai_specs)} AI-completed = {len(merged)} total"
                    )
            else:
                specs_raw = safe_str(row.get("Specifications (Raw)",""))
                if specs_raw and specs_raw != "N/A":
                    for pair in specs_raw.split("|"):
                        if ":" in pair:
                            k, _, v = pair.partition(":")
                            st.markdown(f"**{k.strip()}:** {v.strip()}")
                else:
                    st.info("No specifications found.")

            warrant = safe_str(row.get("Warranty Info",""))
            gtin    = safe_str(row.get("GTIN",""))
            vars_   = safe_str(row.get("Variations",""))
            if warrant and warrant != "N/A": st.markdown(f"**Warranty:** {warrant}")
            if gtin    and gtin    != "N/A": st.markdown(f"**GTIN:** {gtin}")
            if vars_   and vars_   != "N/A":
                st.markdown("**Variations:**")
                for v in vars_.split("|"):
                    if v.strip(): st.markdown(f"- {v.strip()}")

        # AI Analysis tab
        with tab_ai:
            if has_ai_summary(ai_raw):
                render_ai_summary(ai_raw, dict(row), uid=uid)
            else:
                st.info("No AI analysis in this export.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ══════════════════════════════════════════════════════════════════════════════

st.title("🛍️ Jumia Product Viewer")
st.caption("Upload an export file to browse products with full AI analysis, images, and descriptions.")

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
    search = st.text_input("Filter by name", placeholder="Name, brand, SKU...")

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

if sel_cats   and "Category" in filt.columns:
    filt = filt[filt["Category"].isin(sel_cats)]
if sel_brands and "Brand" in filt.columns:
    filt = filt[filt["Brand"].isin(sel_brands)]
if sel_grades and "AI Summary" in filt.columns:
    filt = filt[filt["AI Summary"].apply(
        lambda x: json.loads(x).get("grade","") if has_ai_summary(x) else ""
    ).isin(sel_grades)]
if price_range and "Price (KSh)" in filt.columns:
    filt = filt.copy()
    filt["_p"] = pd.to_numeric(filt["Price (KSh)"], errors="coerce")
    filt = filt[(filt["_p"] >= price_range[0]) & (filt["_p"] <= price_range[1])].drop(columns=["_p"])

# Sort
if sort_by == "Price: Low to High" and "Price (KSh)" in filt.columns:
    filt = filt.copy(); filt["_s"] = pd.to_numeric(filt["Price (KSh)"],errors="coerce"); filt = filt.sort_values("_s").drop(columns=["_s"])
elif sort_by == "Price: High to Low" and "Price (KSh)" in filt.columns:
    filt = filt.copy(); filt["_s"] = pd.to_numeric(filt["Price (KSh)"],errors="coerce"); filt = filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "Rating" and "Rating" in filt.columns:
    filt = filt.copy(); filt["_s"] = pd.to_numeric(filt["Rating"],errors="coerce"); filt = filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "AI Score" and "AI Summary" in filt.columns:
    filt = filt.copy()
    filt["_s"] = filt["AI Summary"].apply(
        lambda x: json.loads(x).get("overall_score",0) if has_ai_summary(x) else 0
    )
    filt = filt.sort_values("_s",ascending=False).drop(columns=["_s"])
elif sort_by == "Name A-Z":
    nc = "Product Name" if "Product Name" in filt.columns else "Title"
    if nc in filt.columns: filt = filt.sort_values(nc)

# ─── Stats bar ────────────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Summary")
price_s    = pd.to_numeric(filt.get("Price (KSh)", pd.Series()), errors="coerce").dropna()
rating_num = pd.to_numeric(filt.get("Rating", pd.Series(dtype=float)), errors="coerce")
rated      = filt[rating_num.notna()].copy()
if len(rated): rated["_r"] = rating_num[rating_num.notna()]
ai_done    = filt["AI Summary"].apply(has_ai_summary).sum() if "AI Summary" in filt.columns else 0
scores     = filt["AI Summary"].apply(
    lambda x: json.loads(x).get("overall_score",0) if has_ai_summary(x) else None
).dropna() if "AI Summary" in filt.columns else pd.Series()

c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("Products",     len(filt))
c2.metric("Avg Price",    f"KSh {price_s.mean():,.0f}" if len(price_s) else "N/A")
c3.metric("Min Price",    f"KSh {price_s.min():,.0f}"  if len(price_s) else "N/A")
c4.metric("Max Price",    f"KSh {price_s.max():,.0f}"  if len(price_s) else "N/A")
c5.metric("Avg Rating",   f"{rated['_r'].mean():.1f}/5" if len(rated) else "N/A")
c6.metric("AI Summaries", f"{ai_done}/{len(filt)}")

if filt.empty:
    st.warning("No products match your filters.")
    st.stop()

filt_reset = filt.reset_index(drop=True)

# ─── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["Products", "PDP Details", "Analytics"])

# ── Tab 1: Products ───────────────────────────────────────────────────────────
with tab1:
    t1_c1, t1_c2 = st.columns([3, 1])
    with t1_c1:
        search_filter = st.text_input("Filter by name", key="tab1_search")
    with t1_c2:
        view_mode = st.radio("View", ["Table","Cards"], horizontal=True)

    tab1_df = (
        filt_reset[filt_reset["Product Name"].str.contains(search_filter, case=False, na=False)]
        if search_filter else filt_reset
    )

    if view_mode == "Table":
        listing_cols = [
            "Product Name","Price (KSh)","Original Price (KSh)","Savings (KSh)",
            "Discount","Rating","Reviews","Official Store","Warranty",
            "Jumia Express","AI Summary","URL",
        ]
        if "Search Query" in filt_reset.columns:
            listing_cols = ["Search Query","Search Type"] + listing_cols
        cols_show = [c for c in listing_cols if c in filt_reset.columns]
        st.dataframe(
            tab1_df[cols_show].reset_index(drop=True),
            use_container_width=True,
            height=520,
            column_config={
                "URL":                  st.column_config.LinkColumn("URL"),
                "Price (KSh)":          st.column_config.NumberColumn(format="KSh %d"),
                "Original Price (KSh)": st.column_config.NumberColumn(format="KSh %d"),
                "Savings (KSh)":        st.column_config.NumberColumn(format="KSh %d"),
                "Rating":               st.column_config.NumberColumn(format="%.1f"),
                "AI Summary":           st.column_config.TextColumn("AI Summary", width="large"),
            },
        )
    else:
        cpr = st.select_slider("Cards per row", [2,3,4,5], value=4)
        render_card_grid(tab1_df.reset_index(drop=True), cpr)

    st.caption(f"Showing {len(tab1_df)} of {len(filt_reset)} products · {ai_done} AI summaries")

# ── Tab 2: PDP Details ────────────────────────────────────────────────────────
with tab2:
    s2  = st.text_input("Filter", key="tab2_search")
    vdf = (
        filt_reset[filt_reset["Product Name"].str.contains(s2, case=False, na=False)]
        if s2 else filt_reset
    )
    if ai_done > 0:
        st.success(f"{ai_done}/{len(filt_reset)} products have AI summaries")
    else:
        st.info("No AI summaries in this export.")

    for card_idx, (_, row) in enumerate(vdf.head(50).iterrows()):
        display_pdp_card(row, card_idx=card_idx)

# ── Tab 3: Analytics ──────────────────────────────────────────────────────────
with tab3:
    r1, r2 = st.columns(2)
    with r1:
        st.subheader("Price Distribution")
        if len(price_s):
            bc = pd.cut(price_s, bins=10).value_counts().sort_index()
            st.bar_chart(pd.DataFrame({"Products": bc.values}, index=[str(b) for b in bc.index]))
        else:
            st.info("No price data.")
    with r2:
        st.subheader("Discounted vs Full Price")
        if "Discount" in filt_reset.columns:
            disc_counts = (filt_reset["Discount"] != "N/A").value_counts()
            st.bar_chart(pd.DataFrame(
                {"Products": disc_counts.values},
                index=["Discounted" if i else "Full Price" for i in disc_counts.index],
            ))

    r3, r4 = st.columns(2)
    with r3:
        st.subheader("Variation Counts")
        if "Variations Count" in filt_reset.columns:
            vc = pd.to_numeric(filt_reset["Variations Count"], errors="coerce").fillna(0).astype(int)
            st.bar_chart(vc.value_counts().sort_index())
    with r4:
        st.subheader("Top Sellers")
        if "Seller Name" in filt_reset.columns:
            sc = filt_reset[filt_reset["Seller Name"] != "N/A"]["Seller Name"].value_counts().head(10)
            if len(sc): st.bar_chart(sc)

    st.subheader("Top 10 Rated Products")
    if len(rated) and "_r" in rated.columns:
        st.dataframe(
            rated.nlargest(10, "_r")[
                [c for c in ["Product Name","Price (KSh)","Rating","Reviews","AI Summary","URL"]
                 if c in rated.columns]
            ].reset_index(drop=True),
            use_container_width=True,
            column_config={
                "URL":         st.column_config.LinkColumn("URL"),
                "Price (KSh)": st.column_config.NumberColumn(format="KSh %d"),
                "Rating":      st.column_config.NumberColumn(format="%.1f"),
                "AI Summary":  st.column_config.TextColumn("AI Summary", width="large"),
            },
        )
    else:
        st.info("No rated products found.")
