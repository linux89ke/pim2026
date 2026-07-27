"""
Carrefour Kenya (carrefour.ke) API scraper
--------------------------------------------
Reverse-engineered from a HAR capture of the site's XHR traffic.
The site is a Majid Al Futtaim headless-commerce storefront: no login
or session cookie is required, requests just need a specific set of
custom headers and the API treats you as an anonymous user.

Captured & confirmed endpoints:
  - GET  /api/v1/menu                                        -> full nested category tree w/ product counts
  - POST /mafrp/api/v1/search/listing/category/{categoryId}  -> THE real PLP/category listing endpoint,
                                                                  clean JSON, paginated via "currentPage" in
                                                                  the POST body (pageSize fixed at 60).
                                                                  This is what "lazy load more" actually calls.
  - POST /api/v5/relevance/products/{productId}               -> related/recommended products (rich product fields)
  - POST /mafrp/api/v1/search/carousel/rr                      -> product carousels for a given product/page

Search / product-listing endpoint (added from a second HAR capture):
  - GET /mafken/en/search?keyword={query}&_rsc={anything}
    This is NOT a clean JSON API - it's a Next.js React Server Component
    (RSC) streaming payload (`text/x-component`). The trick: it still
    embeds full product-card JSON objects inline (productId, productName,
    sellingPrice, markedPrice, currency, productCategory, productUrl,
    imageUrl, stock status, etc). We don't need to parse the whole RSC
    tree - we just regex/scan for repeated product-card JSON fragments
    and decode each one as standalone JSON.

    Key finding: the Next.js-specific headers seen in the HAR
    (`next-router-state-tree`, `next-url`, `_rsc` query param) are
    cache/routing hints, NOT auth gates. The only header that actually
    matters to get the RSC payload instead of full HTML is `rsc: 1`.

    Note: this /search route's own pagination wasn't captured, but you
    don't need it - use get_category_products() below instead, which hits
    the real paginated listing endpoint directly once you have a category
    ID (from get_category_tree()/flatten_categories(), or from a product's
    productCategory field).

Run this from a machine that can reach carrefour.ke directly (not a
sandboxed environment with an egress allowlist).
"""

import requests
import time
import random
import re
import json

BASE_URL = "https://www.carrefour.ke"

# Nairobi coordinates - required by the API, any valid KE lat/lng works
LATITUDE = "-1.2672236834605626"
LONGITUDE = "36.810586556760555"

HEADERS = {
    "accept": "application/json",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "appflavour": "carrefour",
    "appid": "Reactweb",
    "channel": "c4online",
    "content-type": "application/json; charset=utf-8",
    "currency": "KES",
    "env": "prod",
    "lang": "en",
    "langcode": "en",
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "hashedemail": "anonymous",
    "userid": "anonymous",
    "posinfo": "food=684_Zone01,express=KE4_Zone01,nonfood=681_Zone01",
    "producttype": "ANY",
    "servicetypes": "SLOTTED|DEFAULT|MKP_GLOBAL",
    "storeid": "mafken",
    "x-maf-account": "carrefour",
    "x-maf-env": "prod",
    "x-maf-tenant": "mafretail",
    "x-maf-revamp": "true",
    "x-requested-with": "XMLHttpRequest",
    "referer": f"{BASE_URL}/mafken/en",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
}


def get_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_category_tree(session):
    """Fetch the full nested category tree with product counts per category."""
    url = f"{BASE_URL}/api/v1/menu"
    params = {"lang": "en", "latitude": LATITUDE, "longitude": LONGITUDE}
    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def flatten_categories(tree, parent_path=""):
    """Recursively flatten the nested category tree into flat rows."""
    rows = []
    for node in tree:
        name = node.get("name") or node.get("title")
        cat_id = node.get("id")
        count = node.get("count")
        level = node.get("level")
        path = f"{parent_path} > {name}" if parent_path else name
        rows.append({
            "id": cat_id,
            "name": name,
            "level": level,
            "count": count,
            "path": path,
            "url": node.get("url"),
        })
        children = node.get("children") or []
        if children:
            rows.extend(flatten_categories(children, path))
    return rows


def get_related_products(session, product_id, category_id=""):
    """
    Hits the recommendation endpoint for a known product ID.
    Useful for pulling product data (price, ean, image, category path)
    without needing the PLP search endpoint, if you already have seed
    product IDs (e.g. scraped from sitemap or category page HTML).
    """
    url = f"{BASE_URL}/api/v5/relevance/products/{product_id}"
    params = {
        "categoryId": category_id,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
    }
    payload = {
        "placements": [
            {"placement": "item_page.frequently_bought_together_web"},
            {"placement": "item_page.sponsored_products"},
        ],
        "displayCurr": "KES",
        "locationParams": {"latitude": LATITUDE, "longitude": LONGITUDE},
        "lang": "en",
        "requireSponsProducts": False,
    }
    resp = session.post(url, params=params, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def search_products(session, keyword):
    """
    Hits the Next.js RSC search route directly and extracts embedded
    product-card JSON objects. This is the closest thing to a real
    PLP/search-grid endpoint found so far.

    Returns a list of dicts with the key product-card fields.
    """
    url = f"{BASE_URL}/mafken/en/search"
    params = {"keyword": keyword, "_rsc": "abc123"}  # value doesn't matter, just needs to be present
    headers = {
        "accept": "*/*",
        "rsc": "1",  # <- the header that actually matters
        "referer": f"{BASE_URL}/mafken/en/search?keyword={keyword}",
    }
    resp = session.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    raw = resp.text

    return _extract_products_from_rsc(raw)


def _extract_products_from_rsc(raw_text):
    """
    Scans a raw RSC text payload for embedded product-card JSON objects
    and parses each into a clean dict. Product cards are identified by
    the presence of a "productId" key; we locate each candidate object's
    start brace and use a brace-matching scan (not just regex) since the
    JSON is arbitrarily nested and regex alone can't safely capture it.
    """
    products = []
    seen_ids = set()

    # Each product card's core fields live inside an
    # "additionalAttributes": { ... "productId": "...", "productName": ... }
    # object. Anchor on that key, then brace-match forward from its '{'.
    anchor = '"additionalAttributes":'
    pos = 0
    while True:
        idx = raw_text.find(anchor, pos)
        if idx == -1:
            break
        brace_start = raw_text.find("{", idx + len(anchor))
        pos = idx + len(anchor)
        if brace_start == -1:
            continue
        obj_str, end = _extract_balanced_json(raw_text, brace_start)
        if obj_str is None:
            continue
        pos = end
        try:
            obj = json.loads(obj_str)
        except json.JSONDecodeError:
            continue

        pid = obj.get("productId")
        if not pid or pid in seen_ids:
            continue
        if "productName" not in obj and "sellingPrice" not in obj:
            continue  # skip fragments that matched but aren't real product cards

        seen_ids.add(pid)
        products.append({
            "productId": pid,
            "productName": obj.get("productName"),
            "sellingPrice": obj.get("sellingPrice"),
            "markedPrice": obj.get("markedPrice"),
            "currency": obj.get("currency"),
            "productCategory": obj.get("productCategory"),
            "productUrl": obj.get("productUrl"),
            "imageUrl": obj.get("imageUrl"),
            "stock": (obj.get("stock") or {}).get("stockLevelStatus"),
            "isSponsored": obj.get("isSponsored"),
        })

    return products


def _extract_balanced_json(text, start):
    """
    Given a start index pointing at a '{', scan forward tracking brace
    depth (respecting quoted strings/escapes) to find the matching '}'.
    Returns (json_substring, end_index) or (None, None) if unbalanced.
    """
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        c = text[i]
        if in_string:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[start:i + 1], i + 1
    return None, None


def get_category_products(session, category_id, max_pages=None, delay=True):
    """
    Real PLP/search-grid endpoint (found via a third HAR capture).

    POST /mafrp/api/v1/search/listing/category/{categoryId}
    Body: {"currentPage": N, "pageType": "PLP", "sortBy": "relevance", ...}

    The site's "lazy load more" button is just this same call with
    currentPage incremented - there's no infinite-scroll cursor/offset,
    just plain page numbers. pageSize is fixed at 60.

    Response JSON includes data.pagination = {pageSize, totalPages,
    totalResults, currentPage}, so we can page until totalPages is hit
    (or until a page returns 0 products, as a safety net).

    Returns a list of product dicts (same shape as search_products()).
    """
    url = f"{BASE_URL}/mafrp/api/v1/search/listing/category/{category_id}"
    params = {
        "categoryId": category_id,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
    }

    all_products = []
    page = 1
    total_pages = None

    while True:
        if max_pages and page > max_pages:
            break

        payload = {
            "needOOSProducts": False,
            "needVariantsData": False,
            "verticalCategory": True,
            "pageType": "PLP",
            "currentPage": page,
            "filter": "",
            "sortBy": "relevance",
            "requireSponsProducts": False,
        }
        resp = session.post(url, params=params, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        pagination = data.get("data", {}).get("pagination", {})
        total_pages = pagination.get("totalPages", total_pages)

        page_products = _extract_products_from_rsc(resp.text)
        if not page_products:
            break
        all_products.extend(page_products)

        print(f"  [{category_id}] page {page}/{total_pages}: "
              f"+{len(page_products)} products (total so far: {len(all_products)})")

        if total_pages and page >= total_pages:
            break

        page += 1
        if delay:
            polite_sleep()

    return all_products


def polite_sleep(min_s=1.0, max_s=2.5):
    time.sleep(random.uniform(min_s, max_s))


if __name__ == "__main__":
    session = get_session()

    print("Fetching category tree...")
    tree = get_category_tree(session)
    flat = flatten_categories(tree)

    print(f"Found {len(flat)} categories (all levels).\n")
    for row in flat[:20]:
        print(f"[L{row['level']}] {row['path']}  (count={row['count']}, id={row['id']})")

    # Example: pull related products for a known product ID (from your HAR: 28484)
    print("\nFetching related products for product 28484...")
    related = get_related_products(session, "28484", category_id="Cleaning & Household")
    placements = related.get("placements", [])
    for p in placements:
        for prod in p.get("recommendedProducts", [])[:3]:
            print(f"  - {prod.get('name')} (id={prod.get('id')}, ean={prod.get('ean')})")

    # Example: pull full paginated product listing for a category
    # (from your HAR: NFKEN3020000 = a cleaning/household subcategory, 984 products / 17 pages)
    print("\nFetching full product listing for category NFKEN3020000...")
    products = get_category_products(session, "NFKEN3020000", max_pages=2)  # remove max_pages to get all
    print(f"\nTotal products collected: {len(products)}")
    for p in products[:5]:
        print(f"  - {p['productName']} - {p['sellingPrice']} {p['currency']} ({p['stock']})")

    # Example: keyword search (uses the RSC search route)
    print("\nSearching for 'blueband'...")
    results = search_products(session, "blueband")
    for p in results[:5]:
        print(f"  - {p['productName']} - {p['sellingPrice']} {p['currency']}")