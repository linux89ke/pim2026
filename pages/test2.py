import streamlit as st
import pandas as pd
import asyncio
import json
import time
from playwright.async_api import async_playwright

st.set_page_config(
    page_title="Jumia SKU Finder",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Jumia SKU Finder — Automated")
st.caption("Drives the real sp-sku-finder page and intercepts results")

# ─── Core scraper ────────────────────────────────────────────────────────────

async def scrape_sku_finder(queries: list[str], delay: float = 2.0):
    all_results = []
    captured_api_url = None

    async with async_playwright() as p:
        # Launching with specific arguments to bypass basic bot detection
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security"
            ]
        )
        
        # Using the exact headers from your network trace
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={
                "accept": "*/*",
                "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
                "referer": "https://www.jumia.co.ke/fragment/contents/sp-sku-finder/?lang=en",
                "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin"
            }
        )
        page = await context.new_page()

        # Mask Playwright's default webdriver flag
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        # ── Intercept XHR/fetch responses to find the real API ──
        intercepted = []

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            # catch any JSON response that isn't a tracker/analytics call
            if "application/json" in ct and any(
                kw in url for kw in ["catalog", "search", "product", "sku", "suggest"]
            ):
                try:
                    data = await response.json()
                    intercepted.append({"url": url, "data": data})
                except Exception:
                    pass

        page.on("response", on_response)

        # ── Navigate to the real page revealed in the trace ──
        await page.goto(
            "https://www.jumia.co.ke/fragment/contents/sp-sku-finder/?lang=en",
            wait_until="domcontentloaded",
            timeout=30000
        )

        # Accept cookies if banner appears
        try:
            await page.click("button:has-text('Accept')", timeout=3000)
        except Exception:
            pass

        # ── Find the search input ──
        search_selector = (
            "input[type='search'], "
            "input[placeholder*='search' i], "
            "input[placeholder*='product' i], "
            "input[placeholder*='name' i], "
            "input[placeholder*='sku' i], "
            ".search-form input, "
            "form input[type='text']"
        )

        try:
            await page.wait_for_selector(search_selector, timeout=15000)
        except Exception:
            # take a screenshot to debug what loaded
            await page.screenshot(path="debug.png")
            await browser.close()
            return [], None, "Could not find search input. Check debug.png"

        search_input = page.locator(search_selector).first

        # ── Loop through each query ──
        for query in queries:
            intercepted.clear()

            # Ensure input is cleared before typing
            await search_input.click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            
            await search_input.fill(query)
            await search_input.press("Enter")
            
            # Allow time for results and intercepts to process
            await page.wait_for_timeout(int(delay * 1000) + 1500)

            # Also try clicking a Search button if Enter didn't trigger
            try:
                await page.click("button[type='submit'], button:has-text('Search')", timeout=1000)
                await page.wait_for_timeout(1500)
            except Exception:
                pass

            # ── Parse intercepted JSON ──
            products_found = []
            for item in intercepted:
                data = item["data"]
                captured_api_url = item["url"]
                products = _extract_products(data)
                for p in products:
                    p["_query"] = query
                    p["_source_url"] = captured_api_url
                products_found.extend(products)

            # ── Fallback: scrape DOM if no JSON was intercepted ──
            if not products_found:
                dom_results = await _scrape_dom(page, query)
                products_found.extend(dom_results)

            all_results.extend(products_found)
            await asyncio.sleep(delay)

        await browser.close()

    return all_results, captured_api_url, None


def _extract_products(data: dict | list) -> list[dict]:
    """Flexibly extract product records from various JSON structures."""
    products = []

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Try common keys
        items = (
            data.get("products")
            or data.get("items")
            or data.get("data")
            or data.get("results")
            or data.get("hits")
            or []
        )
        if isinstance(items, dict):
            items = list(items.values())
    else:
        return products

    for item in items:
        if not isinstance(item, dict):
            continue
        products.append({
            "Name":      item.get("name") or item.get("title") or item.get("product_name", ""),
            "Jumia SKU": item.get("sku") or item.get("shop_sku") or item.get("id", ""),
            "Seller SKU":item.get("seller_sku") or item.get("sellerSku", ""),
            "Price":     item.get("price") or item.get("special_price", ""),
            "Brand":     item.get("brand") or item.get("brand_name", ""),
            "Category":  item.get("category") or item.get("categories", ""),
            "Status":    item.get("status", ""),
            "URL":       item.get("url") or item.get("product_url", ""),
            "Image":     item.get("image") or item.get("thumbnail", ""),
        })

    return products


async def _scrape_dom(page, query: str) -> list[dict]:
    """Fallback DOM scraper if JSON interception fails."""
    results = []
    cards = await page.locator(
        "article, [class*='product'], [class*='item'], [class*='result']"
    ).all()
    
    for card in cards[:50]:
        try:
            text = await card.inner_text()
            href = ""
            a = card.locator("a").first
            if await a.count() > 0:
                href = await a.get_attribute("href") or ""
            
            # Clean up the multiline text blob from the card
            clean_text = text[:80].strip().replace('\n', ' - ')
            
            results.append({
                "Name": clean_text,
                "Jumia SKU": "",
                "Seller SKU": "",
                "Price": "",
                "Brand": "",
                "Category": "",
                "Status": "",
                "URL": href,
                "Image": "",
                "_query": query,
                "_source_url": page.url,
                "_note": "DOM fallback — open DevTools to find exact API URL"
            })
        except Exception:
            pass
    return results


# ─── Streamlit UI ─────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["🔎 Single Search", "📋 Bulk Search"])

with tab1:
    q = st.text_input("Product name", placeholder="e.g. Samsung Galaxy A15")
    col1, col2 = st.columns([1, 3])
    with col1:
        delay1 = st.number_input("Wait (sec)", 1.0, 10.0, 3.0, key="d1")
    if st.button("Search", type="primary"):
        if q:
            with st.spinner("Opening Jumia SKU Finder..."):
                results, api_url, error = asyncio.run(
                    scrape_sku_finder([q], delay=delay1)
                )
            if error:
                st.error(error)
            elif results:
                if api_url:
                    st.info(f"✅ API captured: `{api_url}`")
                df = pd.DataFrame(results)
                st.dataframe(df, width='stretch')
                st.download_button("⬇️ CSV", df.to_csv(index=False), f"{q}.csv")
            else:
                st.warning("No results. Try increasing wait time.")

with tab2:
    bulk = st.text_area("Product list")