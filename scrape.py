# scrape_juliet.py
import json
import sys
import re
import requests
import os
from playwright.sync_api import sync_playwright

# Install Playwright browsers and dependencies (for cloud deployment)
try:
    os.system('playwright install')
    os.system('playwright install-deps')
except Exception as e:
    print(f"Warning: Playwright installation commands failed: {e}")

# -------------------------
# Broadway Inbound show extraction (HTTP-based, no browser needed)
# -------------------------

def get_broadway_shows() -> tuple:
    """
    Fetch Broadway shows from https://www.broadwayinbound.com/shows by parsing 
    the embedded JavaScript array directly from the HTML source.
    
    Returns:
        Tuple: (shows_list, debug_messages)
        shows_list: List of dicts: [{"title": "Show Name", "url": "https://...", "firstPerformance": "...", "onSaleThrough": "..."}, ...]
        debug_messages: List of debug strings
    """
    base_url = "https://www.broadwayinbound.com"
    debug = []
    shows = []

    try:
        debug.append(f"Fetching {base_url}/shows...")
        resp = requests.get(f"{base_url}/shows", timeout=15)
        resp.raise_for_status()
        html = resp.text
        debug.append("✅ Successfully fetched page HTML")

        # Pull out the JS array assigned to "var shows = [...]"
        match = re.search(r"var\s+shows\s*=\s*(\[\s*\{.*?\}\s*\]);", html, re.DOTALL)
        if not match:
            debug.append("❌ Could not find the shows array in page source")
            return shows, debug

        debug.append("✅ Found shows array, parsing JSON...")
        try:
            shows_data = json.loads(match.group(1))
            debug.append(f"✅ Parsed {len(shows_data)} shows from JSON")
        except json.JSONDecodeError as e:
            debug.append(f"❌ JSON parse error: {e}")
            return shows, debug

        # Filter and process shows
        for show in shows_data:
            # Only include shows that have pricing (ShowLetUsKnow: false)
            # Shows with ShowLetUsKnow: true don't have pricing yet
            if show.get('ShowLetUsKnow', True) == False:
                slug = show.get('Url') or show.get('ShowUrlEN') or show.get('url') or show.get('slug')
                title = show.get('ShowName') or show.get('SortName') or show.get('title') or show.get('name')
                first_performance = show.get('FirstPerformance', '')
                on_sale_through = show.get('OnSaleThrough', '')
                
                if slug and title:
                    # Ensure slug starts with /
                    if not slug.startswith('/'):
                        slug = '/' + slug
                    
                    full_url = base_url + slug
                    shows.append({
                        'title': title.strip(),
                        'url': full_url,
                        'firstPerformance': first_performance,
                        'onSaleThrough': on_sale_through
                    })
                    debug.append(f"• {title.strip()} → {full_url} (First: {first_performance}, Sale Through: {on_sale_through})")
            else:
                # Debug: show which shows are being skipped
                title = show.get('ShowName') or show.get('SortName') or show.get('title') or show.get('name') or 'Unknown'
                debug.append(f"⏭ Skipped show (no pricing yet): {title}")

        debug.append(f"🎭 Total shows with pricing found: {len(shows)}")

    except requests.RequestException as e:
        debug.append(f"❌ HTTP request failed: {e}")
    except Exception as e:
        debug.append(f"❌ Unexpected error: {e}")

    return shows, debug

# -------------------------
# Core scraping routine
# -------------------------

def scrape_pricing(url: str, from_date: str, to_date: str) -> dict:
    """
    1) Navigate to `url`.
    2) Find and click the Pricing Grid tab (#pricing-grid-tab-trigger).
    3) Wait for Knockout to render the #pricing-grid section.
    4) Extract each "Description" and "Price" pair from:
         #pricing-grid .product-data-column.product-section span
         #pricing-grid .product-data-column.price span
    5) Return a dict:
       {
         "scrapedData": [ { "description": "...", "price": "..." }, ... ],
         "clickSuccessful": True/False,
         "error": None or "error message"
       }
    """
    result = {
        "scrapedData": [],
        "clickSuccessful": False,
        "error": None
    }

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        )
        page = browser.new_page()

        # 1) Go to the page
        page.goto(url)

        # 2) Click the "Pricing Grid" tab
        trigger = page.query_selector("#pricing-grid-tab-trigger")
        if not trigger:
            result["error"] = "Could not find the element with id 'pricing-grid-tab-trigger'."
            browser.close()
            return result

        try:
            trigger.click()
            result["clickSuccessful"] = True
        except Exception as e:
            result["error"] = f"Click failed: {e}"
            browser.close()
            return result

        # 3) Wait for the pricing grid wrapper and its date inputs to appear (important because the
        #    page contains duplicate IDs for these inputs in other calendar views).
        try:
            page.wait_for_selector("#pricing-grid", state="attached", timeout=15000)
            page.wait_for_selector("#pricing-grid input#fromDate", timeout=15000)
            page.wait_for_selector("#pricing-grid input#toDate", timeout=15000)
        except:
            result["error"] = "Date picker inputs did not appear after opening pricing grid."
            browser.close()
            return result

        # 3b) Fill the date range picked by the user. The inputs are readonly, so we drop that attribute and
        #      set the value via JavaScript then dispatch the appropriate events so Knockout/Bootstrap recognise
        #      the change.

        def _set_date_input(selector: str, value: str):
            # Playwright's evaluate allows only one extra argument, so we pass an object { sel, val }
            page.evaluate(
                "({ sel, val }) => {\n"  # JS function receiving an object
                "  const el = document.querySelector(sel);\n"
                "  if (!el) return;\n"
                "  el.removeAttribute('readonly');\n"
                "  el.value = val;\n"
                "  el.dispatchEvent(new Event('input', { bubbles: true }));\n"
                "  el.dispatchEvent(new Event('change', { bubbles: true }));\n"
                "}",
                {"sel": selector, "val": value},
            )

        _set_date_input("#pricing-grid input#fromDate", from_date)
        _set_date_input("#pricing-grid input#toDate", to_date)

        # Give the page a moment for any data reload after date change
        page.wait_for_timeout(1500)

        # 3c) Now wait until at least one product row is visible
        try:
            page.wait_for_selector("#pricing-grid .product-data-column.product-section span", timeout=5000)
        except:
            if not page.query_selector("#pricing-grid"):
                result["error"] = "Could not find the element with id 'pricing-grid' after setting dates."
            else:
                result["error"] = "Pricing grid appeared but no product rows found for the given date range."
            browser.close()
            return result

        # 4) Extract each product row together with the heading (date/time) that precedes it.
        #    We run JavaScript on the page to walk the DOM inside #pricing-grid so we can associate
        #    rows with their corresponding <h3 id="product-date-time-*"> header.

        scraped_rows = page.evaluate("() => {\n" +
            "  const data = [];\n" +
            "  const container = document.querySelector('#pricing-grid');\n" +
            "  if (!container) return data;\n" +
            "  const headers = container.querySelectorAll('h3[id^=\\'product-date-time-\\']');\n" +
            "  headers.forEach(header => {\n" +
            "    const headerText = header.innerText.trim();\n" +
            "    let node = header.nextElementSibling;\n" +
            "    while (node && !(node.tagName === 'H3' && node.id && node.id.startsWith('product-date-time-'))) {\n" +
            "      const descSpans = node.querySelectorAll('.product-data-column.product-section span');\n" +
            "      const priceSpans = node.querySelectorAll('.product-data-column.price span');\n" +
            "      const len = Math.min(descSpans.length, priceSpans.length);\n" +
            "      for (let i = 0; i < len; i++) {\n" +
            "        data.push({ dateTime: headerText, description: descSpans[i].innerText.trim(), price: priceSpans[i].innerText.trim() });\n" +
            "      }\n" +
            "      node = node.nextElementSibling;\n" +
            "    }\n" +
            "  });\n" +
            "  return data;\n" +
            "}")

        result["scrapedData"] = scraped_rows

        browser.close()
        return result

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scrape_juliet.py <URL>")
        sys.exit(1)

    # 1) Ask end-user for date or date range
    user_input = input(
        "Enter a date or a date range to query (e.g. 05/01/2025 or 05/01/2025 05/07/2025): "
    ).strip()

    date_parts = user_input.split()

    # Basic validation helper
    date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")

    if len(date_parts) == 1 and date_pattern.match(date_parts[0]):
        from_date = to_date = date_parts[0]
    elif len(date_parts) == 2 and all(date_pattern.match(d) for d in date_parts):
        from_date, to_date = date_parts
    else:
        print("Input must be one date or two dates in MM/DD/YYYY format.")
        sys.exit(1)

    target_url = sys.argv[1]
    output = scrape_pricing(target_url, from_date, to_date)
    print(json.dumps(output, indent=2))
