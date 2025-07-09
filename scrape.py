# scrape_juliet.py
import json
import sys
from playwright.sync_api import sync_playwright
import re

# -------------------------
# Broadway Inbound show extraction
# -------------------------

def get_broadway_shows() -> tuple:
    """
    Scrape https://www.broadwayinbound.com/shows to get all show URLs 
    that have "Request Tickets" buttons.
    
    Returns:
        Tuple: (shows_list, debug_messages)
        shows_list: List of dicts: [{"title": "Show Name", "url": "https://..."}, ...]
        debug_messages: List of debug strings
    """
    shows = []
    debug = []
    base_url = "https://www.broadwayinbound.com"
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        page = browser.new_page()
        
        try:
            # Navigate to the shows page
            debug.append(f"Navigating to {base_url}/shows...")
            print(f"Navigating to {base_url}/shows...")
            page.goto(f"{base_url}/shows", wait_until="networkidle", timeout=30000)
            
            # 1) Accept terms so the AJAX show-loader can run
            try:
                debug.append("Looking for terms modal...")
                print("Looking for terms modal...")
                page.wait_for_selector('text="I Understand"', timeout=10000)
                debug.append("Found terms modal, clicking 'I Understand'...")
                print("Found terms modal, clicking 'I Understand'...")
                page.click('text="I Understand"')
                debug.append("Terms modal dismissed")
                print("Terms modal dismissed")
            except Exception as e:
                msg = f"No terms modal found or couldn't click: {e}"
                debug.append(msg)
                print(msg)
            
            # Wait for the page to load completely after modal dismissal
            page.wait_for_timeout(3000)
            
            # Grab the page's HTML and extract the shows JSON directly
            debug.append("Extracting shows from page HTML...")
            print("Extracting shows from page HTML...")
            
            html = page.content()
            
            # Use a regex to pull out the `shows = [...]` JSON blob
            import re, json
            match = re.search(r'var shows\s*=\s*(\[\{.*?\}\]);', html, re.S)
            if not match:
                debug.append("Couldn't find the shows array in the page source")
                print("Couldn't find the shows array in the page source")
                # Try alternative patterns
                alt_patterns = [
                    r'shows\s*=\s*(\[\{.*?\}\])',
                    r'showsList\s*=\s*(\[\{.*?\}\])',
                    r'data\.shows\s*=\s*(\[\{.*?\}\])',
                    r'"shows"\s*:\s*(\[\{.*?\}\])'
                ]
                
                for pattern in alt_patterns:
                    match = re.search(pattern, html, re.S)
                    if match:
                        debug.append(f"Found shows using alternative pattern: {pattern}")
                        print(f"Found shows using alternative pattern: {pattern}")
                        break
            
            if match:
                debug.append("Found shows array in page source")
                print("Found shows array in page source")
                
                shows_json = match.group(1)
                shows_data = json.loads(shows_json)
                
                debug.append(f"Parsed {len(shows_data)} shows from JSON")
                print(f"Parsed {len(shows_data)} shows from JSON")
                
                # Now normalize each show into your list
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
                            msg = f"Found show with pricing: {title.strip()} -> {full_url} (First: {first_performance}, Sale Through: {on_sale_through})"
                            debug.append(msg)
                            print(msg)
                    else:
                        # Debug: show which shows are being skipped
                        title = show.get('ShowName') or show.get('SortName') or show.get('title') or show.get('name') or 'Unknown'
                        skip_msg = f"Skipped show (no pricing yet): {title}"
                        debug.append(skip_msg)
                        print(skip_msg)
                
                msg = f"Found {len(shows)} shows via inline JSON"
                debug.append(msg)
                print(msg)
            else:
                debug.append("Could not find shows array with any pattern")
                print("Could not find shows array with any pattern")
            
        except Exception as e:
            msg = f"Error scraping Broadway Inbound: {e}"
            debug.append(msg)
            print(msg)
            
        finally:
            browser.close()
    
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
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
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
