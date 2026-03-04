import asyncio
import argparse
import re
from datetime import datetime
from playwright.async_api import async_playwright
import pandas as pd
import sys
import os

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'sept': 9,
    'oct': 10, 'nov': 11, 'dec': 12,
}

def _parse_single_date(s, default_year=2026):
    """Parse a single date like 'April 7', 'Feb 1, 2026', 'Dec 11, 2025'."""
    s = s.strip().rstrip(',')
    # Month Day, Year
    m = re.match(r'([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})', s)
    if m:
        month = MONTH_MAP.get(m.group(1).lower())
        if month:
            return datetime(int(m.group(3)), month, int(m.group(2)))
    # Month Day (no year)
    m = re.match(r'([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?', s)
    if m:
        month = MONTH_MAP.get(m.group(1).lower())
        if month:
            return datetime(default_year, month, int(m.group(2)))
    return None

def standardize_date_range(raw, default_year=2026):
    """
    Parse a raw date range string into (start_mm_dd_yyyy, end_mm_dd_yyyy).

    Handles formats like:
      "January 28–February 7, 2026"  →  ("01/28/2026", "02/07/2026")
      "April 7-12, 2026"             →  ("04/07/2026", "04/12/2026")
      "May 26 - May 31"              →  ("05/26/2026", "05/31/2026")
      "April 19, 2026"               →  ("04/19/2026", "04/19/2026")
    """
    if not raw or not isinstance(raw, str):
        return "", ""

    s = raw.strip()
    # Normalize en-dash / em-dash to hyphen
    s = s.replace('\u2013', '-').replace('\u2014', '-')

    # Extract trailing year if present (e.g. "April 7-12, 2026")
    year_match = re.search(r',?\s*(\d{4})\s*$', s)
    trailing_year = int(year_match.group(1)) if year_match else default_year
    if year_match:
        s = s[:year_match.start()].strip()

    # Split on hyphen (with optional surrounding spaces), max 1 split
    parts = re.split(r'\s*-\s*', s, maxsplit=1)

    fmt = lambda d: d.strftime("%m/%d/%Y") if d else ""

    if len(parts) == 2:
        start_str = parts[0].strip()
        end_str = parts[1].strip()

        start_date = _parse_single_date(start_str, trailing_year)

        if re.match(r'^\d{1,2}$', end_str) and start_date:
            end_date = datetime(trailing_year, start_date.month, int(end_str))
        else:
            end_date = _parse_single_date(end_str, trailing_year)

        return fmt(start_date), fmt(end_date)

    # Single date
    d = _parse_single_date(s, trailing_year)
    return fmt(d), fmt(d)

async def get_browser_context(p):
    # Launch browser with options to appear less bot-like
    browser = await p.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 720}
    )
    return browser, context

def split_location(location_str):
    """Split 'City, ST' into (city, state). Handles 'City, ST', 'City, Canada', etc."""
    if not location_str or not isinstance(location_str, str):
        return "", ""
    parts = [p.strip() for p in location_str.split(",", 1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""

async def get_tourstoyou_data():
    url = "https://tourstoyou.org/"

    print(f"Starting scrape of {url}...")

    all_data = []
    headers = ["SHOW", "CITY", "STATE", "VENUE", "START_DATE", "END_DATE", "TICKETS"]

    async with async_playwright() as p:
        browser, context = await get_browser_context(p)
        page = await context.new_page()

        try:
            await page.goto(url, timeout=60000)
            print("Page loaded.")

            # Click the "Shows" tab instead of "Now Playing"
            clicked = False
            tabs = page.locator("[id^='elementor-tab-title-']")
            tab_count = await tabs.count()
            for i in range(tab_count):
                tab = tabs.nth(i)
                text = (await tab.inner_text()).strip()
                if text.lower() == "shows":
                    await tab.click()
                    clicked = True
                    print(f"Clicked 'Shows' tab.")
                    break

            if not clicked:
                await page.get_by_role("tab", name="Shows").click()
                print("Clicked 'Shows' tab via role selector.")

            await page.wait_for_timeout(3000)

            # Collect all show links from the visible tables
            show_links = await page.evaluate("""() => {
                const links = [];
                const seen = new Set();
                const anchors = document.querySelectorAll('a[href*="/shows/"]');
                for (const a of anchors) {
                    const href = a.href;
                    const text = a.innerText.trim();
                    if (!href || !text) continue;
                    if (href.match(/tourstoyou\\.org\\/shows\\/[^/]+/) && !seen.has(href)) {
                        seen.add(href);
                        links.push({name: text, url: href});
                    }
                }
                return links;
            }""")

            print(f"Found {len(show_links)} show links.")

            # Visit each show page and scrape schedule tables
            for i, show in enumerate(show_links):
                print(f"[{i+1}/{len(show_links)}] Scraping {show['name']}...")
                try:
                    await page.goto(show['url'], timeout=30000)
                    await page.wait_for_timeout(1500)

                    rows = await page.evaluate("""() => {
                        const data = [];
                        const seen = new Set();
                        const tables = document.querySelectorAll('table');
                        for (const table of tables) {
                            const thCells = table.querySelectorAll('thead th, tr:first-child th');
                            const headerTexts = Array.from(thCells).map(
                                th => th.innerText.trim().toLowerCase()
                            );

                            if (headerTexts.some(h => h.includes('season'))) continue;

                            const locIdx = headerTexts.findIndex(h => h.includes('location'));
                            const venIdx = headerTexts.findIndex(h => h.includes('venue'));
                            const dateIdx = headerTexts.findIndex(h => h.includes('date'));
                            const tickIdx = headerTexts.findIndex(h => h.includes('ticket'));

                            if (locIdx === -1 || venIdx === -1 || dateIdx === -1) continue;

                            const bodyRows = table.querySelectorAll('tbody tr');
                            for (const row of bodyRows) {
                                const cells = row.querySelectorAll('td');
                                if (cells.length < 3) continue;

                                const location = cells[locIdx]?.innerText?.trim() || '';
                                const venue = cells[venIdx]?.innerText?.trim() || '';
                                const dates = cells[dateIdx]?.innerText?.trim() || '';
                                let tickets = '';
                                if (tickIdx !== -1 && cells[tickIdx]) {
                                    const link = cells[tickIdx].querySelector('a');
                                    tickets = link ? link.href : cells[tickIdx].innerText.trim();
                                }
                                if (!location && !venue && !dates) continue;
                                const key = location + '|' + venue + '|' + dates;
                                if (seen.has(key)) continue;
                                seen.add(key);
                                data.push({location, venue, dates, tickets});
                            }
                        }
                        return data;
                    }""")

                    for row in rows:
                        start_dt, end_dt = standardize_date_range(row['dates'])
                        city, state = split_location(row['location'])
                        all_data.append([
                            show['name'],
                            city,
                            state,
                            row['venue'],
                            start_dt,
                            end_dt,
                            row['tickets']
                        ])

                    print(f"  Found {len(rows)} schedule entries.")

                except Exception as e:
                    print(f"  Error scraping {show['name']}: {e}")
                    continue

                await asyncio.sleep(0.5)

            print(f"Total: {len(all_data)} rows from {len(show_links)} shows.")

        except Exception as e:
            print(f"An error occurred during tourstoyou scrape: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

    return headers, all_data

async def get_broadway_data():
    base_url = "https://www.broadway.org"
    list_url = f"{base_url}/tours/"
    
    print(f"Starting scrape of {list_url}...")
    
    all_data = []
    headers = ["SHOW", "CITY", "STATE", "VENUE", "START_DATE", "END_DATE", "TICKETS"]
    
    async with async_playwright() as p:
        browser, context = await get_browser_context(p)
        page = await context.new_page()
        
        try:
            # Step 1: Get list of shows
            await page.goto(list_url, timeout=60000)
            print("Loaded list page.")
            
            # Check for blocking
            title = await page.title()
            if "Intermission" in title or "Just a moment" in title:
                print("Blocked by Cloudflare/Protection. Try running again later or adjust stealth settings.")
                return headers, []

            # Find all show links
            # We look for links that start with /tours/ and are not just "/tours" or language links
            # Using specific logic to identify show links
            show_links = []
            links = await page.locator("a").all()
            seen_hrefs = set()
            
            for link in links:
                href = await link.get_attribute("href")
                if href and href.startswith("/tours/") and href not in ["/tours", "/tours/", "/tours?l=1"]:
                    # Further filtering: ignore query params for filtering
                    if "?" in href:
                        continue
                    
                    full_href = base_url + href if href.startswith("/") else href
                    if full_href not in seen_hrefs:
                        text = await link.inner_text()
                        if text and len(text.strip()) > 0:
                            show_links.append({"name": text.strip(), "url": full_href})
                            seen_hrefs.add(full_href)
            
            print(f"Found {len(show_links)} shows.")
            
            # Step 2: Visit each show page
            for i, show in enumerate(show_links):
                print(f"[{i+1}/{len(show_links)}] Scraping {show['name']}...")
                try:
                    await page.goto(show['url'], timeout=30000)
                    
                    # Wait for content
                    try:
                        await page.wait_for_selector(".tour-linkout-row", timeout=5000)
                    except:
                         # Try different selector or just check page content if selector fails
                         # Sometimes pages might be empty or different structure
                         print(f"  No schedule rows found for {show['name']}")
                         continue
                    
                    # Scrape rows
                    rows = await page.locator(".tour-linkout-row").all()
                    
                    for row in rows:
                        # Extract data
                        # Location: .col.col1 .l1
                        # Venue: .col.col1 .l2 a (or text)
                        # Dates: .col.col2 .l1
                        # Tickets: .col.col3 .l2 a (href)
                        
                        location = await row.locator(".col.col1 .l1").inner_text()
                        
                        venue_el = row.locator(".col.col1 .l2")
                        venue = await venue_el.inner_text()
                        
                        dates = await row.locator(".col.col2 .l1").inner_text()
                        
                        ticket_link = ""
                        ticket_btn = row.locator(".col.col3 .l2 a").first
                        if await ticket_btn.count() > 0:
                            ticket_link = await ticket_btn.get_attribute("href")
                        
                        # Clean up
                        location = location.strip()
                        venue = venue.strip()
                        dates = dates.strip()
                        
                        start_dt, end_dt = standardize_date_range(dates)
                        city, state = split_location(location)
                        all_data.append([show['name'], city, state, venue, start_dt, end_dt, ticket_link])
                        
                except Exception as e:
                    print(f"  Error scraping {show['name']}: {e}")
                    # Continue to next show
                    continue
                
                # Slight delay to be nice
                await asyncio.sleep(0.5)

            print(f"Scraped {len(all_data)} total rows.")
            
        except Exception as e:
            print(f"An error occurred during broadway scrape: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()
            
    return headers, all_data

def save_data(data, headers, output_file):
    if data:
        # Check if header count matches data width
        if len(headers) != len(data[0]):
             print("Warning: Header count mismatch. Adjusting.")
             # Very basic adjustment
             if len(headers) < len(data[0]):
                 headers += [f"Col_{i}" for i in range(len(headers), len(data[0]))]
             else:
                 headers = headers[:len(data[0])]

        df = pd.DataFrame(data, columns=headers)
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
    else:
        print("No data found to save.")

async def scrape_tourstoyou():
    output_file = "now_playing_shows.csv"
    headers, data = await get_tourstoyou_data()
    save_data(data, headers, output_file)

async def scrape_broadway():
    output_file = "now_playing_shows.csv"
    headers, data = await get_broadway_data()
    save_data(data, headers, output_file)

async def main():
    parser = argparse.ArgumentParser(description='Scrape show data from different sources.')
    parser.add_argument('--source', type=str, default='tourstoyou', choices=['tourstoyou', 'broadway'],
                        help='Source website to scrape: tourstoyou or broadway')
    args = parser.parse_args()

    if args.source == 'broadway':
        await scrape_broadway()
    else:
        await scrape_tourstoyou()

if __name__ == "__main__":
    asyncio.run(main())
