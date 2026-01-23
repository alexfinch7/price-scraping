import asyncio
import argparse
from playwright.async_api import async_playwright
import pandas as pd
import sys
import os

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

async def get_tourstoyou_data():
    url = "https://tourstoyou.org/"
    
    print(f"Starting scrape of {url}...")
    
    data_rows = []
    headers = []

    async with async_playwright() as p:
        browser, context = await get_browser_context(p)
        page = await context.new_page()
        
        try:
            # Go to website
            await page.goto(url, timeout=60000)
            print("Page loaded.")

            # Click "Now Playing" tab
            tab_selector = "#elementor-tab-title-1122"
            try:
                await page.wait_for_selector(tab_selector, state="visible", timeout=10000)
                await page.click(tab_selector)
                print("Clicked 'Now Playing' tab.")
            except Exception as e:
                print(f"Could not find tab by ID. Trying by text 'Now Playing'. Error: {e}")
                await page.get_by_text("Now Playing", exact=True).click()
                print("Clicked 'Now Playing' tab by text.")

            # Select "100" entries
            select_selector = "#dt-length-0"
            try:
                await page.wait_for_selector(select_selector, state="visible", timeout=10000)
                await page.select_option(select_selector, value="100")
                print("Set entries to 100.")
            except Exception as e:
                 print(f"Could not select 100 entries. Continuing with default. Error: {e}")

            # Wait for table to update/load
            await page.wait_for_timeout(3000) 

            # Scrape table data
            table_selector = "#tablepress-863"
            await page.wait_for_selector(table_selector)
            
            # Extract headers
            headers = await page.locator(f"{table_selector} thead th").all_inner_texts()
            # Clean headers
            headers = [h.strip().upper() for h in headers]
            print(f"Found headers: {headers}")
            
            # Extract rows
            rows = await page.locator(f"{table_selector} tbody tr").all()
            
            for row in rows:
                cells = await row.locator("td").all()
                row_data = []
                for i, cell in enumerate(cells):
                    # For the last column (Tickets), try to get the link
                    if i == 4: # Assuming 5th column is Tickets based on previous run
                        link_element = cell.locator("a").first
                        if await link_element.count() > 0:
                            href = await link_element.get_attribute("href")
                            row_data.append(href)
                        else:
                            text = await cell.inner_text()
                            row_data.append(text.strip())
                    else:
                        text = await cell.inner_text()
                        row_data.append(text.strip())
                
                if row_data:
                    data_rows.append(row_data)
            
            print(f"Scraped {len(data_rows)} rows.")

        except Exception as e:
            print(f"An error occurred during tourstoyou scrape: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()
            
    return headers, data_rows

async def get_broadway_data():
    base_url = "https://www.broadway.org"
    list_url = f"{base_url}/tours/"
    
    print(f"Starting scrape of {list_url}...")
    
    all_data = []
    headers = ["SHOW", "LOCATION", "VENUE", "DATES", "TICKETS"]
    
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
                        
                        # Format: SHOW,LOCATION,VENUE,DATES,TICKETS
                        all_data.append([show['name'], location, venue, dates, ticket_link])
                        
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
