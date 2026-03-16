import streamlit as st
import re
import json
import concurrent.futures
import asyncio
import pandas as pd
from datetime import datetime, date
from openai import OpenAI

from rapidfuzz import fuzz
from scrape import scrape_pricing, get_broadway_shows
from scrape_shows import get_tourstoyou_data, get_broadway_data, split_location, standardize_date_range

st.set_page_config(page_title="Broadway Scraper Suite", layout="wide")

# Custom CSS for better styling
st.markdown("""
<style>
/* Only style our specific task wrapper class */
.task-wrapper {
    border: 2px solid #e1e5e9;
    border-radius: 10px;
    padding: 15px;
    margin: 8px 0;
    background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    box-sizing: border-box;
}

.task-header {
    font-weight: bold;
    font-size: 18px;
    margin-bottom: 15px;
    color: #1f2937;
}

.stButton > button {
    width: 100%;
}

.remove-btn {
    background-color: #dc3545 !important;
    color: white !important;
}

.show-container {
    background-color: #e3f2fd;
    padding: 15px;
    border-radius: 8px;
    margin: 5px 0;
    border-left: 4px solid #2196f3;
}
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "page" not in st.session_state:
    st.session_state.page = "pricing"

if "tasks" not in st.session_state:
    st.session_state.tasks = []
if "results" not in st.session_state:
    st.session_state.results = []
if "is_running" not in st.session_state:
    st.session_state.is_running = False
if "broadway_shows" not in st.session_state:
    st.session_state.broadway_shows = []
if "shows_loaded" not in st.session_state:
    st.session_state.shows_loaded = False

# Session state for Touring Search
if "shows_df" not in st.session_state:
    st.session_state.shows_df = None
if "shows_last_scraped" not in st.session_state:
    st.session_state.shows_last_scraped = None

# Supabase for touring cache
from supabase import create_client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

@st.cache_resource
def get_supabase():
    """Get Supabase client"""
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def save_cache_to_supabase(df):
    """Save touring shows to Supabase"""
    supabase = get_supabase()
    records = json.loads(df.to_json(orient='records'))
    cache_data = {
        'shows': records,
        'last_scraped': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        supabase.table('touring_cache').upsert({
            'id': 1,
            'data': cache_data,
            'last_updated': datetime.now().isoformat()
        }).execute()
        return cache_data['last_scraped']
    except Exception as e:
        st.error(f"Failed to save cache: {e}")
        return cache_data['last_scraped']

def load_cache_from_supabase():
    """Load touring shows from Supabase"""
    try:
        supabase = get_supabase()
        response = supabase.table('touring_cache').select('data').eq('id', 1).execute()
        if response.data and len(response.data) > 0:
            cache_data = response.data[0]['data']
            if cache_data and 'shows' in cache_data and cache_data['shows']:
                df = pd.DataFrame(cache_data['shows'])
                last_scraped = cache_data.get('last_scraped', 'Unknown')
                return df, last_scraped
    except Exception as e:
        st.warning(f"Could not load cache: {e}")
    return None, None

# Session state for tracking expanded result expanders
if "expanded_results" not in st.session_state:
    st.session_state.expanded_results = set()


# ------------------------------------------------------------------
# Helper Functions - Pricing Scraper
# ------------------------------------------------------------------

def load_broadway_shows():
    """Load Broadway shows from the website"""
    with st.spinner("Loading Broadway shows..."):
        try:
            shows, debug_messages = get_broadway_shows()
            
            # Display debug information
            if debug_messages:
                with st.expander("Debug Information", expanded=True):
                    for message in debug_messages:
                        st.text(message)
            
            st.session_state.broadway_shows = shows
            st.session_state.shows_loaded = True
            
            if shows:
                st.success(f"Successfully loaded {len(shows)} Broadway shows!")
            else:
                st.warning("No shows were found. Check the debug information above.")
            
            return shows
            
        except Exception as e:
            st.error(f"Error loading shows: {e}")
            return []

def validate_date(date_str):
    """Validate MM/DD/YYYY format"""
    if not date_str:
        return True  # Empty is valid for to_date
    pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
    return pattern.match(date_str) is not None

def parse_date_string(date_str):
    """Convert MM/DD/YYYY string to date object"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        return None

def parse_show_date(date_str):
    """Parse show date from M/D/YYYY format (e.g. '9/17/2019')"""
    if not date_str:
        return None
    try:
        # Handle both M/D/YYYY and MM/DD/YYYY formats
        return datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        try:
            # Try with single digits
            parts = date_str.split('/')
            if len(parts) == 3:
                month, day, year = parts
                formatted_date = f"{int(month):02d}/{int(day):02d}/{year}"
                return datetime.strptime(formatted_date, "%m/%d/%Y").date()
        except (ValueError, TypeError):
            pass
    return None

def get_show_date_constraints(task, broadway_shows):
    """Get min and max date constraints for a task based on selected show"""
    if not task.get("url") or not broadway_shows:
        return date(2020, 1, 1), date(2030, 12, 31)
    
    # Find the selected show
    selected_show = None
    for show in broadway_shows:
        if show["url"] == task["url"]:
            selected_show = show
            break
    
    if not selected_show:
        return date(2020, 1, 1), date(2030, 12, 31)
    
    # Parse show dates
    first_performance = parse_show_date(selected_show.get('firstPerformance', ''))
    on_sale_through = parse_show_date(selected_show.get('onSaleThrough', ''))
    
    # Set constraints
    min_date = first_performance if first_performance else date(2020, 1, 1)
    max_date = on_sale_through if on_sale_through else date(2030, 12, 31)
    
    return min_date, max_date

def format_date_for_task(date_obj):
    """Convert date object to MM/DD/YYYY string"""
    if not date_obj:
        return ""
    return date_obj.strftime("%m/%d/%Y")

def add_task():
    """Add a new empty task"""
    st.session_state.tasks.append({
        "id": len(st.session_state.tasks),
        "show_title": "",
        "url": "",
        "from_date": "",
        "to_date": ""
    })

def remove_task(task_id):
    """Remove a task by ID"""
    st.session_state.tasks = [t for t in st.session_state.tasks if t["id"] != task_id]

def extract_price_value(price_str):
    """Extract numeric value from price string for sorting"""
    # Remove currency symbols and extract numbers
    price_match = re.search(r'[\d,]+\.?\d*', price_str.replace('$', '').replace(',', ''))
    if price_match:
        try:
            return float(price_match.group())
        except ValueError:
            pass
    return 0.0  # Default for unparseable prices

def normalize_price_display(price_str):
    """Return a display-friendly price string by removing trailing .00 only."""
    if not isinstance(price_str, str):
        return price_str
    s = price_str.strip()
    # Remove any occurrence of '.00' that directly follows a digit and is not followed by another digit
    return re.sub(r'(?<=\d)\.00(?!\d)', '', s)

def transform_pricing_to_rows(scraped_data):
    """
    Transform scraped pricing data so each dateTime has its own row with price tiers as columns.
    
    Input format:
    [
        { "dateTime": "SUNDAY, 3/8/2026 6:30PM", "description": "Orchestra", "price": "$100" },
        { "dateTime": "SUNDAY, 3/8/2026 6:30PM", "description": "Mezzanine", "price": "$80" },
        ...
    ]
    
    Output format:
    [
        {
            "event_date": "Mar 8, 2026",
            "event_time": "evening",
            "time": "6:30 PM",
            "reg_tier_1": "$100",
            "reg_tier_2": "$80",
            ...
        },
        ...
    ]
    """
    if not scraped_data:
        return []
    
    # Group data by dateTime
    grouped = {}
    for item in scraped_data:
        dt = item.get('dateTime', 'Unknown')
        if dt not in grouped:
            grouped[dt] = []
        grouped[dt].append(item)
    
    result = []
    for date_time, items in grouped.items():
        row = {}
        
        # Parse dateTime string like "SUNDAY, 3/8/2026 6:30PM" or "SUNDAY, 03/08/2026 6:30 PM"
        m = re.match(r"^\s*([A-Za-z]+),\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[APMapm]{2})\s*$", date_time)
        if m:
            day_name = m.group(1).capitalize()
            date_part = m.group(2)  # e.g., "3/8/2026"
            time_part = m.group(3).upper().replace(" ", "")  # e.g., "6:30PM"
            
            # Format date nicely (e.g., "Mar 8, 2026")
            try:
                parts = date_part.split('/')
                month_num = int(parts[0])
                day_num = int(parts[1])
                year = parts[2]
                month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                formatted_date = f"{month_names[month_num]} {day_num}, {year}"
            except:
                formatted_date = date_part
            
            # Format time nicely (e.g., "6:30 PM")
            time_formatted = time_part[:-2] + " " + time_part[-2:]
            
            # Determine matinee vs evening based on time
            try:
                hour = int(time_part.split(':')[0])
                is_pm = 'PM' in time_part.upper()
                if is_pm and hour != 12:
                    hour += 12
                elif not is_pm and hour == 12:
                    hour = 0
                event_time = "matinee" if hour < 17 else "evening"  # Before 5pm = matinee
            except:
                event_time = "unknown"
            
            row["event_date"] = formatted_date
            row["event_time"] = event_time
            row["time"] = time_formatted
        else:
            # Fallback if parsing fails
            row["event_date"] = date_time
            row["event_time"] = "unknown"
            row["time"] = ""
        
        # Collect prices with their descriptions, splitting on "/"
        # Use disambiguation suffixes when the same section name appears with different prices
        for item in items:
            price_str = item.get('price', '')
            description = item.get('description', '').strip()
            if description:
                normalized_price = normalize_price_display(price_str)
                sections = [section.strip() for section in description.split('/')] if '/' in description else [description]
                for section in sections:
                    if not section:
                        continue
                    key = section
                    if key in row and row[key] != normalized_price:
                        counter = 2
                        while f"{section} ({counter})" in row:
                            counter += 1
                        key = f"{section} ({counter})"
                    row[key] = normalized_price
        
        result.append(row)
    
    # Sort result by date
    def parse_event_date(row):
        try:
            date_str = row.get('event_date', '')
            return datetime.strptime(date_str, "%b %d, %Y")
        except:
            return datetime.max
    
    result.sort(key=lambda r: (parse_event_date(r), r.get('time', '')))
    
    return result

def format_pricing_with_ai(transformed_data, scraped_data=None):
    """
    Use OpenAI to categorize section names into standard tiers, then compute
    per-date min/max price ranges from the raw scraped data.
    """
    if not transformed_data:
        return None, None, "No data to process"
    if not scraped_data:
        return None, None, "No raw scraped data available"

    # Collect all unique section names (skip Student)
    all_sections = set()
    for item in scraped_data:
        desc = item.get('description', '').strip()
        if desc:
            sections = [s.strip() for s in desc.split('/')] if '/' in desc else [desc]
            for s in sections:
                if s and s.lower() != 'student':
                    all_sections.add(s)

    sections_list = "\n".join(sorted(all_sections))

    prompt = f"""Here are the seating section names for a Broadway show:

{sections_list}

Categorize each section name into exactly one of these tiers:
- Premium
- MidPremium
- Orchestra
- FrontMezzanine
- RearMezzanine

Return a JSON object mapping each tier to the list of section names that belong to it.
If a tier has no matching sections, set its value to null.

Example:
{{
  "Premium": ["Premium"],
  "MidPremium": ["Mid Premium"],
  "Orchestra": ["Orchestra Rows AA-N", "Orchestra Center Rows L-N, Side Rows L-N"],
  "FrontMezzanine": ["Mezzanine Rows A-D", "Mezzanine Center Rows A-D"],
  "RearMezzanine": ["Mezzanine Rows G-K", "Mezzanine Rows H-K"]
}}

NOTE: Mid Premium and Premium are separate tiers. Sometimes there will not be a mid premium tier - in that case set the MidPremium value to null.

Return ONLY the JSON, no other text."""

    try:
        client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        ai_response = response.choices[0].message.content.strip()

        if ai_response.startswith("```"):
            ai_response = re.sub(r'^```json?\s*', '', ai_response)
            ai_response = re.sub(r'\s*```$', '', ai_response)

        tier_mapping = json.loads(ai_response)

        print("tier_mapping: ", json.dumps(tier_mapping, indent=2))

        # Reverse mapping: section_name -> tier_name
        section_to_tier = {}
        for tier_name, sections in tier_mapping.items():
            if sections:
                for section_name in sections:
                    section_to_tier[section_name] = tier_name

        # Group scraped_data by dateTime
        date_groups = {}
        for item in scraped_data:
            dt = item.get('dateTime', 'Unknown')
            if dt not in date_groups:
                date_groups[dt] = []
            date_groups[dt].append(item)

        tier_order = ["Premium", "MidPremium", "Orchestra", "FrontMezzanine", "RearMezzanine"]

        result_rows = []
        for date_time, items in date_groups.items():
            row = {}

            # Parse dateTime (same logic as transform_pricing_to_rows)
            m = re.match(r"^\s*([A-Za-z]+),\s*(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*[APMapm]{2})\s*$", date_time)
            if m:
                date_part = m.group(2)
                time_part = m.group(3).upper().replace(" ", "")
                try:
                    parts = date_part.split('/')
                    month_num = int(parts[0])
                    day_num = int(parts[1])
                    year = parts[2]
                    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                    formatted_date = f"{month_names[month_num]} {day_num}, {year}"
                except Exception:
                    formatted_date = date_part
                time_formatted = time_part[:-2] + " " + time_part[-2:]
                try:
                    hour = int(time_part.split(':')[0])
                    is_pm = 'PM' in time_part.upper()
                    if is_pm and hour != 12:
                        hour += 12
                    elif not is_pm and hour == 12:
                        hour = 0
                    event_time = "matinee" if hour < 17 else "evening"
                except Exception:
                    event_time = "unknown"
                row["event_date"] = formatted_date
                row["event_time"] = event_time
                row["time"] = time_formatted
            else:
                row["event_date"] = date_time
                row["event_time"] = "unknown"
                row["time"] = ""

            # Collect prices per tier for THIS date only
            tier_prices = {tier: [] for tier in tier_order}
            for item in items:
                desc = item.get('description', '').strip()
                price = item.get('price', '')
                if not desc:
                    continue
                sections = [s.strip() for s in desc.split('/')] if '/' in desc else [desc]
                for section in sections:
                    tier = section_to_tier.get(section)
                    if tier:
                        price_val = extract_price_value(price)
                        if price_val > 0:
                            tier_prices[tier].append(price_val)

            # Build tier columns with per-date min/max
            for tier_num, tier_name in enumerate(tier_order, start=1):
                prices = tier_prices[tier_name]
                if prices:
                    min_price = int(round(min(prices)))
                    max_price = int(round(max(prices) * 1.04))
                    row[f"reg_tier_{tier_num}"] = f"${min_price} - ${max_price}"
                else:
                    row[f"reg_tier_{tier_num}"] = "null"

            result_rows.append(row)

        # Sort by date then time
        def parse_event_date(r):
            try:
                return datetime.strptime(r.get('event_date', ''), "%b %d, %Y")
            except Exception:
                return datetime.max

        result_rows.sort(key=lambda r: (parse_event_date(r), r.get('time', '')))

        result_df = pd.DataFrame(result_rows)

        return result_df, tier_mapping, None

    except json.JSONDecodeError as e:
        return None, None, f"Failed to parse AI response as JSON: {e}"
    except Exception as e:
        return None, None, f"Error calling OpenAI: {e}"

def format_pricing_by_date(scraped_data, show_title=None):
    """Format pricing data as dictionary grouped by date."""
    if not scraped_data:
        return {}
    
    # Group data by dateTime
    grouped_data = {}
    for item in scraped_data:
        date_time = item.get('dateTime', 'Unknown Date')
        if date_time not in grouped_data:
            grouped_data[date_time] = []
        grouped_data[date_time].append(item)
    
    # Format each date group as text
    formatted_by_date = {}
    for date_time, items in grouped_data.items():
        # Build header: "Below is group pricing for SHOW on DAY, DATE at TIME"
        header_text = None
        if show_title:
            # Expected formats like: "SUNDAY, 3/8/2026 6:30PM" or "SUNDAY, 03/08/2026 6:30 PM"
            m = re.match(r"^\s*([A-Za-z]+),\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}\s*[APMapm]{2})\s*$", date_time)
            if m:
                day_part, date_part, time_part = m.group(1), m.group(2), m.group(3).replace(" ", "") if " " in m.group(3) else m.group(3)
                # Normalize AM/PM to no extra spaces like "6:30PM"
                time_part = time_part.upper().replace(" ", "")
                # Make day not all caps: use sentence-style capitalization
                day_part_normalized = day_part.capitalize()
                header_text = f"Below is group pricing for {show_title} on {day_part_normalized}, {date_part} at {time_part}, subject to change and availability.\n"
            else:
                # Fallback if we cannot parse the string format: still normalize leading day portion
                _m2 = re.match(r"^\s*([A-Za-z]+)(.*)$", date_time)
                if _m2:
                    _day, _rest = _m2.group(1), _m2.group(2)
                    normalized_dt = f"{_day.capitalize()}{_rest}"
                else:
                    normalized_dt = date_time
                header_text = f"Below is group pricing for {show_title} on {normalized_dt}, subject to change and availability.\n"
        else:
            # No show title provided; still normalize day casing if present
            m = re.match(r"^\s*([A-Za-z]+),\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}\s*[APMapm]{2})\s*$", date_time)
            if m:
                day_part, date_part, time_part = m.group(1), m.group(2), m.group(3).replace(" ", "") if " " in m.group(3) else m.group(3)
                time_part = time_part.upper().replace(" ", "")
                header_text = f"Below is group pricing for {day_part.capitalize()}, {date_part} at {time_part}, subject to change and availability.\n"
            else:
                _m2 = re.match(r"^\s*([A-Za-z]+)(.*)$", date_time)
                if _m2:
                    _day, _rest = _m2.group(1), _m2.group(2)
                    normalized_dt = f"{_day.capitalize()}{_rest}"
                else:
                    normalized_dt = date_time
                header_text = f"Below is group pricing for {normalized_dt}, subject to change and availability.\n"

        text_lines = [header_text]
        
        # Collect all pricing lines with their prices for sorting
        pricing_lines = []

        # Helper to categorize by seating area for grouping
        def _categorize(section_text: str):
            t = (section_text or "").lower()
            if "mid-premium" in t or "mid premium" in t:
                return ("mid-premium", 1)
            if "premium" in t:
                return ("premium", 0)
            if "orchestra" in t or "orch" in t:
                return ("orchestra", 2)
            if "mezzanine" in t or "mezz" in t:
                return ("mezzanine", 3)
            if "balcony" in t or "balc" in t:
                return ("balcony", 4)
            return ("other", 5)
        
        # Format the pricing items
        for item in items:
            description = item.get('description', 'Unknown Description')
            price = item.get('price', 'Unknown Price')
            display_price = normalize_price_display(price)
            
            # Split descriptions with "/" into separate lines with same price
            if '/' in description:
                sections = [section.strip() for section in description.split('/')]
                for section in sections:
                    if section:  # Skip empty sections
                        line = f"{section} - {display_price}"
                        _, cat_rank = _categorize(section)
                        pricing_lines.append((line, extract_price_value(display_price), cat_rank))
            else:
                line = f"{description} - {display_price}"
                _, cat_rank = _categorize(description)
                pricing_lines.append((line, extract_price_value(display_price), cat_rank))
        
        # Remove exact duplicate lines (same seat text and same price)
        # Duplicates can occur if the source data repeats entries for a date
        seen_lines = set()
        deduped_pricing_lines = []
        for line, numeric_price, cat_rank in pricing_lines:
            if line not in seen_lines:
                seen_lines.add(line)
                deduped_pricing_lines.append((line, numeric_price, cat_rank))
        pricing_lines = deduped_pricing_lines

        # Sort by category group then by price descending within each group
        pricing_lines.sort(key=lambda x: (x[2], -x[1], x[0]))
        
        # Add sorted lines to text_lines
        for line, _, _ in pricing_lines:
            text_lines.append(line)
        
        formatted_by_date[date_time] = "\n".join(text_lines)
    
    return formatted_by_date

def run_scraping_task(task):
    """Run a single scraping task"""
    try:
        to_date = task["to_date"] if task["to_date"] else task["from_date"]
        result = scrape_pricing(task["url"], task["from_date"], to_date)
        return {
            "task": task,
            "result": result,
            "success": True,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
    except Exception as e:
        return {
            "task": task,
            "result": {"error": str(e), "scrapedData": [], "clickSuccessful": False},
            "success": False,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }

def run_all_tasks():
    """Run all tasks concurrently"""
    if not st.session_state.tasks:
        st.warning("No tasks to run!")
        return
    
    # Validate all tasks first
    valid_tasks = []
    for i, task in enumerate(st.session_state.tasks):
        if not task["url"].strip():
            st.error(f"Task {i + 1}: Show selection is required")
            return
        if not task["from_date"]:
            st.error(f"Task {i + 1}: From date is required")
            return
        if not validate_date(task["from_date"]):
            st.error(f"Task {i + 1}: Invalid from date format (use MM/DD/YYYY)")
            return
        if not validate_date(task["to_date"]):
            st.error(f"Task {i + 1}: Invalid to date format (use MM/DD/YYYY)")
            return
        
        valid_tasks.append(task)
    
    st.session_state.is_running = True
    st.rerun()

# ------------------------------------------------------------------
# Helper Functions - Touring Search
# ------------------------------------------------------------------

def load_venue_registry():
    """Load venue registry from Supabase into a DataFrame."""
    try:
        supabase = get_supabase()
        response = supabase.table('venue_registry').select('*').execute()
        if response.data:
            return pd.DataFrame(response.data)
    except Exception as e:
        st.warning(f"Could not load venue registry: {e}")
    return pd.DataFrame(columns=['id', 'city', 'state', 'venue_name', 'address'])

def match_venue(scraped_city, scraped_venue, registry_df, threshold=65):
    """
    Fuzzy-match a scraped venue against the registry, scoped by city.
    Returns (venue_id, canonical_venue_name, address) or (None, None, None).
    """
    if registry_df.empty or not scraped_city:
        return None, None, None

    scraped_city_lower = scraped_city.strip().lower()

    city_candidates = registry_df[
        registry_df['city'].str.strip().str.lower() == scraped_city_lower
    ]

    if city_candidates.empty:
        city_candidates = registry_df[
            registry_df['city'].str.strip().str.lower().apply(
                lambda c: fuzz.ratio(c, scraped_city_lower)
            ) >= 80
        ]

    if city_candidates.empty:
        return None, None, None

    scraped_venue_clean = scraped_venue.strip()
    best_score = 0
    best_row = None

    for _, row in city_candidates.iterrows():
        score = fuzz.token_sort_ratio(
            scraped_venue_clean.lower(),
            row['venue_name'].strip().lower()
        )
        if score > best_score:
            best_score = score
            best_row = row

    if best_score >= threshold and best_row is not None:
        return int(best_row['id']), best_row['venue_name'], best_row['address']

    return None, None, None


def _normalize_show_for_dedup(name):
    """Strip parentheticals and punctuation for dedup comparison."""
    s = name.lower().strip()
    s = re.sub(r'\s*\([^)]*\)\s*', ' ', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _shows_match(name_i, name_j):
    """Check if two show names refer to the same show using multiple strategies."""
    ni, nj = name_i.strip().lower(), name_j.strip().lower()
    if fuzz.token_sort_ratio(ni, nj) >= 85:
        return True
    base_i = _normalize_show_for_dedup(name_i)
    base_j = _normalize_show_for_dedup(name_j)
    if base_i == base_j:
        return True
    if fuzz.token_sort_ratio(base_i, base_j) >= 85:
        return True
    shorter, longer = (base_i, base_j) if len(base_i) <= len(base_j) else (base_j, base_i)
    if len(shorter) >= 3 and longer.startswith(shorter):
        return True
    return False


def _preferred_show_title(title_i, title_j):
    """Pick the preferred title when deduplicating two entries for the same show."""
    clean_i = re.sub(r'\s*\([^)]*\)\s*', ' ', title_i).strip()
    clean_j = re.sub(r'\s*\([^)]*\)\s*', ' ', title_j).strip()
    if len(clean_j) > len(clean_i):
        return title_j
    if len(clean_i) > len(clean_j):
        return title_i
    if len(title_i) <= len(title_j):
        return title_i
    return title_j


def match_and_dedup(df):
    """
    Post-scrape processing: match venues to registry and deduplicate across sources.
    Expects columns: SHOW, CITY, STATE, VENUE, START_DATE, END_DATE, TICKETS
    Returns DataFrame with added: VENUE_ID, CANONICAL_VENUE, ADDRESS (deduped).
    """
    registry_df = load_venue_registry()

    venue_ids = []
    canonical_names = []
    addresses = []

    for _, row in df.iterrows():
        vid, cname, addr = match_venue(
            str(row.get('CITY', '')),
            str(row.get('VENUE', '')),
            registry_df
        )
        venue_ids.append(vid)
        canonical_names.append(cname or row.get('VENUE', ''))
        addresses.append(addr or '')

    df = df.copy()
    df['VENUE_ID'] = venue_ids
    df['CANONICAL_VENUE'] = canonical_names
    df['ADDRESS'] = addresses

    # Filter out past shows (END_DATE earlier than today)
    today = pd.Timestamp.now().normalize()
    end_dates = pd.to_datetime(df['END_DATE'], format='%m/%d/%Y', errors='coerce')
    df = df[end_dates.isna() | (end_dates >= today)].reset_index(drop=True)

    # Deduplicate: same show (fuzzy) + same venue + overlapping dates
    keep = [True] * len(df)
    title_overrides = {}
    venue_norm = df['CANONICAL_VENUE'].str.strip().str.lower().tolist()
    cities = df['CITY'].str.strip().str.lower().tolist()
    starts = pd.to_datetime(df['START_DATE'], format='%m/%d/%Y', errors='coerce')
    ends = pd.to_datetime(df['END_DATE'], format='%m/%d/%Y', errors='coerce')

    for i in range(len(df)):
        if not keep[i]:
            continue
        for j in range(i + 1, len(df)):
            if not keep[j]:
                continue

            # Check venue match: same VENUE_ID, or fuzzy venue name + same city
            vid_i = df.iloc[i]['VENUE_ID']
            vid_j = df.iloc[j]['VENUE_ID']

            same_venue = False
            if pd.notna(vid_i) and pd.notna(vid_j) and vid_i == vid_j:
                same_venue = True
            elif cities[i] == cities[j] and fuzz.token_sort_ratio(venue_norm[i], venue_norm[j]) >= 80:
                same_venue = True

            if not same_venue:
                continue

            if not _shows_match(df.iloc[i]['SHOW'], df.iloc[j]['SHOW']):
                continue

            # Date overlap check (with 3-day tolerance)
            tolerance = pd.Timedelta(days=3)
            s_i, e_i = starts.iloc[i], ends.iloc[i]
            s_j, e_j = starts.iloc[j], ends.iloc[j]

            if pd.isna(s_i) or pd.isna(s_j):
                continue

            if s_i - tolerance <= e_j and s_j - tolerance <= e_i:
                preferred = _preferred_show_title(df.iloc[i]['SHOW'], df.iloc[j]['SHOW'])
                has_tickets_i = bool(df.iloc[i]['TICKETS'])
                has_tickets_j = bool(df.iloc[j]['TICKETS'])
                if has_tickets_j and not has_tickets_i:
                    keep[i] = False
                    title_overrides[j] = preferred
                elif has_tickets_i and not has_tickets_j:
                    keep[j] = False
                    title_overrides[i] = preferred
                else:
                    if preferred == df.iloc[j]['SHOW']:
                        keep[i] = False
                    else:
                        keep[j] = False

    for idx, title in title_overrides.items():
        if keep[idx]:
            df.iloc[idx, df.columns.get_loc('SHOW')] = title

    result = df[keep].reset_index(drop=True)
    dropped = len(df) - len(result)
    if dropped:
        print(f"Dedup: removed {dropped} duplicate rows.")

    return result

async def scrape_all_touring():
    EMPTY_COLS = ["SHOW", "CITY", "STATE", "VENUE", "START_DATE", "END_DATE", "TICKETS"]

    with st.status("Scraping shows...", expanded=True) as status:
        st.write("Scraping tourstoyou.org...")
        headers1, data1 = await get_tourstoyou_data()
        st.write(f"Found {len(data1)} rows from tourstoyou.org")

        st.write("Scraping broadway.org...")
        headers2, data2 = await get_broadway_data()
        st.write(f"Found {len(data2)} rows from broadway.org")

        df1 = pd.DataFrame(data1, columns=headers1) if data1 else pd.DataFrame(columns=EMPTY_COLS)
        df2 = pd.DataFrame(data2, columns=headers2) if data2 else pd.DataFrame(columns=EMPTY_COLS)

        df1.columns = [c.upper() for c in df1.columns]
        df2.columns = [c.upper() for c in df2.columns]

        combined = pd.concat([df1, df2], ignore_index=True)

        st.write("Matching venues & deduplicating...")
        combined = match_and_dedup(combined)

        st.session_state.shows_df = combined

        st.write("Saving to Supabase...")
        st.session_state.shows_last_scraped = save_cache_to_supabase(combined)

        status.update(
            label=f"Scraping complete! {len(combined)} shows after dedup.",
            state="complete", expanded=False
        )

def normalize_date_year(date_str, default_year="2026"):
    """Add year to date string if it doesn't end with a 4-digit year"""
    if not isinstance(date_str, str):
        return date_str
    s = date_str.strip()
    # Check if string ends with a 4-digit year
    if re.search(r'\d{4}$', s):
        return s
    return f"{s}, {default_year}"

def parse_start_date(date_str):
    if not isinstance(date_str, str):
        return pd.Timestamp.max
    
    # Clean string
    s = date_str.strip()
    
    # Try to extract the first date part
    # Common formats:
    # "January 6-25, 2026" -> "January 6, 2026"
    # "Dec 11, 2025-Feb 1, 2026" -> "Dec 11, 2025"
    
    # Regex to find Month Day, Year
    match_year = re.search(r"([A-Za-z]+\.?\s+\d+(?:st|nd|rd|th)?,?\s*\d{4})", s)
    if match_year:
        try:
            return pd.to_datetime(match_year.group(0), errors='coerce')
        except:
            pass

    # Regex for Month Day (no year) - append current year
    match_no_year = re.search(r"([A-Za-z]+\.?\s+\d+(?:st|nd|rd|th)?)", s)
    if match_no_year:
        try:
            current_year = datetime.now().year
            dt = pd.to_datetime(f"{match_no_year.group(0)}, {current_year}", errors='coerce')
            return dt
        except:
            pass
            
    return pd.Timestamp.max # Put unparseable at the end

# ------------------------------------------------------------------
# Main UI Logic
# ------------------------------------------------------------------

if st.session_state.page == "pricing":
    # Limit width for the pricing page to mimic "centered" layout
    st.markdown("""
        <style>
            .block-container {
                max-width: 50rem;
                padding-left: 2rem;
                padding-right: 2rem;
            }
        </style>
    """, unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # Pricing Scraper UI
    # ------------------------------------------------------------------
    st.title("🎭 Broadway Shows Pricing Scraper")
    st.markdown("Select Broadway shows and date ranges to scrape pricing information")

    # Load shows if not already loaded
    if not st.session_state.shows_loaded:
        if st.button("🔄 Load Broadway Shows", use_container_width=True):
            load_broadway_shows()
            st.rerun()
        st.info("Click the button above to load available Broadway shows")
    else:
        # Only show task configuration if not running
        if not st.session_state.is_running:
            st.markdown("## 📝 Configure Tasks")
            
            if st.session_state.broadway_shows:
                st.success(f"Loaded {len(st.session_state.broadway_shows)} Broadway shows")
            
            # Display existing tasks
            for i, task in enumerate(st.session_state.tasks):
                # Create a unique container for each task
                with st.container():
                    # Task header
                    st.markdown(f"""
                        <div class="task-header">
                        🎭 Task {task['id']} 
                        <span style="font-size: 0.8em; color: #6b7280;">
                            {task.get('status', 'Ready')}
                        </span>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    # URL and Date selection on the same row
                    col1, col2 = st.columns([3, 2])
                    
                    with col1:
                        # Show selection
                        if st.session_state.get('broadway_shows'):
                            show_options = ["Select a show..."] + [f"{show['title']}" for show in st.session_state.broadway_shows]
                            
                            # Find current selection index
                            current_index = 0
                            if task["url"]:
                                for idx, show in enumerate(st.session_state.broadway_shows):
                                    if show["url"] == task["url"]:
                                        current_index = idx + 1
                                        break
                            
                            selected_show = st.selectbox(
                                "Show",
                                show_options,
                                key=f"show_select_{task['id']}"
                            )
                            
                            if selected_show != "Select a show...":
                                # Find the selected show and update URL and title
                                for show in st.session_state.broadway_shows:
                                    if show["title"] == selected_show:
                                        task["url"] = show["url"]
                                        task["show_title"] = show["title"]
                                        break
                            else:
                                task["url"] = ""
                                task["show_title"] = ""
                        else:
                            st.warning("No shows loaded. Please refresh and load shows again.")
                    
                    with col2:
                        # Get date constraints for this show
                        min_date, max_date = get_show_date_constraints(task, st.session_state.broadway_shows)
                        
                        default_date = datetime.now().date()
                        if min_date > datetime.now().date():
                            default_date = min_date
                        
                        # Date picker
                        d = st.date_input(
                            "Date Range", 
                            value=(default_date, default_date),
                            min_value = (min_date if min_date > datetime.now().date() else datetime.now().date()),
                            max_value=max_date,
                            key=f"date_range_{task['id']}",
                            help=f"Select dates between {min_date:%m/%d/%Y} and {max_date:%m/%d/%Y}"
                        )
                        # Handle intermediate selection state
                        if isinstance(d, (list, tuple)) and len(d) == 2:
                             start_date, end_date = d
                        elif isinstance(d, date):
                             start_date = end_date = d
                        else:
                             start_date = end_date = default_date
                        
                        # Show date constraints info
                        if task.get("url") and (min_date != date(2020, 1, 1) or max_date != date(2030, 12, 31)):
                            st.caption(f"📅 Available: {min_date.strftime('%m/%d/%Y')} - {max_date.strftime('%m/%d/%Y')}")
                        
                        task["from_date"] = start_date.strftime("%m/%d/%Y")
                        task["to_date"] = end_date.strftime("%m/%d/%Y")
                    
                    # Bottom action row
                    col_left, col_right = st.columns([3, 1])
                    
                    with col_right:
                        if st.button("Remove", key=f"remove_{task['id']}", help="Remove this task"):
                            remove_task(task["id"])
                            st.rerun()
                    
                    # End task wrapper
                    st.markdown('</div>', unsafe_allow_html=True)
            
            # Centered buttons
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                if st.button("➕ Add New Task", use_container_width=True):
                    add_task()
                    st.rerun()
                
                if st.session_state.tasks:
                    st.markdown("")  # Spacing
                    if st.button("🚀 Run All Tasks", type="primary", use_container_width=True):
                        run_all_tasks()
                
                # Refresh shows button
                st.markdown("")
                if st.button("🔄 Refresh Shows List", use_container_width=True):
                    st.session_state.shows_loaded = False
                    st.rerun()

        # Show running state
        if st.session_state.is_running:
            st.markdown("## ⏳ Running Tasks...")
            
            # Validate and run tasks
            valid_tasks = []
            for task in st.session_state.tasks:
                if task["url"].strip() and task["from_date"] and validate_date(task["from_date"]) and validate_date(task["to_date"]):
                    valid_tasks.append(task)
            
            if valid_tasks:
                # Run tasks concurrently
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_task = {executor.submit(run_scraping_task, task): task for task in valid_tasks}
                    results = []
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    completed = 0
                    
                    for future in concurrent.futures.as_completed(future_to_task):
                        result = future.result()
                        results.append(result)
                        completed += 1
                        progress = completed / len(valid_tasks)
                        progress_bar.progress(progress)
                        status_text.text(f"Completed {completed}/{len(valid_tasks)} tasks...")
                    
                    progress_bar.empty()
                    status_text.empty()
                
                st.session_state.results = results
                st.session_state.expanded_results = set()  # Reset expanded state for new results
                st.success("All tasks completed!")
            
            st.session_state.is_running = False
            
            # Button to go back to task configuration
            if st.button("← Back to Task Configuration"):
                st.rerun()

        # Display results
        if st.session_state.results and not st.session_state.is_running:
            st.markdown("## 📊 Results")
            
            for i, result in enumerate(st.session_state.results):
                task = result["task"]
                date_range = task['from_date']
                if task['to_date'] and task['to_date'] != task['from_date']:
                    date_range += f" - {task['to_date']}"
                
                status_icon = "✅" if result["success"] else "❌"
                show_name = task.get('show_title', 'Unknown Show')
                task_title = f"{status_icon} {show_name} ({date_range})"
                
                # Check if this expander should be expanded (user interacted with it)
                is_expanded = i in st.session_state.expanded_results
                
                with st.expander(task_title, expanded=is_expanded):
                    # Mark this expander as expanded once user opens it
                    st.session_state.expanded_results.add(i)
                    
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.write(f"**Completed at:** {result['timestamp']}")
                    with col2:
                        if result["result"].get("scrapedData"):
                            json_data = json.dumps(result["result"]["scrapedData"], indent=2)
                            st.download_button(
                                label="📥 Download",
                                data=json_data,
                                file_name=f"task_{i+1}_data.json",
                                mime="application/json",
                                key=f"download_{i}"
                            )
                    
                    if result["result"].get("error"):
                        st.error(f"Error: {result['result']['error']}")
                    
                    scraped_data = result["result"].get("scrapedData", [])
                    if scraped_data:
                        # Transform data for AI processing
                        transformed_data = transform_pricing_to_rows(scraped_data)
                        
                        # Format Pricing with AI button (centered) - at the top
                        col_left, col_center, col_right = st.columns([1, 2, 1])
                        with col_center:
                            format_clicked = st.button("Process Price Tiers", key=f"format_pricing_{i}", use_container_width=True)
                        
                        if format_clicked:
                            with st.spinner("Processing with AI..."):
                                result_df, pricing_tiers, error = format_pricing_with_ai(transformed_data, scraped_data)
                                if error:
                                    st.error(error)
                                else:
                                    st.success("AI processing complete!")
                                    st.markdown("**AI-Processed Pricing Tiers:**")
                                    st.dataframe(result_df, use_container_width=True, hide_index=True)
                        
                        # Add separate code blocks for each date/time with built-in copy buttons
                        st.markdown("### 📋 Formatted Pricing Text")
                        formatted_by_date = format_pricing_by_date(scraped_data, show_title=task.get('show_title'))
                        if formatted_by_date:
                            for j, (date_time, formatted_text) in enumerate(formatted_by_date.items()):
                                st.markdown(f"**📅 {date_time}**")
                                st.code(f"\n{formatted_text}", language=None)
                        else:
                            st.info("No pricing data to format")

                        # Show raw table below the formatted text
                        st.dataframe(scraped_data, use_container_width=True)
                        
                        # Raw JSON toggle as details instead of nested expander
                        if st.checkbox("Show Raw JSON", key=f"raw_json_{i}"):
                            st.json(transformed_data)
                    else:
                        st.info("No data found for this task")
            
            # Clear results button
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🗑️ Clear All Results", use_container_width=True):
                    st.session_state.results = []
                    st.session_state.expanded_results = set()
                    st.rerun()

    # Link to Touring Search
    st.markdown("---")
    st.markdown("### More Tools")
    if st.button("🎭 Go to Touring Production Search", use_container_width=True):
        st.session_state.page = "touring"
        st.rerun()

elif st.session_state.page == "touring":
    # ------------------------------------------------------------------
    # Touring Search UI
    # ------------------------------------------------------------------
    col_back, _ = st.columns([1, 4])
    with col_back:
        if st.button("← Back"):
            st.session_state.page = "pricing"
            st.rerun()

    st.title("Touring Production Search")

    # Load from Supabase if no data in session
    if st.session_state.shows_df is None:
        with st.spinner("Loading cached data..."):
            cached_df, last_scraped = load_cache_from_supabase()
        if cached_df is not None:
            needs_save = False

            # Migrate old format: split LOCATION → CITY + STATE
            if 'LOCATION' in cached_df.columns and 'CITY' not in cached_df.columns:
                splits = cached_df['LOCATION'].apply(split_location)
                cached_df['CITY'] = splits.apply(lambda x: x[0])
                cached_df['STATE'] = splits.apply(lambda x: x[1])
                cached_df.drop(columns=['LOCATION'], inplace=True)
                needs_save = True

            # Migrate old format: split DATES → START_DATE + END_DATE
            if 'DATES' in cached_df.columns and 'START_DATE' not in cached_df.columns:
                parsed = cached_df['DATES'].apply(standardize_date_range)
                cached_df['START_DATE'] = parsed.apply(lambda x: x[0])
                cached_df['END_DATE'] = parsed.apply(lambda x: x[1])
                cached_df.drop(columns=['DATES'], inplace=True)
                needs_save = True

            # Run venue matching + dedup if not yet applied
            if 'VENUE_ID' not in cached_df.columns:
                cached_df = match_and_dedup(cached_df)
                needs_save = True

            if needs_save:
                save_cache_to_supabase(cached_df)

            st.session_state.shows_df = cached_df
            st.session_state.shows_last_scraped = last_scraped

    if st.session_state.shows_df is None:
        st.info("No cached data found. Click below to scrape shows.")
        if st.button("Scrape All Shows", type="primary"):
            asyncio.run(scrape_all_touring())
            st.rerun()
    else:
        df = st.session_state.shows_df.copy() # Work on a copy
        
        # Filter/Search + Refresh button
        col1, col2 = st.columns([4, 1])
        with col1:
            search_term = st.text_input("Search shows, cities, venues...")
        with col2:
            st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)  # Align with input
            if st.button("Refresh", use_container_width=True):
                asyncio.run(scrape_all_touring())
                st.rerun()
        
        if st.session_state.shows_last_scraped:
            st.caption(f"Last updated: {st.session_state.shows_last_scraped}")
        if search_term:
            # Create a mask for filtering
            mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
            display_df = df[mask].reset_index(drop=True)
        else:
            display_df = df.reset_index(drop=True)
            
        st.markdown("### Scraped Shows")
        st.caption("Select rows to generate venue schedule text.")

        # Choose which columns to show in the table
        display_cols = [c for c in ["SHOW", "CITY", "STATE", "CANONICAL_VENUE", "START_DATE", "END_DATE", "ADDRESS", "TICKETS"]
                        if c in display_df.columns]
        show_df = display_df[display_cols] if display_cols else display_df

        event = st.dataframe(
            show_df,
            selection_mode="multi-row",
            on_select="rerun",
            use_container_width=True,
            hide_index=True
        )

        if len(event.selection.rows) > 0:
            selected_venues = []
            seen = set()
            for idx in event.selection.rows:
                selected_row = display_df.iloc[idx]
                venue_id = selected_row.get('VENUE_ID')
                city = str(selected_row.get('CITY', '')).strip()
                state = str(selected_row.get('STATE', '')).strip()
                venue = str(selected_row.get('CANONICAL_VENUE', selected_row.get('VENUE', ''))).strip()
                address = str(selected_row.get('ADDRESS', '')).strip()

                key = (venue_id, city) if pd.notna(venue_id) else (venue, city)
                if key not in seen:
                    seen.add(key)
                    selected_venues.append({
                        'venue_id': venue_id, 'city': city, 'state': state,
                        'venue': venue, 'address': address
                    })

            st.subheader(f"Selected Venue Schedules ({len(selected_venues)})")

            all_venue_texts = []

            for sv in selected_venues:
                if pd.notna(sv['venue_id']):
                    venue_shows = df[df['VENUE_ID'] == sv['venue_id']].copy()
                else:
                    v_col = 'CANONICAL_VENUE' if 'CANONICAL_VENUE' in df.columns else 'VENUE'
                    venue_shows = df[
                        (df[v_col].astype(str).str.strip() == sv['venue'])
                        & (df['CITY'].astype(str).str.strip() == sv['city'])
                    ].copy()

                venue_shows['_sort_date'] = pd.to_datetime(
                    venue_shows['START_DATE'], format='%m/%d/%Y', errors='coerce'
                )
                venue_shows = venue_shows.sort_values('_sort_date', na_position='last')

                location_label = f"{sv['city']}, {sv['state']}" if sv['state'] else sv['city']
                header = f"{location_label} - {sv['venue'].upper()}"
                if sv['address']:
                    header += f"\n{sv['address']}"
                lines = [header]

                for _, row in venue_shows.iterrows():
                    start = row.get('START_DATE', '')
                    end = row.get('END_DATE', '')
                    if start and end and start != end:
                        dates = f"{start} - {end}"
                    elif start:
                        dates = start
                    else:
                        dates = ""
                    line = f"{row['SHOW']}: {dates}"
                    lines.append(line)

                all_venue_texts.append("\n".join(lines))

            final_text = "\n\n".join(all_venue_texts)
            st.text_area("Copy Text", final_text, height=250)
