import streamlit as st
import re
import json
import concurrent.futures
from datetime import datetime, date

from scrape import scrape_pricing, get_broadway_shows

st.set_page_config(page_title="Broadway Shows Pricing Scraper", layout="centered")

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

st.title("🎭 Broadway Shows Pricing Scraper")
st.markdown("Select Broadway shows and date ranges to scrape pricing information")

# Initialize session state
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
    import re
    # Remove currency symbols and extract numbers
    price_match = re.search(r'[\d,]+\.?\d*', price_str.replace('$', '').replace(',', ''))
    if price_match:
        try:
            return float(price_match.group())
        except ValueError:
            pass
    return 0.0  # Default for unparseable prices

def normalize_price_display(price_str):
    """Return a display-friendly price string by removing trailing .00 only.

    Examples:
    "$169.00" -> "$169"
    "$169.50" -> "$169.50" (unchanged)
    "$99.00 - $299.00" -> "$99 - $299"
    """
    import re
    if not isinstance(price_str, str):
        return price_str
    s = price_str.strip()
    # Remove any occurrence of '.00' that directly follows a digit and is not followed by another digit
    return re.sub(r'(?<=\d)\.00(?!\d)', '', s)

def format_pricing_by_date(scraped_data, show_title=None):
    """Format pricing data as dictionary grouped by date.

    If `show_title` is provided, the header will read:
    "Below is group pricing for SHOW on DAY, DATE at TIME, subject to change and availability."
    Otherwise, it falls back to the original header using the raw date/time text.
    """
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
            # Try to parse into DAY, DATE and TIME parts
            import re as _re
            m = _re.match(r"^\s*([A-Za-z]+),\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}\s*[APMapm]{2})\s*$", date_time)
            if m:
                day_part, date_part, time_part = m.group(1), m.group(2), m.group(3).replace(" ", "") if " " in m.group(3) else m.group(3)
                # Normalize AM/PM to no extra spaces like "6:30PM"
                time_part = time_part.upper().replace(" ", "")
                # Make day not all caps: use sentence-style capitalization
                day_part_normalized = day_part.capitalize()
                header_text = f"Below is group pricing for {show_title} on {day_part_normalized}, {date_part} at {time_part}, subject to change and availability.\n"
            else:
                # Fallback if we cannot parse the string format: still normalize leading day portion
                _m2 = _re.match(r"^\s*([A-Za-z]+)(.*)$", date_time)
                if _m2:
                    _day, _rest = _m2.group(1), _m2.group(2)
                    normalized_dt = f"{_day.capitalize()}{_rest}"
                else:
                    normalized_dt = date_time
                header_text = f"Below is group pricing for {show_title} on {normalized_dt}, subject to change and availability.\n"
        else:
            # No show title provided; still normalize day casing if present
            import re as _re
            m = _re.match(r"^\s*([A-Za-z]+),\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s+([0-9]{1,2}:[0-9]{2}\s*[APMapm]{2})\s*$", date_time)
            if m:
                day_part, date_part, time_part = m.group(1), m.group(2), m.group(3).replace(" ", "") if " " in m.group(3) else m.group(3)
                time_part = time_part.upper().replace(" ", "")
                header_text = f"Below is group pricing for {day_part.capitalize()}, {date_part} at {time_part}, subject to change and availability.\n"
            else:
                _m2 = _re.match(r"^\s*([A-Za-z]+)(.*)$", date_time)
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
            # Premium tier first
            if "premium" in t or "mid-premium" in t or "mid premium" in t:
                return ("premium", 0)
            if "orchestra" in t or "orch" in t:
                return ("orchestra", 1)
            if "mezzanine" in t or "mezz" in t:
                return ("mezzanine", 2)
            if "balcony" in t or "balc" in t:
                return ("balcony", 3)
            return ("other", 4)
        
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

# Load shows if not already loaded
if not st.session_state.shows_loaded:
    if st.button("🔄 Load Broadway Shows", use_container_width=True):
        load_broadway_shows()
        st.rerun()
    st.info("Click the button above to load available Broadway shows")
    st.stop()

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
                
                # 1) Compute two date objects to feed back in:
                existing_from = parse_date_string(task["from_date"]) or min_date
                existing_to = parse_date_string(task["to_date"]) or min_date

                # 2) Pass them as a 2-tuple to value= → forces range picker
                # start_date, end_date = st.date_input(
                #     "Date Range",
                #     value=(existing_from, existing_to),    # <-- tuple = range mode
                #     min_value=min_date,
                #     max_value=max_date,
                #     key=f"date_range_{task['id']}",
                #     help=f"Select dates between {min_date:%m/%d/%Y} and {max_date:%m/%d/%Y}"
                # )
                default_date = datetime.now().date()
                if min_date > datetime.now().date():
                    default_date = min_date
                d = st.date_input(
                    "Date Range", 
                    value=(default_date, default_date),    # <-- tuple = range mode
                    min_value = (min_date if min_date > datetime.now().date() else datetime.now().date()),
                    max_value=max_date,
                    key=f"date_range_{task['id']}",
                    help=f"Select dates between {min_date:%m/%d/%Y} and {max_date:%m/%d/%Y}"
                )
                 # Handle intermediate selection state (when user has only picked first date)
                if isinstance(d, (list, tuple)) and len(d) == 2:
                     start_date, end_date = d
                elif isinstance(d, date):
                     # User is halfway through selection - use same date for both
                     start_date = end_date = d
                else:
                     # Fallback to default
                     start_date = end_date = default_date
                # Show date constraints info if a show is selected (below date picker)
                if task.get("url") and (min_date != date(2020, 1, 1) or max_date != date(2030, 12, 31)):
                    st.caption(f"📅 Available: {min_date.strftime('%m/%d/%Y')} - {max_date.strftime('%m/%d/%Y')}")
                
                # 3) Unpack what you get back (always a tuple now)
                task["from_date"] = start_date.strftime("%m/%d/%Y")
                task["to_date"] = end_date.strftime("%m/%d/%Y")
            
            # Bottom action row
            col_left, col_right = st.columns([3, 1])
            
            # with col_left:
                # # Show task status or URL info
                # if task["url"]:
                    
                # else:
                #     st.caption("⚠️ No show selected")
            
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
        
        with st.expander(task_title, expanded=False):
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
            else:
                st.info("No data found for this task")
            
            # Raw JSON toggle as details instead of nested expander
            if st.checkbox("Show Raw JSON", key=f"raw_json_{i}"):
                st.json(result["result"])
    
    # Clear results button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🗑️ Clear All Results", use_container_width=True):
            st.session_state.results = []
            st.rerun()

