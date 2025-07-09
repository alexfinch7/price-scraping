# 🎭 Broadway Shows Pricing Scraper

A Streamlit web application that scrapes Broadway show pricing information from Broadway Inbound for group bookings.

## Features

- **Automated Show Loading**: Automatically loads all available Broadway shows with pricing
- **Date-Constrained Scheduling**: Date pickers are automatically constrained to each show's performance dates
- **Multi-Task Scraping**: Configure and run multiple scraping tasks concurrently
- **Formatted Output**: Get both tabular data and copy-ready formatted text for emails
- **Real-time Progress**: See scraping progress with live updates

## How to Use

1. **Load Shows**: Click "Load Broadway Shows" to fetch all available shows
2. **Add Tasks**: Click "Add New Task" to create scraping tasks
3. **Configure Tasks**: 
   - Select a Broadway show from the dropdown
   - Choose date range (automatically constrained to show's availability)
   - Add more tasks as needed
4. **Run Tasks**: Click "Run All Tasks" to scrape pricing data
5. **View Results**: See data tables and copy formatted text for use in communications

## Installation

### Local Development

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Install Playwright browsers:
   ```bash
   playwright install chromium
   ```
4. Run the app:
   ```bash
   streamlit run app.py
   ```

### Streamlit Cloud Deployment

1. Fork/push this repository to GitHub
2. Connect your GitHub account to [Streamlit Cloud](https://streamlit.io/cloud)
3. Deploy directly from your repository
4. The app will automatically install dependencies and be available at your custom URL

## Output Format

The app provides pricing data in two formats:

### 1. Data Table
Structured data showing Date/Time, Description, and Price for easy analysis.

### 2. Formatted Text
Copy-ready text formatted as:
```
Below is the group pricing for [DATE], subject to change and availability.

Orchestra 1/Grand Tier 1 (Performance Only, Gala/Dinner Not Included) - $125
Mezzanine Center - $98
Balcony - $75
```

## Technical Details

- **Backend**: Python with Playwright for web scraping
- **Frontend**: Streamlit for the web interface
- **Concurrency**: ThreadPoolExecutor for parallel task execution
- **Data Sources**: Broadway Inbound show listings and pricing grids

## File Structure

- `app.py` - Main Streamlit application
- `scrape.py` - Web scraping functions using Playwright
- `requirements.txt` - Python dependencies

## Requirements

- Python 3.8+
- Modern web browser (Chrome/Chromium)
- Internet connection for live scraping

## Contributing

Feel free to submit issues and pull requests to improve the scraper or add new features.

## License

This project is for educational and business use. Please respect Broadway Inbound's terms of service when using this scraper. 