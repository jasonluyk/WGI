import os
import io
import time
import requests
import subprocess
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
#from playwright.sync_api import sync_playwright
#import asyncio
import sys

# Windows compatibility fix for Playwright and Streamlit
#if sys.platform == 'win32':
   # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())



# --- Configuration ---
INDEX_URL = "https://www.wgi.org/scores/color-guard-scores/"
RAW_DIR = "debug_html"
CSV_DIR = "regional_csvs"
MASTER_DIR = "analytics"

# Ensure all directories exist
for folder in [RAW_DIR, CSV_DIR, MASTER_DIR]:
    if not os.path.exists(folder):
        os.makedirs(folder)

def get_event_list(page):
    """Stage 1: Now finds BOTH Prelims and Finals."""
    print("Searching for Prelims and Finals events...")
    page.goto(INDEX_URL)
    page.wait_for_selector("table", timeout=15000)
    
    events = []
    rows = page.query_selector_all("tr")
    for row in rows:
        row_text = row.inner_text().upper()
        # Updated to include Prelims
        if "FINALS" in row_text or "PRELIMS" in row_text:
            link = row.query_selector("a[href*='ShowId=']")
            if link:
                href = link.get_attribute("href")
                show_id = href.split("ShowId=")[1].split("&")[0]
                # Keep the event name but label it for identification
                event_name = row_text.split('\t')[0].replace(" ", "_").strip()
                events.append({"id": show_id, "name": event_name})
    
    unique_events = {v['id']: v for v in events}.values()
    print(f"Found {len(unique_events)} score sessions.")
    return unique_events

EVENT_LUT = {
    "Bakersfield Regional": {
        "prelims": "https://schedules.competitionsuite.com/4f2fd993-6475-4fd3-85b1-2840b8e525b8_standard.htm",
        "finals": "https://schedules.competitionsuite.com/5e5e9123-31da-49cc-aee1-0986b3aca05a_standard.htm"
    },
    "Flint Regional": "a0tUy00000YzB6GIAV",
    "Gulfport Regional+": "a0tUy00000YzDHhIAN",
    "Indianapolis Regional+": "a0tUy00000Yyws4IAB",
    "Philadelphia Regional+": "a0tUy00000YzDxdIAF",
    "San Diego Regional": "a0tUy00000Yysy7IAB",
    "Austin Regional+": "a0tUy00000YzFLAIA3",
    "Charlotte Regional+": "a0tUy00000Yz92qIAB",
    "Chicago Regional": "a0tUy00000YzFLBIA3",
    "Denver Regional": "a0tUy00000Yz439IAB",
    "Nashville Regional": "a0tUy00000YzDklIAF",
    "Phoenix Regional+": "a0tUy00000Yz5FSIAZ",
    "Tampa Regional": "a0tUy00000YynIQIAZ",
    "Knoxville Regional": "a0tUy00000YzFQCIA3",
    "Manhattan Beach Regional+": "a0tUy00000Yz77ZIAR",
    "Memphis Regional": "a0tUy00000YzYSbIAN",
    "Minneapolis Regional": "a0tUy00000Yz2SpIAJ",
    "Seattle Regional": "a0tUy00000YzNDwIAN",
    "Tulsa Regional+": "a0tUy00000YzA2KIAV",
    "Union City Regional": "a0tUy00000YzNCGIA3",
    "Avon Regional+": "a0tUy00000Z064WIAR",
    "Bellevue Regional": "a0tUy00000YzzO6IAJ",
    "Las Vegas Regional": "a0tUy00000Z09giIAB",
    "Mansfield Regional+": "a0tUy00000Z0BddIAF",
    "Richmond Regional": "a0tUy00000YzgwTIAR",
    "Salt Lake City Regional": "a0tUy00000YzyOmIAJ",
    "Stuart Regional+": "a0tUy00000Z0CI0IAN",
    "Bethlehem Regional+": "a0tUy00000Yze6fIAB",
    "Buford Regional+": "a0tUy00000Z0770IAB",
    "Palm Desert Regional+": "a0tUy00000Z0IC8IAN",
    "San Antonio Regional+": "a0tUy00000Z0LbNIAV",
}

def get_manifest_events():
    """Returns the manifest, handling both dicts and standard strings."""
    events = []
    for name, path in EVENT_LUT.items():
        if isinstance(path, dict):
            # Pass the dict directly if it contains prelims/finals links
            full_url = path
        elif path.startswith("http"):
            full_url = path
        else:
            full_url = f"https://www.wgi.org/event-details-page/?eventId={path}&division=CG"
        
        events.append({"name": name, "url": full_url})
    return events

def pull_dual_event_data(prelims_url, finals_url):
    """Probes Port A (Prelims) and Port B (Finals) to map the day."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        # 1. Probing Prelims
        p_resp = requests.get(prelims_url, headers=headers, timeout=10)
        p_soup = BeautifulSoup(p_resp.text, 'html.parser')
        p_rows = p_soup.find_all('div', class_='schedule-row')
        
        prelims_data = []
        for row in p_rows:
            if 'schedule-row--custom' in row.get('class', []): continue
            name = row.find('div', class_='schedule-row__name')
            initials = row.find('div', class_='schedule-row__initials')
            time = row.find('div', class_='schedule-row__time')
            if name and initials:
                prelims_data.append({
                    "Guard": name.get_text(strip=True),
                    "Class": initials.get_text(strip=True),
                    "Perform Time": time.get_text(strip=True),
                    "Score": 0.0
                })
        
        # 2. Probing Finals Slots
        f_resp = requests.get(finals_url, headers=headers, timeout=10)
        f_soup = BeautifulSoup(f_resp.text, 'html.parser')
        f_rows = f_soup.find_all('div', class_='schedule-row')
        
        finals_slots = {}
        for row in f_rows:
            if 'schedule-row--custom' in row.get('class', []): continue
            initials = row.find('div', class_='schedule-row__initials')
            if initials:
                g_class = initials.get_text(strip=True)
                finals_slots[g_class] = finals_slots.get(g_class, 0) + 1
                
        return pd.DataFrame(prelims_data), finals_slots
    except Exception as e:
        return pd.DataFrame(), {}




def get_wgi_events_by_date(target_date):
    # This URL provides a direct, chronological list that isn't filtered like the calendar
    url = "https://www.wgi.org/scores/color-guard-scores/"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        events = []
        month_token = target_date.strftime("%b") # "Feb"
        day_token = str(target_date.day)         # "21"
        
        # On the scores page, events are listed in a table (tr/td)
        # This is a much more stable "Digital Signal" than the nested divs
        rows = soup.find_all('tr')
        
        for row in rows:
            row_text = row.get_text()
            
            # Logic: If 'Feb' and '21' are found in the same row
            if month_token in row_text and day_token in row_text:
                link = row.find('a', href=True)
                if link:
                    href = link['href']
                    full_url = href if href.startswith('http') else f"https://www.wgi.org{href}"
                    
                    events.append({
                        "name": link.get_text(strip=True),
                        "url": full_url
                    })
        
        # Clean up duplicates
        return list({v['url']:v for v in events}.values())
    except Exception as e:
        print(f"Scraper Error: {e}")
        return []

def pull_full_event_data(event_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(event_url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Pull ALL rows from the 'schedule-area'
        all_rows = soup.find_all('div', class_='schedule-row')
        
        prelims_data = []
        finals_slots_by_class = {}
        current_mode = "PRELIMS" # Start state
        
        for row in all_rows:
            # BROADENED SYNC LOGIC: Catch any mention of Finals
            row_text = row.get_text().upper().strip()
            
            # If the row itself is a header (no initials/time), it's likely a mode switch
            if "FINALS" in row_text and "PRELIMS" not in row_text:
                current_mode = "FINALS"
                continue
            
            # Skip 'Break' or 'Lunch' rows
            if 'schedule-row--custom' in row.get('class', []):
                continue

            # 2. DATA EXTRACTION
            try:
                name_div = row.find('div', class_='schedule-row__name')
                class_div = row.find('div', class_='schedule-row__initials')
                time_div = row.find('div', class_='schedule-row__time')
                
                # If we have a class and time, it's a valid slot
                if class_div and time_div:
                    guard_class = class_div.get_text(strip=True)
                    
                    if current_mode == "PRELIMS" and name_div:
                        # PRELIMS: We need the guard name for the roster
                        name = name_div.get_text(strip=True)
                        prelims_data.append({
                            "Guard": name, 
                            "Class": guard_class, 
                            "Perform Time": time_div.get_text(strip=True), 
                            "Score": 0.0
                        })
                    
                    elif current_mode == "FINALS":
                        # FINALS: We just increment the slot count for that class
                        finals_slots_by_class[guard_class] = finals_slots_by_class.get(guard_class, 0) + 1
            except AttributeError:
                continue
        
        return pd.DataFrame(prelims_data), finals_slots_by_class
    except Exception as e:
        print(f"Full Scraper Fault: {e}")
        return pd.DataFrame(), {}

        
def download_event_html(page, event):
    """Stage 2: Only downloads if the file doesn't exist."""
    file_path = os.path.join(RAW_DIR, f"{event['name']}_RAW.html")
    
    # Smart Update Check
    if os.path.exists(file_path):
        print(f"  Skipping {event['name']} (Already downloaded).")
        return True

    score_url = f"https://www.wgi.org/scores/color-guard-score-event/?ShowId={event['id']}"
    print(f"Downloading HTML for: {event['name']}...")
    try:
        page.goto(score_url)
        page.wait_for_selector("table", timeout=10000)
        page.wait_for_timeout(2000) 
        soup = BeautifulSoup(page.content(), 'html.parser')
        tables = soup.find_all('table')
        with open(file_path, "w", encoding="utf-8") as f:
            for table in tables:
                f.write(f"\n{table.prettify()}\n\n")
        return True
    except Exception as e:
        print(f"  Error downloading {event['name']}: {e}")
        return False

def parse_and_rank():
    """Stage 3: Logic to keep only the 'Weekend High' for each guard per event."""
    print("\nStarting data parsing with 'Weekend High' logic...")
    all_regional_data = []
    html_files = [f for f in os.listdir(RAW_DIR) if f.endswith('.html')]

    for filename in html_files:
        # We extract the base event name (e.g., 'Clayton' instead of 'Clayton_FINALS')
        # This helps us group Prelims and Finals from the same location together
        base_event = filename.split('_')[0] 
        file_path = os.path.join(RAW_DIR, filename)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        tables = soup.find_all('table')
        for table in tables:
            current_class = "Unknown Class"
            for row in table.find_all('tr'):
                div_header = row.find('th', class_='division-name')
                if div_header:
                    current_class = div_header.get_text(strip=True)
                    continue 

                cells = row.find_all('td')
                if len(cells) >= 3:
                    try:
                        team_name = cells[1].get_text(strip=True)
                        score_text = cells[2].get_text(strip=True).upper().replace("VIEW RECAP", "").strip()
                        
                        if team_name and score_text:
                            all_regional_data.append({
                                'Class': current_class,
                                'Guard': team_name,
                                'Score': float(score_text),
                                'Event_Group': base_event # Grouping ID
                            })
                    except: continue

    if all_regional_data:
        raw_df = pd.DataFrame(all_regional_data)
        
        # KEY STEP: For every guard at a specific event group (like Clayton),
        # only keep their highest score from that weekend.
        weekend_highs = raw_df.groupby(['Class', 'Guard', 'Event_Group'])['Score'].max().reset_index()

        # Now calculate national stats based on those Weekend Highs
        rankings = weekend_highs.groupby(['Class', 'Guard']).agg(
            Average_Score=('Score', 'mean'),
            Season_High=('Score', 'max'),
            Events_Count=('Event_Group', 'count')
        ).reset_index()

        rankings = rankings.sort_values(by=['Class', 'Average_Score'], ascending=[True, False]).round(2)
        rankings.to_csv(os.path.join(MASTER_DIR, "NATIONAL_PROJECTED_RANKINGS.csv"), index=False)
        print("Success! Master rankings updated using Weekend High logic.")

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        events = get_event_list(page)
        
        for event in events:
            download_event_html(page, event)
            time.sleep(1) # Polite delay
            
        browser.close()
    
    parse_and_rank()

if __name__ == "__main__":
    main()
