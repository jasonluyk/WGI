import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import pymongo
import streamlit as st
import re


def clean_class_name(raw_class):
    """Strips out WGI round/prelim/finals tags to keep classes unified."""
    clean = re.sub(r'(?i)\s*-\s*(Prelims|Finals|Round.*|Semi.*)', '', raw_class)
    clean = re.sub(r'(?i)\s*\((Prelims|Finals|Round.*|Semi.*)\)', '', clean)
    return clean.strip()

def scrape_all_wgi_to_mongo():
    master_dict = {}

    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(
            headless=True, 
            args=["--disable-blink-features=AutomationControlled"]
        )

        # 1. Put on the mask (Spoofing a real Windows machine)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            
        )

        # 2. Open the page using that disguised context
        page = context.new_page()

        # 3. Now go to the URL (Keep your 60s timeout!)
        page.goto(
            "https://www.wgi.org/scores/color-guard-scores/", 
            timeout=60000, 
            wait_until="domcontentloaded"
        )

        # --- PART 1: GET ALL WGI EVENT URLs AND SHOW NAMES ---
        print("Fetching master list of WGI events...")
        page.goto(
            "https://www.wgi.org/scores/color-guard-scores/", 
            timeout=60000, 
            wait_until="domcontentloaded"
        )
        
        try:
            page.wait_for_selector("a[href*='ShowId']", timeout=20000)
        except Exception:
            print("Timeout waiting for main page links.")
        
        soup = BeautifulSoup(page.content(), 'html.parser')
        live_shows = {}
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'ShowId=' in href:
                # Attempt to grab the show name from the link or its parent row
                show_name = link.get_text(strip=True)
                if not show_name or "View" in show_name or "Score" in show_name:
                    row = link.find_parent('tr')
                    if row:
                        cols = row.find_all('td')
                        if len(cols) > 0:
                            show_name = cols[0].get_text(strip=True)
                
                if not show_name: show_name = "Unknown Regional"
                
                full_url = href if href.startswith('http') else f"https://www.wgi.org{href}"
                live_shows[full_url] = show_name
        
        print(f"Found {len(live_shows)} unique regional events.")

        # --- PART 2: SCRAPE EVERY EVENT USING YOUR TRUSTED LOGIC ---
        for idx, (url, show_name) in enumerate(live_shows.items()):
            print(f"Scraping event {idx + 1} of {len(live_shows)}: {show_name}...")
            try:
                page.goto(url)
                page.wait_for_selector("table", timeout=15000)
                page.wait_for_timeout(4000) 
                
                event_soup = BeautifulSoup(page.content(), 'html.parser')
                all_tables = event_soup.find_all('table')
                
                for table in all_tables:
                    current_class = "Unknown Class"
                    
                    for row in table.find_all('tr'):
                        div_header = row.find('th', class_='division-name')
                        if div_header:
                            current_class = clean_class_name(div_header.get_text(strip=True)) 
                            continue 

                        cells = row.find_all('td')
                        if len(cells) >= 3:
                            try:
                                team_name = cells[1].get_text(strip=True)
                                score_clean = cells[2].get_text(strip=True).upper().replace("VIEW RECAP", "").strip()
                                score = float(score_clean)

                                # UNIQUE KEY: Guard + Class + Show (Ensures all performances are saved)
                                guard_key = f"{team_name}_{current_class}_{show_name}"
                                
                                # If they performed in prelims and finals at the SAME show, keep the higher score
                                if guard_key in master_dict:
                                    if score > master_dict[guard_key]['Score']:
                                        master_dict[guard_key]['Score'] = score
                                else:
                                    master_dict[guard_key] = {
                                        'Show': show_name,
                                        'Class': current_class,
                                        'Guard': team_name,
                                        'Score': score
                                    }
                            except (ValueError, IndexError):
                                continue
            except Exception as e:
                print(f"No data or timeout at {show_name}.")
        
        browser.close()

    # --- PART 3: FINAL DATABASE EXPORT ---
    master_list = list(master_dict.values())
    
    if master_list:
        df = pd.DataFrame(master_list)

        print("\nConnecting to MongoDB...")
        # 1. Look in the cloud environment first
        mongo_url = os.environ.get("MONGO_URI")

        # 2. If it's empty (running locally), use Streamlit secrets
        if not mongo_url:
            mongo_url = st.secrets["MONGO_URI"]

        client = pymongo.MongoClient(mongo_url)
        
        db = client["rankings_2026"]
        collection = db["wgi_analytics"]

        collection.drop()
        print("Dropped old collection.")

        records = df.to_dict("records")
        collection.insert_many(records)
        
        print(f"Success! {len(records)} individual performances saved to MongoDB.")
    else:
        print("No data found across all tables.")

if __name__ == "__main__":
    scrape_all_wgi_to_mongo()