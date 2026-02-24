import os
import subprocess
import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# --- 1. SYSTEM BOOT LATCH (The Post-Assembly Fix) ---
@st.cache_resource
def install_playwright_binaries():
    """Ensures the browser engine is seated before the app starts."""
    try:
        # Check if chromium is already installed; if not, install it.
        # This prevents the 'ModuleNotFoundError' and 'ExecutableNotFound'
        subprocess.run(["playwright", "install", "chromium"], check=True)
        return True
    except Exception as e:
        # If this fails, we want to know why on the UI
        print(f"Boot Error: {e}")
        return False

# Trigger the install sequence immediately upon import
install_playwright_binaries()


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
    """Standardizes the LUT for the UI dropdown."""
    events = []
    for name, path in EVENT_LUT.items():
        if isinstance(path, dict):
            url_data = path
        elif path.startswith("http"):
            url_data = path
        else:
            url_data = f"https://www.wgi.org/event-details-page/?eventId={path}&division=CG"
        events.append({"name": name, "url": url_data})
    return events

# --- 3. LIVE PROBE (The Playwright Scraper) ---
def pull_dual_event_data(prelims_url, finals_url):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
            page = browser.new_page()
            
            # --- CHANNEL A: PRELIMS ---
            page.goto(prelims_url, wait_until="networkidle", timeout=45000)
            page.wait_for_timeout(5000) # Give the JS clock time to cycle
            
            # Probing for the table body directly
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for the specific CompetitionSuite table class if it exists, 
            # otherwise grab the first available table
            table = soup.find('table')
            
            prelims_data = []
            if table:
                rows = table.find_all('tr')
                # Log the raw 'pin count' to the serial monitor
                print(f"DEBUG: Found {len(rows)} rows in table.") 
                
                for row in rows:
                    cols = row.find_all('td')
                    # CompetitionSuite Standard: Time(0), Guard(1), Class(2), Score(3)
                    if len(cols) >= 4:
                        name = cols[1].get_text(strip=True)
                        g_class = cols[2].get_text(strip=True)
                        time_str = cols[0].get_text(strip=True)
                        score_str = cols[3].get_text(strip=True)
                        
                        # Filter out header rows that contain 'Group' or 'Class' text
                        if "Group" in name or "Class" in name:
                            continue

                        try:
                            # Convert score string (e.g., "78.450") to float
                            score_val = float(score_str) if score_str else 0.0
                        except:
                            score_val = 0.0

                        if name and g_class:
                            prelims_data.append({
                                "Guard": name, "Class": g_class, 
                                "Perform Time": time_str, "Score": score_val
                            })
            
            # --- CHANNEL B: FINALS (SLOTS) ---
            f_slots = {}
            if finals_url:
                page.goto(finals_url, wait_until="networkidle")
                page.wait_for_timeout(3000)
                f_soup = BeautifulSoup(page.content(), 'html.parser')
                f_table = f_soup.find('table')
                if f_table:
                    for f_row in f_table.find_all('tr'):
                        f_cols = f_row.find_all('td')
                        if len(f_cols) >= 3:
                            s_class = f_cols[2].get_text(strip=True)
                            if s_class and s_class != "Class":
                                f_slots[s_class] = f_slots.get(s_class, 0) + 1
            
            browser.close()
            return pd.DataFrame(prelims_data), f_slots
    except Exception as e:
        print(f"Scraper Hardware Fault: {e}")
        return pd.DataFrame(), {}

# --- 4. ANALYTICS SCRAPER (Standard Requests) ---
def get_wgi_events_by_date(target_date):
    """Scrapes the main WGI score list using standard requests."""
    url = "https://www.wgi.org/scores/color-guard-scores/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        events = []
        month_token = target_date.strftime("%b")
        day_token = str(target_date.day)
        
        for row in soup.find_all('tr'):
            row_text = row.get_text()
            if month_token in row_text and day_token in row_text:
                link = row.find('a', href=True)
                if link:
                    href = link['href']
                    full_url = href if href.startswith('http') else f"https://www.wgi.org{href}"
                    events.append({"name": link.get_text(strip=True), "url": full_url})
        return list({v['url']:v for v in events}.values())
    except Exception:
        return []