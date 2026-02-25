import os
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/workspace/.cache/ms-playwright"
import time
import pymongo
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import streamlit as st 
import re
import requests
import pdfplumber
import io



# Connect to MongoDB
# 1. Look in the cloud first...
mongo_url = os.environ.get("MONGO_URI")

# 2. If we are on your desktop, just use the Streamlit secrets file!
if not mongo_url:
    mongo_url = st.secrets["MONGO_URI"]
client = pymongo.MongoClient(mongo_url)
db = client["rankings_2026"]
national_collection = db["wgi_analytics"]
live_collection = db["live_state"]
command_collection = db["system_state"]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"

def clean_class_name(raw_class):
    """Strips out WGI round/prelim/finals tags to keep classes unified."""
    clean = raw_class.strip()
    
    # 1. Handle WGI lazy data entry where they ONLY type "Round 1"
    if re.match(r'(?i)^Round\s*\d+', clean):
        return "Scholastic A"
        
    # 2. Aggressively strip tags even if they forgot the hyphen or parentheses
    # This catches "Scholastic A Round 1", "Scholastic A - Round 1", and "Scholastic A (Round 1)"
    clean = re.sub(r'(?i)\s*(?:-|\()?\s*(Prelims|Finals|Round\s*\d+|Semi.*)\)?', '', clean)
    
    # Fallback just in case aggressive stripping leaves an empty string
    return clean.strip() if clean.strip() else "Scholastic A"

# --- 1. THE NATIONAL LEDGER & ZERO-TOUCH DISCOVERY ENGINE ---
def scrape_national_scores():
    print("🚀 [WORKER] Running Zero-Touch Discovery (Calendar -> Details -> Scores)...")
    master_events = {} 

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, 
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        # --- HOP 1: GET EVENT DETAILS LINKS FROM CALENDAR ---
        print("🗓️ Hop 1: Hunting for Event Pages on WGI Calendar...")
        details_links = {}
        try:
            page.goto("https://www.wgi.org/color-guard/cg-calendar/")
            page.wait_for_timeout(5000)
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            for link in soup.find_all('a', href=re.compile(r'event-details-page')):
                href = link['href']
                event_name = "Unknown Event"
                parent = link.find_parent(['div', 'li', 'article', 'td'])
                if parent:
                    header = parent.find(['h2', 'h3', 'h4', 'strong', 'span'])
                    if header:
                        event_name = header.get_text(strip=True)
                
                clean_name = event_name.split(",")[0].replace("Regional", "").strip()
                full_url = href if href.startswith('http') else f"https://www.wgi.org{href}"
                details_links[clean_name] = full_url
                
            print(f"✅ Found {len(details_links)} Event Details pages.")
        except Exception as e:
            print(f"⚠️ [WORKER] Calendar Scrape Error: {e}")

        # --- HOP 2: SCAN EVENT PAGES FOR SCHEDULE URLS ---
        print("🔍 Hop 2: Scanning Event Pages for Schedule Links...")
        for event_name, event_url in details_links.items():
            print(f"  -> Scanning Event Page: {event_name}...")
            p_url = ""
            f_url = ""
            try:
                page.goto(event_url)
                page.wait_for_timeout(5000) 
                soup = BeautifulSoup(page.content(), 'html.parser')
                
                for a in soup.find_all('a', href=True):
                    link_text = a.get_text(strip=True).lower()
                    href = a['href']
                    if "prelims" in link_text and "regional a" not in link_text and not p_url:
                        p_url = href
                        print(f"      🔗 Found Main Prelims: {p_url}")
                    elif "finals" in link_text and "regional a" not in link_text and not f_url:
                        f_url = href
                        print(f"      🔗 Found Main Finals: {f_url}")
            except Exception as e:
                print(f"⚠️ [WORKER] Error scanning {event_name}: {e}")
            
            # THE FIX: Save the URLs to the dictionary so they survive Hop 3!
            master_events[event_name] = {
                "name": event_name,
                "p_url": p_url,
                "f_url": f_url,
                "show_id": ""
            }

        # --- HOP 3: WGI SCORES FOR SHOW IDs ---
        print("🔍 Hop 3: Hunting for ShowIDs on WGI Scores Page...")
        try:
            page.goto("https://www.wgi.org/scores/color-guard-scores/")
            page.wait_for_timeout(5000) 
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'ShowId=' in href:
                    show_name = link.get_text(strip=True)
                    if not show_name or "View" in show_name or "Score" in show_name:
                        row = link.find_parent('tr')
                        if row:
                            cols = row.find_all('td')
                            if len(cols) > 0:
                                show_name = cols[0].get_text(strip=True)
                    
                    clean_score_name = show_name.split("Regional")[0].strip() if show_name else "Unknown Event"
                    extracted_id = href.split("ShowId=")[-1]
                    
                    matched = False
                    for key in master_events.keys():
                        if clean_score_name.lower() in key.lower() or key.lower() in clean_score_name.lower():
                            master_events[key]["show_id"] = extracted_id
                            matched = True
                            break
                    
                    if not matched:
                         master_events[clean_score_name] = {"name": clean_score_name, "show_id": extracted_id, "p_url": "", "f_url": ""}
                        
            print(f"✅ Successfully mapped ShowIDs to the master dictionary.")
        except Exception as e:
             print(f"⚠️ [WORKER] Scores Scrape Error: {e}")

        browser.close()

    # --- FINAL DB UPDATE ---
    event_metadata_list = list(master_events.values())
    if event_metadata_list:
        db["event_metadata"].delete_many({})
        db["event_metadata"].insert_many(event_metadata_list)
        db["system_state"].update_one(
            {"type": "discovery_status"},
            {"$set": {"status": "complete", "count": len(event_metadata_list)}},
            upsert=True
        )
        print(f"🎉 [WORKER] Zero-Touch Sync Complete! {len(event_metadata_list)} total events ready in UI.")
    else:
        db["system_state"].update_one(
            {"type": "discovery_status"},
            {"$set": {"status": "failed", "error": "No events found"}},
            upsert=True
        )
        print("❌ [WORKER] Discovery failed. No events found.")

def parse_pdf_schedule(pdf_url, combined_data):
    print(f"📄 [TRAFFIC COP] Running Ultimate PDF Parser: {pdf_url}")
    
    class_map = {
        "SRA": "Scholastic Regional A",
        "SA": "Scholastic A",
        "SO": "Scholastic Open",
        "SW": "Scholastic World",
        "IRA": "Independent Regional A",
        "IA": "Independent A",
        "IO": "Independent Open",
        "IW": "Independent World"
    }
    debug_log = open("root/WGI/pdf_debug.txt","w")
    try:
        response = requests.get(pdf_url)
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                for line in text.split('\n'):
                    line = line.strip()
                    
                    # Skip the giant diagonal "D R A F T" watermark
                    if len(line) < 5: continue
                        
                    match = re.search(r'^(.*?)\s+(SRA|SA|SO|SW|IRA|IA|IO|IW)(?:\s*-\s*ROUND\D*(\d+))?\s+(\d{1,2}:\d{2}\s*[AP]M)$', line, re.IGNORECASE)
                    
                    if match:
                        raw_front_text = match.group(1).strip()
                        debug_log.write(f"RAW: '{raw_front_text}'\n")
                        debug_log.flush()
                        base_abbr = match.group(2).upper()
                        round_num = match.group(3) 
                        time_str = match.group(4).strip()
                        
                        if ',' in raw_front_text:
                            # Strip state (everything after last comma e.g. ", SC")
                            before_comma = raw_front_text.rsplit(',', 1)[0].strip()
                            
                            # Remove zip codes and parenthetical state codes e.g. "(NC)", "33463"
                            before_comma = re.sub(r'\(\w{2}\)', '', before_comma).strip()
                            before_comma = re.sub(r'\b\d{5}\b', '', before_comma).strip()
                            
                            # Find the LAST school/group keyword and cut everything after it
                            # Keeps JV/Varsity/A/B since they appear BEFORE the city
                            school_pattern = re.search(
                                r'^(.*?(?:High School|HS|Academy|Winterguard|WG|Independent|Performing Arts|Visual Productions|Nuance\s+\w+)(?:\s+(?:JV|Varsity|[A-Z]))?)',
                                before_comma, re.IGNORECASE
                            )
                            if school_pattern:
                                guard_name = school_pattern.group(1).strip()
                            else:
                                # No keyword found (e.g. "Paramount", "Etude", "First Flight")
                                # Strip last word as city
                                guard_name = before_comma.rsplit(' ', 1)[0].strip()

                        else:
                            guard_name = raw_front_text

                        # Strip leading stray single capital letter (e.g. "D East Lincoln" -> "East Lincoln")
                        guard_name = re.sub(r'^[A-Z](?=[A-Z])', '', guard_name).strip()

                        # Strip leading stray digits
                        guard_name = re.sub(r'^\d+\s+', '', guard_name).strip()

                        # Strip trailing truncation artifacts (e.g. "from Ge…")
                        guard_name = re.sub(r'\s+from\s+\w+…?$', '', guard_name, flags=re.IGNORECASE).strip()
                        
                        if round_num:
                            g_class = f"{base_clean} - Round {round_num}"
                        else:
                            g_class = base_clean
                        
                        combined_data[guard_name] = {
                            "Guard": guard_name, "Class": g_class, 
                            "Prelims Time": time_str, "Prelims Score": 0.0,
                            "Finals Time": "", "Finals Score": 0.0
                        }
                        print(f"➕ Found Guard: {guard_name} ({g_class}) @ {time_str}")
        debug_log.close()                
    except Exception as e:
        print(f"⚠️ [WORKER] PDF Parser Failed: {e}")

def parse_html_schedule(html_url, combined_data, page):
    print(f"📡 [TRAFFIC COP] Routing to HTML Parser: {html_url}")
    try:
        page.goto(html_url)
        page.wait_for_timeout(5000) 
        soup = BeautifulSoup(page.content(), 'html.parser')
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 3:
                    time_str = cols[0].get_text(strip=True)
                    name = cols[1].get_text(strip=True)
                    if "Group" in name or "Class" in name or "Break" in name: continue
                    g_class = clean_class_name(cols[2].get_text(strip=True))
                    
                    combined_data[name] = {
                        "Guard": name, "Class": g_class, 
                        "Prelims Time": time_str, "Prelims Score": 0.0,
                        "Finals Time": "", "Finals Score": 0.0
                    }
    except Exception as e:
        print(f"⚠️ [WORKER] HTML Parser Failed: {e}")

# --- FINALS SPOT COUNTERS (Pass 2) ---

def count_pdf_finals_spots(pdf_url, class_spots):
    print(f"📄 [TRAFFIC COP] Routing to PDF Finals Spot Counter: {pdf_url}")
    class_map = {
        "SRA": "Scholastic Regional A", "SA": "Scholastic A",
        "SO": "Scholastic Open", "SW": "Scholastic World",
        "IRA": "Independent Regional A", "IA": "Independent A",
        "IO": "Independent Open", "IW": "Independent World"
    }
    try:
        response = requests.get(pdf_url)
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                for line in text.split('\n'):
                    line = line.strip()
                    if len(line) < 5: continue
                    
                    # Regex: Just look for the abbreviation and a Time at the end of the line
                    match = re.search(r'(SRA|SA|SO|SW|IRA|IA|IO|IW)\s+(\d{1,2}:\d{2}\s*[AP]M)$', line, re.IGNORECASE)
                    if match:
                        base_abbr = match.group(1).upper()
                        full_class = class_map.get(base_abbr, base_abbr)
                        g_class = clean_class_name(full_class)
                        
                        # Add 1 to the counter for this class
                        class_spots[g_class] = class_spots.get(g_class, 0) + 1
                        print(f"🎯 Finals Spot Found: {g_class} (Total so far: {class_spots[g_class]})")
                        
    except Exception as e:
        print(f"⚠️ [WORKER] PDF Finals Parser Failed: {e}")


def count_html_finals_spots(html_url, class_spots, page):
    print(f"📡 [TRAFFIC COP] Routing to HTML Finals Spot Counter: {html_url}")
    try:
        page.goto(html_url)
        page.wait_for_timeout(5000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 3:
                    raw_g_class = cols[2].get_text(strip=True)
                    if "Class" in raw_g_class or not raw_g_class: continue
                    g_class = clean_class_name(raw_g_class)
                    
                    class_spots[g_class] = class_spots.get(g_class, 0) + 1
    except Exception as e:
        print(f"⚠️ [WORKER] HTML Finals Parser Failed: {e}")


# --- 2. THE LIVE SHOW SCRAPER (The Orchestrator) ---
def scrape_live_show(show_id, prelims_url, finals_url):
    print(f"🚀 [WORKER] Running Hybrid Live Scrape...")
    combined_data = {}
    class_spots = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        # --- PASS 1: PRELIMS SCHEDULE (The Traffic Cop) ---
        if prelims_url:
            if prelims_url.lower().endswith('.pdf'):
                parse_pdf_schedule(prelims_url, combined_data)
            else:
                parse_html_schedule(prelims_url, combined_data, page)

        # --- PASS 2: FINALS SPOT COUNTER (The Traffic Cop) ---
        if finals_url:
            if finals_url.lower().endswith('.pdf'):
                count_pdf_finals_spots(finals_url, class_spots)
            else:
                count_html_finals_spots(finals_url, class_spots, page)

        # --- PASS 3: WGI SCORES (The Ultimate Source of Truth) ---
        if show_id and str(show_id).strip() != "":
            wgi_url = f"https://www.wgi.org/scores/color-guard-score-event/?ShowId={show_id}"
            print(f"📡 Probing WGI Scores: {wgi_url}")
            try:
                page.goto(wgi_url)
                page.wait_for_timeout(4000)
                
                soup = BeautifulSoup(page.content(), 'html.parser')
                for table in soup.find_all('table'):
                    raw_class = "Unknown Class"
                    for row in table.find_all('tr'):
                        th_cells = row.find_all('th')
                        if th_cells:
                            if len(th_cells) == 1: raw_class = th_cells[0].get_text(strip=True)
                            elif row.find(['th', 'td'], class_='division-name'):
                                raw_class = row.find(['th', 'td'], class_='division-name').get_text(strip=True)
                            continue 
                        
                        cols = row.find_all('td')
                        if len(cols) >= 3:
                            team_name = cols[1].get_text(strip=True)
                            score_text = cols[2].get_text(strip=True).upper().replace("VIEW RECAP", "").strip()
                            
                            try: score = float(score_text)
                            except ValueError: continue
                            if not team_name: continue

                            base_class = clean_class_name(raw_class)
                            
                            # If guard isn't in schedule (e.g. past event or schedule failed), add them!
                            if team_name not in combined_data:
                                combined_data[team_name] = {
                                    "Guard": team_name, "Class": base_class, 
                                    "Prelims Time": "Finished", "Prelims Score": 0.0,
                                    "Finals Time": "", "Finals Score": 0.0
                                }
                            
                            # Inject score and replace time
                            if "Final" in raw_class or "Finals" in raw_class:
                                combined_data[team_name]["Finals Score"] = score
                                combined_data[team_name]["Finals Time"] = "✅" 
                            else:
                                combined_data[team_name]["Prelims Score"] = score
                                combined_data[team_name]["Prelims Time"] = "✅"
            except Exception as e:
                print(f"⚠️ [WORKER] WGI Scrape Error: {e}")

        browser.close()

    final_list = list(combined_data.values())
    if final_list:
        live_collection.update_one(
            {"type": "current_session"}, 
            {"$set": {"data": final_list, "spots": class_spots}}, 
            upsert=True
        )
        print(f"✅ [WORKER] Updated Live Show with {len(final_list)} guards.")
    else:
        print("❌ [WORKER] Live scrape finished, but no data was found.")

# --- 3. THE PAST EVENTS ARCHIVE SCRAPER ---
def scrape_archive(show_id, event_name):
    print(f"📦 [WORKER] Pulling Archive Scores for {event_name} (ShowID: {show_id})...")
    archive_data = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        wgi_url = f"https://www.wgi.org/scores/color-guard-score-event/?ShowId={show_id}"
        try:
            page.goto(wgi_url)
            page.wait_for_timeout(5000) # Wait 5 seconds for Salesforce to load the tables!
            
            soup = BeautifulSoup(page.content(), 'html.parser')
            current_class = "Unknown Class"
            
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    th_cells = row.find_all('th')
                    if th_cells:
                        if len(th_cells) == 1: 
                            current_class = th_cells[0].get_text(strip=True)
                        elif row.find(['th', 'td'], class_='division-name'):
                            current_class = row.find(['th', 'td'], class_='division-name').get_text(strip=True)
                        continue 
                    
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        team = cols[1].get_text(strip=True)
                        score_text = cols[2].get_text(strip=True).upper().replace("VIEW RECAP", "").strip()
                        
                        try: score = float(score_text)
                        except ValueError: continue
                        
                        if team:
                            archive_data.append({
                                "Guard": team,
                                "Class": clean_class_name(current_class), # Ensure names match
                                "Final Score": score
                            })
        except Exception as e:
            print(f"⚠️ [WORKER] Archive Scrape Error: {e}")

        browser.close()

    if archive_data:
        # Sort highest scores to the top, grouped by class
        archive_data = sorted(archive_data, key=lambda x: (x["Class"], -x["Final Score"]))
        db["archive_state"].update_one(
            {"type": "current_archive"}, 
            # THE FIX: Added "status": "complete"
            {"$set": {"event_name": event_name, "show_id": show_id, "data": archive_data, "status": "complete"}}, 
            upsert=True
        )
        print(f"✅ [WORKER] Successfully archived {len(archive_data)} scores for {event_name}.")
    else:
        # THE FIX: Tell Streamlit we failed so it stops spinning
        db["archive_state"].update_one(
            {"type": "current_archive"}, 
            {"$set": {"status": "empty", "event_name": event_name}}
        )
        print("❌ [WORKER] Archive finished, but no scores were found.")

def scrape_projection(show_name, prelims_url, finals_url):
    """
    Lean version of scrape_live_show — same roster + spot parsing,
    but replaces live scores with season averages from wgi_analytics.
    """
    print(f"🔮 [WORKER] Building Projection for: {show_name}...")
    combined_data = {}
    class_spots = {}

    # --- PASS 1: Roster from prelims PDF (identical to Live Hub) ---
    if prelims_url and prelims_url.lower().endswith('.pdf'):
        parse_pdf_schedule(prelims_url, combined_data)
    else:
        db["projection_state"].update_one(
            {"type": "current_projection"},
            {"$set": {"status": "failed", "error": "Only PDF schedule URLs are supported."}},
            upsert=True
        )
        return

    if not combined_data:
        db["projection_state"].update_one(
            {"type": "current_projection"},
            {"$set": {"status": "failed", "error": "No guards found in PDF. Is the schedule posted yet?"}},
            upsert=True
        )
        return

    print(f"✅ Found {len(combined_data)} guards in roster.")

    # --- PASS 2: Finals spot counts from finals PDF (identical to Live Hub) ---
    if finals_url and finals_url.lower().endswith('.pdf'):
        count_pdf_finals_spots(finals_url, class_spots)
        print(f"✅ Finals spots: {class_spots}")

    # --- PASS 3: Replace live scores with season averages from MongoDB ---
    for guard_name, guard_data in combined_data.items():
        base_class = guard_data["Class"].split(" - ")[0].strip()

        scores = list(db["wgi_analytics"].find(
            {"Guard": guard_name, "Class": base_class},
            {"_id": 0, "Score": 1}
        ))

        if scores:
            avg = round(sum(s["Score"] for s in scores) / len(scores), 3)
            combined_data[guard_name]["Prelims Score"] = avg
            combined_data[guard_name]["Shows Attended"] = len(scores)
        # If no data, Prelims Score stays 0.0 from parse_pdf_schedule

    # --- SAVE TO MONGODB ---
    final_list = list(combined_data.values())
    if final_list:
        db["projection_state"].update_one(
            {"type": "current_projection"},
            {"$set": {
                "show_name": show_name,
                "data": final_list,
                "spots": class_spots,
                "status": "complete"
            }},
            upsert=True
        )
        print(f"🎉 [WORKER] Projection complete! {len(final_list)} guards saved.")
    else:
        db["projection_state"].update_one(
            {"type": "current_projection"},
            {"$set": {"status": "failed", "error": "No projection data generated."}},
            upsert=True
        )

# =====================================================================
# --- THE WORKER BRAIN (Command Listener) ---
# =====================================================================
import time

if __name__ == "__main__":
    print("⚙️ Worker Node Online. Listening for Streamlit commands...")
    
    # Optional: Clear out any old, stuck commands when booting up
    db["system_state"].delete_many({"type": "scraper_command"})
    
    while True:
        # Check the database for a new command from Streamlit
        command = db["system_state"].find_one({"type": "scraper_command"})
        
        if command:
            action = command.get("action")
            print(f"\n📥 Received command: {action}")
            
            try:
                # 1. Auto-Discovery
                if action == "sync_national":
                    scrape_national_scores()
                
                # 2. Live Weekend Event (Schedules & Active Scores)
                elif action == "sync_live":
                    scrape_live_show(
                        command.get("show_id"), 
                        command.get("prelims_url"), 
                        command.get("finals_url")
                    )
                
                # 3. Past Events Archive (Scores Only)
                elif action == "sync_archive":
                    scrape_archive(
                        command.get("show_id"), 
                        command.get("event_name")
                    )

                elif action == "sync_projection":
                    scrape_projection(
                        command.get("show_name"),
                        command.get("prelims_url"),
                        command.get("finals_url")
                    )
                    
            except Exception as e:
                print(f"❌ [WORKER] Fatal error executing command '{action}': {e}")
            
            # Delete the command from the database so it only runs once
            db["system_state"].delete_one({"_id": command["_id"]})
            print("⏳ Task complete. Listening for next command...")
            
        # Wait 2 seconds before checking the database again to prevent CPU burnout
        time.sleep(2)