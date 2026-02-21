from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os

def save_raw_html_for_all_finals():
    index_url = "https://www.wgi.org/scores/color-guard-scores/"
    debug_dir = "debug_html"
    
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("Finding Finals events on index page...")
        page.goto(index_url)
        page.wait_for_selector("table", timeout=15000)
        
        # 1. Collect all ShowIds for Finals
        events = []
        rows = page.query_selector_all("tr")
        for row in rows:
            row_text = row.inner_text().upper()
            if "FINALS" in row_text:
                link = row.query_selector("a[href*='ShowId=']")
                if link:
                    href = link.get_attribute("href")
                    show_id = href.split("ShowId=")[1].split("&")[0]
                    # Get the event name from the row to use as a filename
                    event_name = row_text.split('\t')[0].replace(" ", "_").strip()
                    events.append({"id": show_id, "name": event_name})

        print(f"Found {len(events)} events. Starting HTML download...")

        # 2. Visit each event and save the table HTML
        for event in events:
            score_url = f"https://www.wgi.org/scores/color-guard-score-event/?ShowId={event['id']}"
            print(f"Downloading HTML for: {event['name']}...")
            
            try:
                page.goto(score_url)
                page.wait_for_selector("table", timeout=10000)
                page.wait_for_timeout(2000) # Ensure JS finishes
                
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Grab every table on the page
                tables = soup.find_all('table')
                
                # Save to a text file
                file_path = os.path.join(debug_dir, f"{event['name']}_RAW.html")
                with open(file_path, "w", encoding="utf-8") as f:
                    for i, table in enumerate(tables):
                        f.write(f"\n")
                        f.write(table.prettify())
                        f.write(f"\n\n\n")
                
            except Exception as e:
                print(f"Could not download {event['name']}: {e}")

        browser.close()
    print(f"\nFinished! All HTML files are in the '{debug_dir}' folder.")

if __name__ == "__main__":
    save_raw_html_for_all_finals()
