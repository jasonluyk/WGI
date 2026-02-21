from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import io

def scrape_all_wgi_tables():
    url = "https://www.wgi.org/scores/color-guard-score-event/?ShowId=a0uUy000004Hs05IAC"
    
    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        
        # Wait for the first table to appear
        page.wait_for_selector("table", timeout=15000)
        # Extra time for the JS to finish rendering ALL tables
        page.wait_for_timeout(4000) 
        
        html_content = page.content()
        browser.close()

    soup = BeautifulSoup(html_content, 'html.parser')
    master_list = []
    
    # 1. Find ALL tables on the page
    all_tables = soup.find_all('table')
    print(f"Found {len(all_tables)} tables. Processing all sections...")

    for table in all_tables:
        current_class = "Unknown Class"
        
        # 2. Iterate through every row in THIS specific table
        for row in table.find_all('tr'):
            
            # Check for the Division Header
            div_header = row.find('th', class_='division-name')
            if div_header:
                current_class = div_header.get_text(strip=True)
                continue 

            # Process Team Rows
            cells = row.find_all('td')
            if len(cells) >= 3:
                try:
                    # Column 1: Team Name | Column 2: Score
                    team_name = cells[1].get_text(strip=True)
                    score_text = cells[2].get_text(strip=True)
                    
                    # Clean the score (remove "View Recap")
                    score_clean = score_text.upper().replace("VIEW RECAP", "").strip()
                    
                    # Only add if we have a valid number
                    score = float(score_clean)

                    master_list.append({
                        'Class': current_class,
                        'Guard': team_name,
                        'Score': score
                    })
                except (ValueError, IndexError):
                    # Skips rows that aren't score data
                    continue

    # 3. Final Export
    if master_list:
        df = pd.DataFrame(master_list)
        # Drop duplicates just in case some tables overlap
        df = df.drop_duplicates()
        # Sort by Class and then Score
        df = df.sort_values(by=['Class', 'Score'], ascending=[True, False])
        
        df.to_csv('wgi_2026_complete_scores.csv', index=False)
        print(f"Success! {len(df)} teams saved to wgi_2026_complete_scores.csv")
        print(df.tail(10)) # Show the end to confirm we hit the bottom classes
    else:
        print("No data found across all tables.")

if __name__ == "__main__":
    scrape_all_wgi_tables()
