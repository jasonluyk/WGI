from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os
import io

def diagnostic_scraper():
    url = "https://www.wgi.org/scores/color-guard-score-event/?ShowId=a0uUy000004Hs05IAC"
    debug_dir = "debug_html"
    
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        page.wait_for_selector("table", timeout=15000)
        page.wait_for_timeout(3000)
        html_content = page.content()
        browser.close()

    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    print(f"\n--- Found {len(tables)} tables ---")

    for i, table in enumerate(tables):
        # 1. Save the raw HTML snippet for this table to a file
        file_path = os.path.join(debug_dir, f"table_{i}.html")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(str(table.prettify()))
        
        # 2. Print the structure to the console
        rows = table.find_all('tr')
        if rows:
            first_row = rows[0].find_all(['td', 'th'])
            print(f"Table {i}: {len(rows)} rows | {len(first_row)} columns detected")
            
            # Show a sample of the first data row
            if len(rows) > 1:
                sample_cells = [c.get_text(strip=True) for c in rows[1].find_all(['td', 'th'])]
                print(f"   Sample Row: {sample_cells}")
        
    print(f"\nDiagnostic complete. Check the '{debug_dir}' folder to see the raw HTML for each table.")

if __name__ == "__main__":
    diagnostic_scraper()
