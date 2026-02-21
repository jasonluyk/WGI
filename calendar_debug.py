from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import os

def calendar_diagnostic():
    url = "https://www.wgi.org/color-guard/cg-calendar/"
    debug_dir = "debug_calendar"
    
    if not os.path.exists(debug_dir):
        os.makedirs(debug_dir)

    with sync_playwright() as p:
        print(f"📡 Opening {url}...")
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        
        # Increase timeout to ensure JavaScript-rendered dates appear
        page.wait_for_timeout(7000) 
        html_content = page.content()
        browser.close()

    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 1. Save the full page HTML for offline inspection
    with open(os.path.join(debug_dir, "full_page.html"), "w", encoding="utf-8") as f:
        f.write(soup.prettify())

    # 2. Logic Check: Search for a known event date (e.g., Saturday Feb 21)
    target_date_text = "Feb 21" 
    print(f"\n--- Searching for '{target_date_text}' containers ---")
    
    # Look for any element containing the date text
    found_elements = soup.find_all(lambda tag: target_date_text in tag.text and tag.name not in ['html', 'body', 'script'])
    
    for i, elem in enumerate(found_elements[:10]):
        # We want to see the 'Parent' of the date text to find the structure
        parent = elem.parent
        print(f"Match {i}: Tag=<{elem.name}> | Parent Class={parent.get('class', 'No Class')}")
        
        # Save a snippet of this section
        with open(os.path.join(debug_dir, f"sample_{i}.html"), "w", encoding="utf-8") as f:
            f.write(parent.prettify())

    print(f"\n✅ Diagnostic complete. Check the '{debug_dir}' folder.")

if __name__ == "__main__":
    calendar_diagnostic()