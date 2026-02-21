from bs4 import BeautifulSoup
import re

# Load your uploaded file
with open("calendar.html", "r", encoding="utf-8") as f:
    soup = BeautifulSoup(f, "html.parser")

event_map = {}

# Find all location links
links = soup.find_all('a', class_='wgi-color-theme-el-text-color')

for link in links:
    name = link.get_text(strip=True)
    href = link.get('href', '')
    
    # Use Regex to find the 18-character Salesforce ID after 'eventId='
    match = re.search(r'eventId=([a-zA-Z0-9]{18})', href)
    if match:
        event_id = match.group(1)
        event_map[name] = event_id

# Print in a format you can copy/paste into wgi_final.py
print("EVENT_LUT = {")
for name, eid in event_map.items():
    print(f'    "{name}": "{eid}",')
print("}")
