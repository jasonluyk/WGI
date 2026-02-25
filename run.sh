#!/bin/bash
python seed_db.py


export PLAYWRIGHT_BROWSERS_PATH=/workspace/.cache/ms-playwright
playwright install --with-deps chromium

# 2. Start the infinite scraper loop in the background
python scraper_worker.py &

# 3. Start the Streamlit dashboard
streamlit run dashboard.py --server.port=$PORT --server.address=0.0.0.0