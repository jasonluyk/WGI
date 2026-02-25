#!/bin/bash

# 1. Force the server to download the Chromium browser right as it boots up
playwright install chromium

# 2. Start the infinite scraper loop in the background
python scraper_worker.py &

# 3. Start the Streamlit dashboard
streamlit run dashboard.py --server.port=$PORT --server.address=0.0.0.0