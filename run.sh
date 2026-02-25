#!/bin/bash

export PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright
source /root/WGI/venv/bin/activate

# Start the infinite scraper loop in the background
python scraper_worker.py &

# Start the Streamlit dashboard
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0