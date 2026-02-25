#!/bin/bash

# Start the infinite scraper loop in the background
python scraper_worker.py &

# Start the Streamlit dashboard on the port DigitalOcean dynamically assigns
streamlit run dashboard.py --server.port=$PORT --server.address=0.0.0.0