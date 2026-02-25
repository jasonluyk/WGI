#!/bin/bash

source /root/WGI/venv/bin/activate
export MONGO_URI="mongodb+srv://jason:bsidata@bsicluster.uifuc9m.mongodb.net/?appName=BSICluster"
export ADMIN_PASS="WE7AJNoQ456^%$"

cd /root/WGI

# 1. Seed the database first
echo "Seeding database..."
python seed_db.py

# 2. Then launch the app
echo "Launching app..."
bash run.sh