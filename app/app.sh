#!/bin/bash
# Start ssh server
service ssh restart
chmod +x /app/mapreduce/mapper1.py
chmod +x /app/mapreduce/reducer1.py
# Starting the services
bash start_services.sh
# Creating a virtual environment
python3 -m venv .venv
source /app/.venv/bin/activate
# Install any packages
pip install -r requirements.txt  
# Package the virtual env.
venv-pack -o .venv.tar.gz
# Collect data
bash prepare_data.sh
# Run the indexer
bash index.sh
# Run the ranker
bash search.sh "this is a query!"