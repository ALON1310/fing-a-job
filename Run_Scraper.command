#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
python3 scraper_agent.py
echo "------------------------------------------"
echo "Process Finished!"
read -p "Press Enter to close this window..."
