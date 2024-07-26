#!/bin/bash
# Change to the target directory
cd /home/support-info/TM/09-monitoring-global || { echo "Failed to change directory"; exit 1; }

# Activate the virtual environment
source /home/support-info/TM/09-monitoring-global/venv/bin/activate

# Check if the virtual environment was activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
  echo "Virtual environment activated"
  # Run Streamlit
  python3 main.py
   
else
  echo "Failed to activate virtual environment"
fi

