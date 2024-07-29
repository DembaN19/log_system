#!/bin/bash
# Change to the target directory
cd "$PWD" || { echo "Failed to change directory"; exit 1; }

# Activate the virtual environment
source $PWD/venv/bin/activate

# Check if the virtual environment was activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
  echo "Virtual environment activated"
  # Run Streamlit
  python3 auto_pdf.py
   
else
  echo "Failed to activate virtual environment"
fi

