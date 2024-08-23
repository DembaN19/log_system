#!/bin/bash
# Change to the target directory
# Define the target directory
TARGET_DIR="/home/support-info/TM/01-PCC/06-monitoring_global"

cd "$TARGET_DIR" || { echo "Failed to change directory to $TARGET_DIR" >> "$TARGET_DIR/logs/cronjob.log" 2>&1; exit 1; }

# Activate the virtual environment
source "$TARGET_DIR/venv/bin/activate"

# Check if the virtual environment was activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
  datetime=$(date +"%d-%m-%Y %H:%M:%S")
  echo "Script start at: $datetime" > "$TARGET_DIR/logs/cronjob.log" 2>&1
  echo "Virtual environment activated" >> "$TARGET_DIR/logs/cronjob.log" 2>&1
  
  python3 "$TARGET_DIR/auto_pdf.py" >> "$TARGET_DIR/logs/cronjob.log" 2>&1
  
  echo "Job done" >> "$TARGET_DIR/logs/cronjob.log" 2>&1
else
  echo "Failed to activate virtual environment" >> "$TARGET_DIR/logs/cronjob.log" 2>&1
  exit 1
fi