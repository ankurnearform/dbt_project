#!/bin/bash

# Get the top-level directory of the Git repository
git_root=$(git rev-parse --show-toplevel)
if [ $? -ne 0 ]; then
  echo "Failed to find the Git project root. Please run this script from within the Git repository."
  exit 1
fi

# Set paths based on the Git root
dbt_project_path="$git_root/smart_code_generator/generated_code/dbt_project"
utils_path="$git_root/smart_code_generator/feedback/error_logs"

# Proceed with the script...
cd "$dbt_project_path" || { echo "Failed to change directory to $dbt_project_path"; exit 1; }
echo "Running DBT in $(pwd)"
output=$(dbt run 2>&1)
echo "DBT Output:"
echo "$output"

# Get the current timestamp
timestamp=$(date '+%Y-%m-%d %H:%M:%S')

# Generalize error detection
if echo "$output" | grep -q "Error"; then
    echo "Error detected, logging..."
    error_message=$(echo "$output" | grep "Error" -A 5)  # Adjust the number of context lines as necessary
    # Append the timestamp to the log
    log_output=$(python3 "$utils_path/error_logger.py" "DBT Error" "$error_message" "$timestamp")
    echo "Python script output:"
    echo "$log_output"
else
    echo "No Errors detected."
fi

echo "Script execution completed."
