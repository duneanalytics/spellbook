#!/bin/bash

# Check if required environment variables are set
if [ -z "$PROFILE" ] || [ -z "$PROJECT_DIR" ]; then
    echo "Error: PROFILE and PROJECT_DIR environment variables must be set"
    echo "Example usage:"
    echo "PROFILE=myprofile PROJECT_DIR=/path/to/project ./script.sh"
    exit 1
fi

# Run dbt command and store output
dbt_output=$(dbt ls $PROFILE \
    --quiet \
    --no-print \
    --resource-type model \
    --select state:modified+ \
    --output json \
    --output-keys alias schema config \
    --state . \
    --project-dir $PROJECT_DIR)

# For debugging
echo "Raw output:"
echo "$dbt_output"

# Check for no output or error messages
if [ -z "$dbt_output" ] || echo "$dbt_output" | grep -q "No nodes selected"; then
    echo "No modified models found"
    exit 0
fi

# Process the output line by line and format each JSON object
echo "$dbt_output" | \
    while IFS= read -r line; do
        # Skip empty lines
        [ -z "$line" ] && continue
        # Extract and format each line
        echo "$line" | jq -r '"\(.config.materialized) \(.config.schema) \(.alias)"'
    done | \
    sort | \
    while IFS= read -r line; do
        # Split the line into components and format
        materialization=$(echo "$line" | cut -d' ' -f1)
        schema=$(echo "$line" | cut -d' ' -f2)
        alias=$(echo "$line" | cut -d' ' -f3)
        echo "[$materialization] - $schema.$alias"
    done
