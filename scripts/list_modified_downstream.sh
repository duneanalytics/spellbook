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
    --resource-type model \
    --select state:modified+ \
    --output json \
    --output-keys alias schema config \
    --state . \
    --project-dir $PROJECT_DIR)

# Check if output is empty or invalid JSON
if [ -z "$dbt_output" ] || [ "$dbt_output" = "[]" ]; then
    echo "No modified models found"
    exit 0
fi

# Use jq to parse the JSON and format the output, then sort using sort command
echo "$dbt_output" | \
    jq -r '.[] | "\(.config.materialized // "view") \(.schema) \(.alias)"' | \
    sort | \
    while read -r materialization schema alias; do
        echo "[$materialization] - $schema.$alias"
    done
