# Run the dbt command and capture its output and error
dbt ls $PROFILE --resource-type model --select state:modified,config.schema:no_schema --output path --state . --project-dir $PROJECT_DIR
test=$(dbt --quiet --no-print ls $PROFILE --resource-type model --select state:modified,config.schema:no_schema --output path --state . --project-dir $PROJECT_DIR 2>error_output.txt)
dbt_exit_status=$?

# Check if the dbt command failed
if [[ $dbt_exit_status -ne 0 ]]; then
    echo "Error: dbt command failed with exit status $dbt_exit_status"
    echo "dbt error output:"
    cat error_output.txt
    exit $dbt_exit_status
fi

# Check if the output of the dbt command is empty
if [[ -z "$test" ]]; then
    echo "Success: All models have a custom schema"
    exit 0
else
    echo "Found models without custom schema:"
    echo "$test"
    exit 1
fi
