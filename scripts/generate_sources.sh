#!/bin/bash

# bug: dbt-utils get_tables_by_pattern does not work with trino because it uses `ilike` instead of `like`
# workaround: modify the macro under `tokens/dbt_packages/dbt_utils/macros/sql/get_tables_by_pattern_sql.sql` to use `like` instead of `ilike` on line 13-15
# We should open a PR upstream to fix this


# make sure trino cluster is running!
# you may need to run $chmod +x generate_sources.sh to make this script executable

# execute: $ ./scripts/generate_sources.sh

# Get list if distinct schemas in current sub-project
schemas=$(dbt -q ls --project-dir tokens --resource-type model --output json | jq --slurp | jq -r '.[] | "\(.config.schema)"' | sort -u)

# run generate_source for each schema
IFS=$'\n'
for schema_name in $schemas; do
    # echo "Generating source for schema: ${schema_name}"
    dbt -q run-operation generate_source --project-dir tokens  --args "{\"schema_name\": \"dbt_poc_tokens_${schema_name}\", \"generate_columns\": true, \"include_data_types\": true}" --profiles-dir ~/.dbt --profile spellbook-poc-tokens
done