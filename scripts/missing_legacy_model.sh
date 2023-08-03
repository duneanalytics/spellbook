#!/bin/bash

set -euo pipefail

res=$(comm -13 <( dbt ls --resource-type model --select resource_type:model,tag:legacy --output json | jq '.config.schema' | sort | uniq ) <( dbt ls --resource-type model --select tag:dunesql,resource_type:model --output json | jq '.config.schema' | sort | uniq ))
if [ -z "$res" ]; then
  exit 0
else
  echo "Legacy schema missing:"
  echo "$res"
  echo "If you added a new DuneSQL model, please add a dummy _legacy model with the same name and schema to force schema creation on spark."
  echo "Example:
cat > new_model_legacy.sql << EOL
{{ config( 
  schema = 'schema_name',
  alias = alias('table_name', legacy_model=True),
  tags = ['legacy']
  )
}}
  
  
-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1
EOL"
  exit 1
fi
