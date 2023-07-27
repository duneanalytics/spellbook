#!/bin/bash

set -euo pipefail

res=$(comm -13 <( dbt ls --resource-type model --select resource_type:model,tag:legacy --output json | jq '.config.schema' | sort | uniq ) <( dbt ls --resource-type model --select tag:dunesql,resource_type:model --output json | jq '.config.schema' | sort | uniq ))
if [ -z "$res" ]; then
  exit 0
else
  echo "Legacy schema missing:"
  echo "$res"
  echo "If you added a new DuneSQL model, please add a dummy spark model with the same name and schema to force schema creation on spark."
  exit 1
fi
