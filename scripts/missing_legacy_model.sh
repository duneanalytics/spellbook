#!/bin/bash

set -euo pipefail

res=$(comm -13 <( dbt ls --resource-type model --select resource_type:model,tag:legacy --output name | sed 's/_legacy//g' | sort ) <( dbt ls --resource-type model --select tag:dunesql,resource_type:model --output name | sort ))
if [ -z "$res" ]; then
  exit 0
else
  echo "Legacy models missing:"
  echo "$res"
  echo "If you added a new DuneSQL model, please add a dummy spark model with the same name."
  exit 1
fi
