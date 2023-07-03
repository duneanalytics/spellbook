#!/bin/bash

set -euo pipefail

res=$(comm -12 <( dbt ls --exclude tag:dunesql | sort | grep -v "^source" ) <( dbt ls --select +tag:dunesql+ | sort | grep -v "^source" ))
if [ -z "$res" ]; then
  exit 0
else
  echo "Common models between DuneSQL and Spark:"
  echo "$res"
  exit 1
fi
