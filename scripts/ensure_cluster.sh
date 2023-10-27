#!/bin/bash

set -euo pipefail

RETRY_COUNT=0
# max wait: 10 minutes
MAX_RETRIES=40
WAIT_TIME=15

until [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]
do
  dbt debug $PROFILE && break
  RETRY_COUNT=$((RETRY_COUNT+1))
  if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
    echo "Max retries reached, failing..."
    exit 1
  fi
  echo "Retrying in $WAIT_TIME seconds... ($RETRY_COUNT/$MAX_RETRIES)"
  sleep $WAIT_TIME
done