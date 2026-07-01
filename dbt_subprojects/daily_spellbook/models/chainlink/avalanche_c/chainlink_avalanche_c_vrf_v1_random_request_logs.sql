{{
  config(

    alias='vrf_v1_random_request_logs',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key=['tx_hash', 'index']
  )
}}

-- CUR2-2973: materialized (was a view) so downstream consumers join the full
-- request-log history without rescanning avalanche_c.logs (tens of billions of rows)
-- on every run. Logs are append-only, so an incremental block_time window is exact.
-- Replicates the proven #9766 polygon fix.

SELECT
  'avalanche_c' as blockchain,
  block_hash,
  contract_address,
  data,
  topic0,
  topic1,
  topic2,
  topic3,
  tx_hash,
  block_number,
  block_time,
  index,
  tx_index
FROM
  {{ source('avalanche_c', 'logs') }} logs
WHERE
  topic0 = 0x56bd374744a66d531874338def36c906e3a6cf31176eb1e9afd9f1de69725d51 -- RandomnessRequest
{% if is_incremental() %}
  AND {{ incremental_predicate('block_time') }}
{% endif %}
