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

-- Materialized (was a view) so downstream consumers join the full ~6.3M-row
-- request history without rescanning polygon.logs (50B+ rows); logs are
-- append-only, so an incremental block_time window is exact.

SELECT
  'polygon' as blockchain,
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
  {{ source('polygon', 'logs') }} logs
WHERE
  topic0 = 0x56bd374744a66d531874338def36c906e3a6cf31176eb1e9afd9f1de69725d51 -- RandomnessRequest
{% if is_incremental() %}
  AND {{ incremental_predicate('block_time') }}
{% endif %}
