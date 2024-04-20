{{
  config(
    schema='chainlink_optimism',
    alias='ccip_tokens_transferred_logs',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['blockchain', 'tx_hash', 'index']
  )
}}

{% set project_start_date = '2023-07-06' %}

SELECT
  'optimism' as blockchain,
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
  tx_index,
  tx_from,
  bytearray_to_uint256(bytearray_substring(data, 1, 32)) as total_tokens
FROM
  {{ source('optimism', 'logs') }} logs
WHERE
  topic0 = 0x9f1ec8c880f76798e7b793325d625e9b60e4082a553c98f42b6cda368dd60008 -- Locked
  AND block_time >= TIMESTAMP '{{project_start_date}}'
  {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
  {% endif %}
UNION ALL
SELECT
  'optimism' as blockchain,
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
  tx_index,
  tx_from,
  bytearray_to_uint256(bytearray_substring(data, 1, 32)) as total_tokens
FROM
  {{ source('optimism', 'logs') }} logs
WHERE
  topic0 = 0x696de425f79f4a40bc6d2122ca50507f0efbeabbff86a84871b7196ab8ea8df7 -- Burned(address,uint256)
  AND block_time >= TIMESTAMP '{{project_start_date}}'
  {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
  {% endif %}