{{
  config(
    
    alias='ccip_tokens_locked_logs',
    materialized='view'
  )
}}

SELECT
  'arbitrum' as blockchain,
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
  {{ source('arbitrum', 'logs') }} logs
WHERE
  topic0 = 0x9f1ec8c880f76798e7b793325d625e9b60e4082a553c98f42b6cda368dd60008 -- Locked