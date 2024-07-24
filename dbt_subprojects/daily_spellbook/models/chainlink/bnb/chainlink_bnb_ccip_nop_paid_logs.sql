{{
  config(
    
    alias='ccip_nop_paid_logs',
    materialized='view'
  )
}}

SELECT
  'bnb' as blockchain,
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
  bytearray_substring(topic1, 13) as nop_address,
  bytearray_to_uint256(bytearray_substring(data, 1, 32)) as total_tokens
FROM
  {{ source('bnb', 'logs') }} logs
WHERE
  topic0 = 0x55fdec2aab60a41fa5abb106670eb1006f5aeaee1ba7afea2bc89b5b3ec7678f -- NopPaid