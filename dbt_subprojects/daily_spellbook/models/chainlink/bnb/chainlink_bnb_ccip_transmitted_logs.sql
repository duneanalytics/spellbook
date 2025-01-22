{{
  config(
    
    alias='ccip_transmitted_logs',
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
  tx_from
FROM
  {{ source('bnb', 'logs') }} logs
WHERE
  topic0 = 0xb04e63db38c49950639fa09d29872f21f5d49d614f3a969d8adf3d4b52e41a62 -- Transmitted