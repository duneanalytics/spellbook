{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_v1_random_request_logs'),
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
  tx_index
FROM
  {{ source('bnb', 'logs') }} logs
WHERE
  topic0 = 0x56bd374744a66d531874338def36c906e3a6cf31176eb1e9afd9f1de69725d51 -- RandomnessRequest