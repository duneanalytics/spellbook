{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_v2_random_fulfilled_logs'),
    materialized='view'
  )
}}

SELECT
  'ethereum' as blockchain,
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
  {{ source('ethereum', 'logs') }} logs
WHERE
  topic0 = 0x7dffc5ae5ee4e2e4df1651cf6ad329a73cebdb728f37ea0187b9b17e036756e4 -- RandomWordsFulfilled