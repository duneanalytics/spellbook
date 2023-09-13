{{
  config(
    tags=['dunesql'],
    alias=alias('fm_gas_submission_logs'),
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
  tx_index
FROM
  {{ source('ethereum', 'logs') }} logs
WHERE
  topic0 = 0x92e98423f8adac6e64d0608e519fd1cefb861498385c6dee70d58fc926ddc68c -- SubmissionReceived