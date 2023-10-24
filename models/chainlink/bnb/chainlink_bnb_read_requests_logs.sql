{{
  config(
    
    alias='read_requests_logs',
    materialized='view'
  )
}}

SELECT
  'bnb' as blockchain,
  block_hash,
  block_number,
  block_time,
  tx_hash,
  traces."from",
  traces."to",
  input,
  traces."output"

FROM
  {{ source('bnb', 'traces') }} traces
WHERE
  input = 0xfeaf968c -- latestRoundData()
OR
  input = 0x50d25bcd -- latestAnswer()