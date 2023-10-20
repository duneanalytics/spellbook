{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_feeds'),
    materialized='view',
  )
}}

SELECT
  'avalanche_c' as blockchain,
  block_time as date_start,
  log_meta."to" as feed_address,
  feed_name
FROM {{ ref('chainlink_avalanche_c_read_requests_logs') }} log_meta
RIGHT JOIN {{ ref('chainlink_avalanche_c_price_feeds_oracle_addresses') }} oracle_addresses ON oracle_addresses.proxy_address = log_meta."to"
