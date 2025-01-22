{{
  config(
    
    alias='read_requests_feeds',
    materialized='view',
  )
}}

SELECT
  'ethereum' as blockchain,
  block_time as date_start,
  log_meta."to" as feed_address,
  feed_name
FROM {{ ref('chainlink_ethereum_read_requests_logs') }} log_meta
RIGHT JOIN {{ ref('chainlink_ethereum_price_feeds_oracle_addresses') }} oracle_addresses ON oracle_addresses.proxy_address = log_meta."to"
