{{
  config(
    
    alias='read_requests_requester',
    materialized='view'
  )
}}


SELECT
  'optimism' as blockchain,
  log_meta.block_time as date_start,
  oracle_addresses.proxy_address as feed_address,
  oracle_addresses.feed_name,
  COALESCE(requester_addresses.requester_address, log_meta."from") as requester_address,
  requester_addresses.requester_name
FROM {{ ref('chainlink_optimism_read_requests_logs') }} log_meta
LEFT JOIN {{ ref('chainlink_optimism_read_requests_requester_meta') }} requester_addresses ON requester_addresses.requester_address = log_meta."from"
LEFT JOIN {{ ref('chainlink_optimism_price_feeds_oracle_addresses') }} oracle_addresses ON oracle_addresses.proxy_address = log_meta."to"
