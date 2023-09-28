{{
  config(
    tags=['dunesql'],
    alias=alias('read_requests_feeds_daily'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'feed_address']
  )
}}

{% set incremental_interval = '7' %}
{% set truncate_by = 'day' %}

WITH 
 log_meta as (
    SELECT
      CAST(date_trunc('{{truncate_by}}', block_time) as date) as date_start,
      "to" as feed_address,
      COUNT(*) as total_requests
    FROM {{ ref('chainlink_ethereum_read_requests_logs') }}
    GROUP BY
        1, 2
    ORDER BY
        1, 2
 )   

SELECT
  'ethereum' as blockchain,
  CAST(date_trunc('month', date_start) as date) as date_month,
  date_start,
  proxy_address as feed_address,
  feed_name,
  total_requests
FROM log_meta
RIGHT JOIN {{ ref('chainlink_ethereum_price_feeds_oracle_addresses') }} oracle_addresses ON oracle_addresses.proxy_address = log_meta.feed_address
{% if is_incremental() %}
  WHERE
   block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}
ORDER BY
   date_start
