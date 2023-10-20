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

SELECT
  'arbitrum' as blockchain,
  MAX(CAST(date_trunc('month', date_start) as date)) as date_month,
  CAST(date_trunc('day', date_start) AS date) AS date_start,
  log_meta.feed_address,
  MAX(feed_name) as feed_name,
  COALESCE(COUNT(log_meta.feed_address), 0) as total_requests
FROM {{ ref('chainlink_arbitrum_read_requests_feeds') }} log_meta
{% if is_incremental() %}
  WHERE
   {{ incremental_predicate('date_start') }}
{% endif %}
GROUP BY
  3, 4
ORDER BY
  3, 4

