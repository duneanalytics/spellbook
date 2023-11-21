{{
  config(
    
    alias='vrf_request_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'node_address']
  )
}}

{% set incremental_interval = '7' %}
{% set truncate_by = 'day' %}

WITH
  vrf_request_daily_meta AS (
    SELECT
      COALESCE(
        cast(date_trunc('{{truncate_by}}', fulfilled.block_time) as date),
        cast(date_trunc('{{truncate_by}}', reverted.block_time) as date)
      ) AS "date_start",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(COUNT(fulfilled.token_amount), 0) as fulfilled_requests,
      COALESCE(COUNT(reverted.token_amount), 0) as reverted_requests,
      COALESCE(COUNT(fulfilled.token_amount), 0) + COALESCE(COUNT(reverted.token_amount), 0) as total_requests
    FROM
      {{ ref('chainlink_ethereum_vrf_fulfilled_transactions') }} fulfilled
      FULL OUTER JOIN {{ ref('chainlink_ethereum_vrf_reverted_transactions') }} reverted ON
        reverted.block_time = fulfilled.block_time AND
        reverted.node_address = fulfilled.node_address
    {% if is_incremental() %}
      WHERE
        fulfilled.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        AND reverted.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  vrf_request_daily AS (
    SELECT
      'ethereum' as blockchain,
      date_start,
      cast(date_trunc('month', date_start) as date) as date_month,
      vrf_request_daily_meta.node_address as node_address,
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM vrf_request_daily_meta
  )
SELECT 
  blockchain,
  date_start,
  date_month,
  node_address,
  fulfilled_requests,
  reverted_requests,
  total_requests
FROM
  vrf_request_daily
ORDER BY
  "date_start"
