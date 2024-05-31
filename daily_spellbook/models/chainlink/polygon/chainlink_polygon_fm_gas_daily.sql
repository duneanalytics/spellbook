{{
  config(
    
    alias='fm_gas_daily',
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
  fm_gas_fulfilled_daily AS (
    SELECT
      cast(date_trunc('{{truncate_by}}', fulfilled.block_time) as date) as date_start,
      fulfilled.node_address,
      SUM(fulfilled.token_amount) as token_amount,
      SUM(fulfilled.token_amount * fulfilled.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_polygon_fm_fulfilled_transactions') }} fulfilled
    {% if is_incremental() %}
      WHERE
        fulfilled.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  fm_gas_reverted_daily AS (
    SELECT
      cast(date_trunc('{{truncate_by}}', reverted.block_time) as date) as date_start,
      reverted.node_address,
      SUM(reverted.token_amount) as token_amount,
      SUM(reverted.token_amount * reverted.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_polygon_fm_reverted_transactions') }} reverted
    {% if is_incremental() %}
      WHERE
        reverted.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  fm_gas_daily_meta AS (
    SELECT
      COALESCE(
        fulfilled.date_start,
        reverted.date_start
      ) AS "date_start",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(fulfilled.token_amount, 0) as fulfilled_token_amount,
      COALESCE(reverted.token_amount, 0) as reverted_token_amount,
      COALESCE(fulfilled.usd_amount, 0) as fulfilled_usd_amount,
      COALESCE(reverted.usd_amount, 0) as reverted_usd_amount
    FROM
      fm_gas_fulfilled_daily fulfilled
      FULL OUTER JOIN fm_gas_reverted_daily reverted ON
        reverted.date_start = fulfilled.date_start AND
        reverted.node_address = fulfilled.node_address
    ORDER BY
      1, 2
  ),
  fm_gas_daily AS (
    SELECT
      'polygon' as blockchain,
      date_start,
      cast(date_trunc('month', date_start) as date) as date_month,
      fm_gas_daily_meta.node_address as node_address,
      operator_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      fulfilled_token_amount + reverted_token_amount as total_token_amount,
      fulfilled_usd_amount + reverted_usd_amount as total_usd_amount
    FROM fm_gas_daily_meta
    LEFT JOIN {{ ref('chainlink_polygon_ocr_operator_node_meta') }} fm_operator_node_meta ON fm_operator_node_meta.node_address = fm_gas_daily_meta.node_address
  )
SELECT 
  blockchain,
  date_start,
  date_month,
  node_address,
  operator_name,
  fulfilled_token_amount,
  fulfilled_usd_amount,
  reverted_token_amount,
  reverted_usd_amount,
  total_token_amount,
  total_usd_amount    
FROM
  fm_gas_daily
ORDER BY
  "date_start"
