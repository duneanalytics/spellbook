{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_gas_daily'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'node_address'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}
{% set truncate_by = 'day' %}

WITH
  ocr_gas_daily_meta AS (
    SELECT
      COALESCE(
        cast(date_trunc('{{truncate_by}}', fulfilled.block_time) as date),
        cast(date_trunc('{{truncate_by}}', reverted.block_time) as date)
      ) AS "date_start",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(SUM(fulfilled.token_amount), 0) as fulfilled_token_amount,
      COALESCE(SUM(reverted.token_amount), 0) as reverted_token_amount,
      COALESCE(SUM(fulfilled.token_amount * fulfilled.usd_amount), 0) as fulfilled_usd_amount,
      COALESCE(SUM(reverted.token_amount * reverted.usd_amount), 0) as reverted_usd_amount
    FROM
      {{ ref('chainlink_polygon_ocr_fulfilled_transactions') }} fulfilled
      FULL OUTER JOIN {{ ref('chainlink_polygon_ocr_reverted_transactions') }} reverted ON
        reverted.block_time = fulfilled.block_time AND
        reverted.node_address = fulfilled.node_address
    {% if is_incremental() %}
      WHERE
        fulfilled.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        OR reverted.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  ocr_gas_daily AS (
    SELECT
      'polygon' as blockchain,
      date_start,
      cast(date_trunc('month', date_start) as date) as date_month,
      ocr_gas_daily_meta.node_address as node_address,
      operator_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      fulfilled_token_amount + reverted_token_amount as total_token_amount,
      fulfilled_usd_amount + reverted_usd_amount as total_usd_amount
    FROM ocr_gas_daily_meta
    LEFT JOIN {{ ref('chainlink_polygon_ocr_operator_node_meta') }} ocr_operator_node_meta ON ocr_operator_node_meta.node_address = ocr_gas_daily_meta.node_address
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
  ocr_gas_daily
ORDER BY
  "date_start"
