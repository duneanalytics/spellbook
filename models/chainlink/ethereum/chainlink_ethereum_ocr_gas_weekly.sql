{{ config(
    alias = 'ocr_gas_weekly',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
    )
}}

{% set truncate_by = 'week' %}

WITH
  ocr_gas_aggregate_meta AS (
    SELECT
      COALESCE(
        date_trunc('{{truncate_by}}', fulfilled.block_time),
        date_trunc('{{truncate_by}}', reverted.block_time)
      ) AS "block_date",      
      COALESCE(
        fulfilled.node_address,
        reverted.node_address
      ) AS "node_address",
      COALESCE(SUM(fulfilled.token_value), 0) as fulfilled_token_amount,
      COALESCE(SUM(reverted.token_value), 0) as reverted_token_amount,
      COALESCE(SUM(fulfilled.token_value * fulfilled.usd_value), 0) as fulfilled_usd_amount,
      COALESCE(SUM(reverted.token_value * reverted.usd_value), 0) as reverted_usd_amount
    FROM
      {{ ref('chainlink_ethereum_ocr_fulfilled_transactions') }} fulfilled
      FULL OUTER JOIN {{ ref('chainlink_ethereum_ocr_reverted_transactions') }} reverted ON
        reverted.block_time = fulfilled.block_time AND
        reverted.node_address = fulfilled.node_address
    GROUP BY
      1, 2
    ORDER BY
      1, 2
  ),
  ocr_gas_aggregate AS (
    SELECT
      'ethereum' as blockchain,
      block_date,
      node_address,
      node_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      fulfilled_token_amount + reverted_token_amount as total_token_amount,
      fulfilled_usd_amount + reverted_usd_amount as total_usd_amount
    FROM ocr_gas_aggregate_meta
    LEFT JOIN {{ ref('chainlink_ethereum_ocr_node_meta') }} ON ocr_node_meta.node_address = ocr_gas_aggregate_meta.node_address
  )
SELECT 
  blockchain,
  block_date,
  node_address,
  node_name,
  fulfilled_token_amount,
  fulfilled_usd_amount,
  reverted_token_amount,
  reverted_usd_amount,
  total_token_amount,
  total_usd_amount    
FROM
  ocr_gas_aggregate
ORDER BY
  "block_date"
