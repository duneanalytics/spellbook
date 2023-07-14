{{
  config(
    tags=['dunesql'],
    alias='ocr_reverted_transactions',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

WITH
  optimism_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'ETH'
      {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
  ),
  ocr_reverted_transactions AS (
    SELECT
      MAX(tx.block_time) as block_time,
      tx."from" as "node_address",
      MAX(
        (cast((l1_gas_used) as double) / 1e18) * l1_gas_price
      ) as token_amount,
      MAX(optimism_usd.usd_amount) as usd_amount
    FROM
      {{ source('optimism', 'transactions') }} tx
      LEFT JOIN optimism_usd ON date_trunc('minute', tx.block_time) = optimism_usd.block_time
    WHERE
      success = false
      {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'optimism' as blockchain,
 block_time,
 node_address,
 token_amount,
 usd_amount
FROM
  ocr_reverted_transactions