{{
  config(
    tags=['dunesql'],
    alias='ocr_reverted_transactions',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

WITH
  arbitrum_usd AS (
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
        (cast((gas_used) as double) / 1e18) * effective_gas_price
      ) as token_amount,
      MAX(arbitrum_usd.usd_amount) as usd_amount
    FROM
      {{ source('arbitrum', 'transactions') }} tx
      LEFT JOIN arbitrum_usd ON date_trunc('minute', tx.block_time) = arbitrum_usd.block_time
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
 'arbitrum' as blockchain,
 block_time,
 node_address,
 token_amount,
 usd_amount
FROM
  ocr_reverted_transactions