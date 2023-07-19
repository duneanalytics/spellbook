{{
  config(
    tags=['dunesql'],
    alias='ocr_fulfilled_transactions',
    materialized='view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

WITH
  fantom_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'FTM'
      {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
  ),
  ocr_fulfilled_transactions AS (
    SELECT
      MAX(tx.block_time) as block_time,
      tx."from" as "node_address",
      MAX((cast((gas_used) as double) / 1e18) * gas_price) as token_amount,
      MAX(fantom_usd.usd_amount) as usd_amount
    FROM
      {{ source('fantom', 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_fantom_ocr_gas_transmission_logs') }} ocr_gas_transmission_logs ON ocr_gas_transmission_logs.tx_hash = tx.hash
      LEFT JOIN fantom_usd ON date_trunc('minute', tx.block_time) = fantom_usd.block_time
    {% if is_incremental() %}
      WHERE tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'fantom' as blockchain,
 block_time,
 node_address,
 token_amount,
 usd_amount
FROM
  ocr_fulfilled_transactions