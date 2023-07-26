{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_reverted_transactions'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['hash', 'index', 'from'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

WITH
  avalanche_c_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'AVAX'
      {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
  ),
  ocr_reverted_transactions AS (
    SELECT
      MAX(tx.block_time) as block_time,
      date_trunc('month', MAX(tx.block_time)) as date_month,
      tx."from" as "node_address",
      MAX((cast((gas_used) as double) / 1e18) * gas_price) as token_amount,
      MAX(avalanche_c_usd.usd_amount) as usd_amount
    FROM
      {{ source('avalanche_c', 'transactions') }} tx
      LEFT JOIN avalanche_c_usd ON date_trunc('minute', tx.block_time) = avalanche_c_usd.block_time
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
 'avalanche_c' as blockchain,
 block_time,
 date_month,
 node_address,
 token_amount,
 usd_amount
FROM
  ocr_reverted_transactions