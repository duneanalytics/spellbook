{{
  config(
    
    alias='ccip_reverted_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'caller_address']
  )
}}

{% set incremental_interval = '7' %}

WITH
  base_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'MATIC'
      {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
  ),
  ccip_reverted_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "caller_address",
      MAX(
        (cast((gas_used) as double) / 1e18) * gas_price
      ) as token_amount,
      MAX(base_usd.usd_amount) as usd_amount
    FROM
      {{ source('polygon', 'transactions') }} tx
      LEFT JOIN base_usd ON date_trunc('minute', tx.block_time) = base_usd.block_time
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
 'polygon' as blockchain,
  block_time,
  date_month,
  caller_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ccip_reverted_transactions