{{
  config(
    
    alias='ccip_transmitted_reverted',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address']
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
  ccip_reverted_transactions AS (
    SELECT
      tx.tx_hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx.tx_from as "node_address",
      MAX(((gas_price * gas_used) + l1_fee) / 1e18) as token_amount,
      MAX(optimism_usd.usd_amount) as usd_amount
    FROM
      {{ ref('chainlink_optimism_ccip_transmitted_logs') }} tx
      LEFT JOIN {{ source('optimism', 'transactions') }} tx2 ON tx2.hash = tx.tx_hash
      {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}
      LEFT JOIN optimism_usd ON date_trunc('minute', tx.block_time) = optimism_usd.block_time
    WHERE
      tx2.success = false
      {% if is_incremental() %}
        AND tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
    GROUP BY
      tx.tx_hash,
      tx.index,
      tx.tx_from
  )
SELECT
 'optimism' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ccip_reverted_transactions