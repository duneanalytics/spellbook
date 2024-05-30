{{
  config(
    
    alias='ccip_transmitted_fulfilled',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address']
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
      symbol = 'ETH'
      {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}      
  ),
  ccip_fulfilled_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX((cast((l1_gas_used) as double) / 1e18) * l1_gas_price) as token_amount,
      MAX(base_usd.usd_amount) as usd_amount
    FROM
      {{ source('base', 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_base_ccip_transmitted_logs') }} ccip_tx ON ccip_tx.tx_hash = tx.hash
      {% if is_incremental() %}
        AND ccip_tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}
      LEFT JOIN base_usd ON date_trunc('minute', tx.block_time) = base_usd.block_time
    {% if is_incremental() %}
      WHERE tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'base' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  ccip_fulfilled_transactions