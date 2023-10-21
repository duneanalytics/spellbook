{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_fulfilled_transactions'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address']
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
  vrf_fulfilled_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX(
        (cast((gas_used) as double) / 1e18) * effective_gas_price
      ) as token_amount,
      MAX(arbitrum_usd.usd_amount) as usd_amount
    FROM
      {{ source('arbitrum', 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_arbitrum_vrf_v1_random_fulfilled_logs') }} vrf_v1_logs ON vrf_v1_logs.tx_hash = tx.hash
      {% if is_incremental() %}
        AND vrf_v1_logs.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}
      RIGHT JOIN {{ ref('chainlink_arbitrum_vrf_v2_random_fulfilled_logs') }} vrf_v2_logs ON vrf_v2_logs.tx_hash = tx.hash
      {% if is_incremental() %}
        AND vrf_v2_logs.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}
      LEFT JOIN arbitrum_usd ON date_trunc('minute', tx.block_time) = arbitrum_usd.block_time
    {% if is_incremental() %}
      WHERE tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'arbitrum' as blockchain,
  block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  vrf_fulfilled_transactions