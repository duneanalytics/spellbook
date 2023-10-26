{{
  config(
    
    alias='vrf_fulfilled_transactions',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'tx_index', 'node_address']
  )
}}

WITH
  ethereum_usd AS (
    SELECT
      minute as block_time,
      price as usd_amount
    FROM
      {{ source('prices', 'usd') }} price
    WHERE
      symbol = 'ETH'
      {% if is_incremental() %}
        AND
           {{ incremental_predicate('minute') }}
      {% endif %}      
  ),
  vrf_fulfilled_transactions AS (
    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as tx_block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX(
        (cast((gas_used) as double) / 1e18) * gas_price
      ) as token_amount,
      MAX(ethereum_usd.usd_amount) as usd_amount
    FROM
      {{ source('ethereum', 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_ethereum_vrf_v1_random_fulfilled_logs') }} vrf_v1_logs ON vrf_v1_logs.tx_hash = tx.hash
      {% if is_incremental() %}
        AND 
          {{ incremental_predicate('tx.block_time') }}
      {% endif %}
      LEFT JOIN ethereum_usd ON date_trunc('minute', tx.block_time) = ethereum_usd.block_time
    {% if is_incremental() %}
      WHERE
        {{ incremental_predicate('tx.block_time') }}
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"

    UNION

    SELECT
      tx.hash as tx_hash,
      tx.index as tx_index,
      MAX(tx.block_time) as tx_block_time,
      cast(date_trunc('month', MAX(tx.block_time)) as date) as date_month,
      tx."from" as "node_address",
      MAX(
        (cast((gas_used) as double) / 1e18) * gas_price
      ) as token_amount,
      MAX(ethereum_usd.usd_amount) as usd_amount
    FROM
      {{ source('ethereum', 'transactions') }} tx
      RIGHT JOIN {{ ref('chainlink_ethereum_vrf_v2_random_fulfilled_logs') }} vrf_v2_logs ON vrf_v2_logs.tx_hash = tx.hash
      {% if is_incremental() %}
        AND
          {{ incremental_predicate('tx.block_time') }}
      {% endif %}
      LEFT JOIN ethereum_usd ON date_trunc('minute', tx.block_time) = ethereum_usd.block_time
    {% if is_incremental() %}
      WHERE
        {{ incremental_predicate('tx.block_time') }}
    {% endif %}      
    GROUP BY
      tx.hash,
      tx.index,
      tx."from"
  )
SELECT
 'ethereum' as blockchain,
  tx_block_time as block_time,
  date_month,
  node_address,
  token_amount,
  usd_amount,
  tx_hash,
  tx_index
FROM
  vrf_fulfilled_transactions