{{
  config(
    
    alias='ccip_reverted_transactions',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'trace_address', 'node_address']
  )
}}

{% set incremental_interval = '7' %}

WITH
  ccip_reverted_transactions AS (
    SELECT
      tx.tx_hash as tx_hash,
      tx.block_time as block_time,
      cast(date_trunc('day', tx.block_time) as date) as date_start,
      tx."from" as "node_address",
      tx.trace_address as trace_address
    FROM
      {{ ref('chainlink_avalanche_c_ccip_send_traces') }} tx
      WHERE
        tx.tx_success = false 
        {% if is_incremental() %}
          AND tx.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        {% endif %}    
  )
SELECT
 'avalanche_c' as blockchain,
  block_time,
  date_start,
  node_address,
  tx_hash,
  trace_address
FROM
  ccip_reverted_transactions