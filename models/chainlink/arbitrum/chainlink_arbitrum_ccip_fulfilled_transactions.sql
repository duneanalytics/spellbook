{{
  config(
    
    alias='ccip_fulfilled_transactions',
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['tx_hash', 'trace_address', 'node_address']
  )
}}

{% set incremental_interval = '7' %}

WITH
  ccip_fulfilled_transactions AS (
    SELECT
      ccip_send_traces.tx_hash as tx_hash,
      ccip_send_traces.block_time as block_time,
      cast(date_trunc('day', ccip_send_traces.block_time) as date) as date_start,
      ccip_send_traces."from" as "node_address",
      ccip_send_traces.trace_address as trace_address
    FROM
      {{ ref('chainlink_arbitrum_ccip_send_traces') }} ccip_send_traces
      WHERE
        ccip_send_traces.tx_success = true
      {% if is_incremental() %}
        AND ccip_send_traces.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}
        
  )
SELECT
 'arbitrum' as blockchain,
  block_time,
  date_start,
  node_address,
  tx_hash,
  trace_address
FROM
  ccip_fulfilled_transactions