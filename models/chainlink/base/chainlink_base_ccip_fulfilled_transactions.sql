{{
  config(
    
    alias='ccip_fulfilled_transactions',
    partition_by=['date_start'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge'
  )
}}

{% set incremental_interval = '7' %}

WITH
  ccip_fulfilled_transactions AS (
    SELECT
      ccip_send_traces.tx_hash as tx_hash,
      ccip_send_traces.block_time as block_time,
      cast(date_trunc('day', ccip_send_traces.block_time) as date) as date_start,
      ccip_send_traces."from" as "node_address"
    FROM
      {{ ref('chainlink_base_ccip_send_traces') }} ccip_send_traces
      WHERE
        ccip_send_traces.tx_success = true
      {% if is_incremental() %}
        AND ccip_send_traces.block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
      {% endif %}  
  )
SELECT
 'base' as blockchain,
  block_time,
  date_start,
  node_address,
  tx_hash
FROM
  ccip_fulfilled_transactions