{{
  config(
    
    alias='ccip_send_requested_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'token', 'destination_blockchain']
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'base' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  SUM(ccip_send_requested.fee_token_amount) as fee_amount,
  ccip_send_requested.token as token,
  ccip_send_requested.destination_blockchain AS destination_blockchain,
  COUNT(ccip_send_requested.destination_blockchain) AS count
FROM
  {{ref('chainlink_base_ccip_send_requested')}} ccip_send_requested
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 5, 6
ORDER BY
  2, 5, 6
