{{
  config(
    
    alias='vrf_request_fulfilled_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'operator_address']
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'bnb' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  vrf_request_fulfilled.operator_address,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_bnb_vrf_request_fulfilled')}} vrf_request_fulfilled
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4