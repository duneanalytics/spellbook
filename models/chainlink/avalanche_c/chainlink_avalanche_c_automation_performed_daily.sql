{{
  config(
    
    alias='automation_performed_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'keeper_address']
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'avalanche_c' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  automation_performed.keeper_address as keeper_address,
  MAX(automation_performed.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_avalanche_c_automation_performed')}} automation_performed
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4
