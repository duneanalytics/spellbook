{{
  config(
    tags=['dunesql'],
    alias=alias('fm_reward_evt_transfer_daily'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'admin_address']
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'avalanche_c' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  fm_reward_evt_transfer.admin_address as admin_address,
  MAX(fm_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_avalanche_c_fm_reward_evt_transfer')}} fm_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_avalanche_c_ocr_operator_admin_meta') }} fm_operator_admin_meta ON fm_operator_admin_meta.admin_address = fm_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4
