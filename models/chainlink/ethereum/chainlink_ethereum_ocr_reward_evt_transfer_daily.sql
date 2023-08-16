{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_reward_evt_transfer_daily'),
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'admin_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'ethereum' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  ocr_reward_evt_transfer.admin_address as admin_address,
  MAX(ocr_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_ethereum_ocr_reward_evt_transfer')}} ocr_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_ethereum_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 4
ORDER BY
  2, 4



