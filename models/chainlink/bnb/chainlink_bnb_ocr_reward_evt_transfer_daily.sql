{{
  config(
    tags=['dunesql'],
    alias='ocr_reward_evt_transfer_daily',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

{% set incremental_interval = '7' %}

SELECT
  'bnb' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  ocr_reward_evt_transfer.admin_address as admin_address,
  MAX(ocr_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_bnb_ocr_reward_evt_transfer')}} ocr_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_bnb_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 3
ORDER BY
  2, 3



