{{ config(
    alias = 'ocr_reward_evt_transfer_daily',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
    )
}}

{% set incremental_interval = '7' %}

SELECT
  'ethereum' as blockchain,
  date_trunc('day', evt_block_time) as date_start,
  admin_address,
  operator_name,
  SUM(token_value) as token_amount
FROM
  {{ref('chainlink_ethereum_ocr_reward_evt_transfer')}}
  LEFT JOIN {{ ref('chainlink_ethereum_ocr_operator_admin_meta') }} ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE evt_block_time >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
{% endif %}      
GROUP BY
  2, 3
ORDER BY
  2, 3



