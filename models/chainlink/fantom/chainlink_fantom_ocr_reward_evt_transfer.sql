{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_reward_evt_transfer'),
    materialized='view',
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
  )
}}

SELECT
  'fantom' as blockchain,
  to as admin_address,
  MAX(operator_name) as operator_name,
  MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
  MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value
FROM
  {{ source('erc20_fantom', 'evt_Transfer') }} reward_evt_transfer
  RIGHT JOIN {{ ref('chainlink_fantom_ocr_reward_transmission_logs') }} ocr_reward_transmission_logs ON ocr_reward_transmission_logs.contract_address = reward_evt_transfer."from"
  LEFT JOIN {{ ref('chainlink_fantom_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = reward_evt_transfer.to
WHERE
  reward_evt_transfer."from" IN (ocr_reward_transmission_logs.contract_address)
GROUP BY
  evt_tx_hash,
  evt_index,
  to
