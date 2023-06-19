{{ config(
    alias = 'ocr_reward_evt_transfer',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "chainlink",
                                \'["linkpool_ryan"]\') }}'
    )
}}

{% set incremental_interval = '1 week' %}

-- TODO: add node_name

SELECT
  'ethereum' as blockchain,
  to as admin_address,
  MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
  MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value,
FROM
  erc20_ethereum.evt_Transfer reward_evt_transfer
  RIGHT JOIN {{ ref('chainlink_ethereum_ocr_reward_transmission_logs') }} ON ocr_reward_transmission_logs.contract_address = reward_evt_transfer."from"
WHERE
  reward_evt_transfer."from" IN (ocr_reward_transmission_logs.contract_address)
  {% if is_incremental() %}
    AND block_time >= date_trunc("day", now() - interval '{{incremental_interval}}')
  {% endif %}      
GROUP BY
  evt_tx_hash,
  evt_index,
  to