{{
  config(
    
    alias='fm_reward_evt_transfer',
    materialized='view'
  )
}}

SELECT
  'avalanche_c' as blockchain,
  to as admin_address,
  MAX(operator_name) as operator_name,
  MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
  MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value
FROM
  {{ source('erc20_avalanche_c', 'evt_Transfer') }} reward_evt_transfer
  RIGHT JOIN {{ ref('chainlink_avalanche_c_price_feeds_oracle_addresses') }} price_feeds ON price_feeds.aggregator_address = reward_evt_transfer."from"
  LEFT JOIN {{ ref('chainlink_avalanche_c_ocr_operator_admin_meta') }} fm_operator_admin_meta ON fm_operator_admin_meta.admin_address = reward_evt_transfer.to
WHERE
  reward_evt_transfer."from" IN (price_feeds.aggregator_address)
GROUP BY
  evt_tx_hash,
  evt_index,
  to