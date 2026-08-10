{{
  config(

    alias='ocr_reward_evt_transfer_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.date_start')],
    unique_key=['date_start', 'admin_address']
    , post_hook='{{ hide_spells() }}'
  )
}}

-- Performance (CUR2-2973): the upstream ocr_reward_evt_transfer view joined the full
-- erc20 evt_Transfer history against an unbounded NewTransmission logs view, so each
-- incremental run re-scanned tens of billions of rows to merge ~1 row/day. The view is
-- inlined below: the reward-distributor contract set is read from the now-incremental
-- ocr_reward_transmission_logs table (all-history, so no lagged-transfer drop), and the
-- evt_Transfer scan is bounded by its evt_block_date partition on incremental runs. Each
-- output day depends only on its own transfers' block_time, so the date bound is sound.

WITH ocr_reward_evt_transfer AS (
  SELECT
    'optimism' as blockchain,
    reward_evt_transfer.to as admin_address,
    MAX(ocr_operator_admin_meta.operator_name) as operator_name,
    MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
    MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value
  FROM
    {{ source('erc20_optimism', 'evt_Transfer') }} reward_evt_transfer
    LEFT JOIN {{ ref('chainlink_optimism_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = reward_evt_transfer.to
  WHERE
    reward_evt_transfer."from" IN (
      SELECT contract_address
      FROM {{ ref('chainlink_optimism_ocr_reward_transmission_logs') }}
    )
{% if is_incremental() %}
    AND {{ incremental_predicate('reward_evt_transfer.evt_block_date') }}
{% endif %}
  GROUP BY
    reward_evt_transfer.evt_tx_hash,
    reward_evt_transfer.evt_index,
    reward_evt_transfer.to
)

SELECT
  'optimism' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  ocr_reward_evt_transfer.admin_address as admin_address,
  MAX(ocr_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  ocr_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_optimism_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON ocr_operator_admin_meta.admin_address = ocr_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 4
ORDER BY
  2, 4
