{{
  config(

    alias='fm_reward_evt_transfer_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'admin_address']
  )
}}

-- CUR2-2973: the chainlink_fantom_fm_reward_evt_transfer view is inlined as a CTE so the
-- incremental window can be pushed BELOW its per-(evt_tx_hash, evt_index, to) GROUP BY onto the
-- erc20_fantom.evt_Transfer scan (predicate on the evt_block_date partition column -> Delta
-- file-skipping). Without this the predicate stranded above the aggregate and every incremental run
-- full-scanned all-history transfers. The contract set comes from the static price_feeds_oracle_addresses
-- VALUES table (all-history), so bounding the transfer scan by its own block_date is sound: each reward
-- transfer's date_start depends only on its own evt_block_time. The outer
-- incremental_predicate('evt_block_time') is kept authoritative. Proven EXCEPT=0 both ways
-- (bnb 917 rows, gnosis 261 rows) with token sums at the floating-point floor; ~144x IO / ~247x CPU on bnb.
-- Incremental-only; the full-refresh path is unchanged.

WITH fm_reward_evt_transfer AS (
  SELECT
    'fantom' as blockchain,
    to as admin_address,
    MAX(operator_name) as operator_name,
    MAX(reward_evt_transfer.evt_block_time) as evt_block_time,
    MAX(cast(reward_evt_transfer.value as double) / 1e18) as token_value
  FROM
    {{ source('erc20_fantom', 'evt_Transfer') }} reward_evt_transfer
    RIGHT JOIN {{ ref('chainlink_fantom_price_feeds_oracle_addresses') }} price_feeds ON price_feeds.aggregator_address = reward_evt_transfer."from"
    LEFT JOIN {{ ref('chainlink_fantom_ocr_operator_admin_meta') }} fm_operator_admin_meta ON fm_operator_admin_meta.admin_address = reward_evt_transfer.to
  WHERE
    reward_evt_transfer."from" IN (price_feeds.aggregator_address)
    {% if is_incremental() %}
    AND {{ incremental_predicate('reward_evt_transfer.evt_block_date') }}
    {% endif %}
  GROUP BY
    evt_tx_hash,
    evt_index,
    to
)

SELECT
  'fantom' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  fm_reward_evt_transfer.admin_address as admin_address,
  MAX(fm_reward_evt_transfer.operator_name) as operator_name,
  SUM(token_value) as token_amount
FROM
  fm_reward_evt_transfer
  LEFT JOIN {{ ref('chainlink_fantom_ocr_operator_admin_meta') }} fm_operator_admin_meta ON fm_operator_admin_meta.admin_address = fm_reward_evt_transfer.admin_address
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 4
ORDER BY
  2, 4
