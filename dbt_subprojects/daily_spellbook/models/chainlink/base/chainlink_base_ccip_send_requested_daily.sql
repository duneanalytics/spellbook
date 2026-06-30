{{
  config(

    alias='ccip_send_requested_daily',
    partition_by=['date_month'],
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key=['date_start', 'token', 'destination_blockchain']
  )
}}

-- The upstream view chainlink_base_ccip_send_requested groups its source logs by tx_hash and
-- exposes evt_block_time = MAX(block_time), so filtering its output with
-- incremental_predicate('evt_block_time') cannot reach the base.logs scan and forces a
-- full-history scan (~100B rows/run) to merge a handful of rows. The view body is inlined here so
-- the incremental time bound applies BELOW the tx_hash aggregation and prunes the Delta logs scan
-- via block_time file skipping. Every log of a transaction shares that transaction's block_time
-- (one tx = one block), so MAX(block_time) = block_time per group and the pre-agg bound selects
-- exactly the same tx_hash groups as the post-agg evt_block_time predicate (kept as authoritative).
WITH combined_logs AS (
  SELECT
    ccip_logs_v1.blockchain,
    ccip_logs_v1.block_time,
    ccip_logs_v1.fee_token_amount / 1e18 AS fee_token_amount,
    token_addresses.token_symbol AS token,
    ccip_logs_v1.fee_token,
    ccip_logs_v1.destination_selector,
    ccip_logs_v1.destination_blockchain,
    ccip_logs_v1.tx_hash
  FROM
    {{ ref('chainlink_base_ccip_send_requested_logs_v1') }} ccip_logs_v1
    LEFT JOIN {{ ref('chainlink_base_ccip_token_meta') }} token_addresses ON token_addresses.token_contract = ccip_logs_v1.fee_token
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('ccip_logs_v1.block_time') }}
  {% endif %}

  UNION ALL

  SELECT
    ccip_logs_v1_2.blockchain,
    ccip_logs_v1_2.block_time,
    ccip_logs_v1_2.fee_token_amount / 1e18 AS fee_token_amount,
    token_addresses.token_symbol AS token,
    ccip_logs_v1_2.fee_token,
    ccip_logs_v1_2.destination_selector,
    ccip_logs_v1_2.destination_blockchain,
    ccip_logs_v1_2.tx_hash
  FROM
    {{ ref('chainlink_base_ccip_send_requested_logs_v1_2') }} ccip_logs_v1_2
    LEFT JOIN {{ ref('chainlink_base_ccip_token_meta') }} token_addresses ON token_addresses.token_contract = ccip_logs_v1_2.fee_token
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('ccip_logs_v1_2.block_time') }}
  {% endif %}
),

ccip_send_requested AS (
  SELECT
    MAX(blockchain) AS blockchain,
    MAX(block_time) AS evt_block_time,
    SUM(fee_token_amount) AS fee_token_amount,
    MAX(token) AS token,
    MAX(fee_token) AS fee_token,
    MAX(destination_selector) AS destination_selector,
    MAX(destination_blockchain) AS destination_blockchain,
    MAX(tx_hash) AS tx_hash
  FROM
    combined_logs
  GROUP BY
    tx_hash
)

SELECT
  'base' as blockchain,
  cast(date_trunc('day', evt_block_time) AS date) AS date_start,
  MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month,
  SUM(ccip_send_requested.fee_token_amount) as fee_amount,
  -- coalesce nullable unique_key columns: destination_blockchain is unmapped (always NULL) and
  -- token is NULL for unmapped fee tokens; a NULL merge key makes the incremental MERGE silently
  -- double-insert (Trino NULL != NULL), so non-null sentinels keep the key dedup-safe.
  coalesce(ccip_send_requested.token, 'unknown') as token,
  coalesce(ccip_send_requested.destination_blockchain, 'unknown') AS destination_blockchain,
  COUNT(ccip_send_requested.destination_blockchain) AS count
FROM
  ccip_send_requested
{% if is_incremental() %}
  WHERE {{ incremental_predicate('evt_block_time') }}
{% endif %}
GROUP BY
  2, 5, 6
ORDER BY
  2, 5, 6
