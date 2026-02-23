{{ config(
    schema = 'thorchain',
    alias = 'defi_streaming_swap_details_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'streaming_swaps', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    tx_id,
    streaming_interval as "interval",
    quantity,
    stream_count as "count",
    last_height,
    deposit_asset,
    deposit_e8,
    in_asset,
    in_e8,
    out_asset,
    out_e8,
    failed_swaps,
    failed_swap_reasons,
    event_id,
    raw_block_timestamp as block_timestamp,
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_streaming_swap_details_events') }}
)

SELECT
  a.block_time,
  date(a.block_time) as block_date,
  date_trunc('month', a.block_time) as block_month,
  -1 AS block_height,
  a.tx_id,
  a.event_id,
  a."interval",
  a.quantity,
  a."count",
  a.last_height,
  a.deposit_asset,
  a.deposit_e8,
  a.in_asset,
  a.in_e8,
  a.out_asset,
  a.out_e8,
  a.failed_swaps,
  a.failed_swap_reasons,
  -- Success rate metrics
  CASE 
    WHEN a.failed_swaps IS NOT NULL AND cardinality(a.failed_swaps) > 0 THEN false
    ELSE true
  END as is_successful,
  COALESCE(cardinality(a.failed_swaps), 0) as failed_swap_count,
  -- Trade direction relative to RUNE
  CASE
    WHEN a.in_asset LIKE 'THOR.RUNE%' THEN 'sell_rune'
    WHEN a.out_asset LIKE 'THOR.RUNE%' THEN 'buy_rune'
    ELSE 'asset_to_asset'
  END as trade_direction,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}
