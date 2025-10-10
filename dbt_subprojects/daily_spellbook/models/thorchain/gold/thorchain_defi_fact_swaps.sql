{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'swaps', 'fact']
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,
    from_asset,
    to_asset,
    from_amount_usd,
    to_amount_usd,
    involves_rune,
    source_table,
    event_id,
    block_time,
    block_date,
    block_month,
    from_asset_amount,
    to_asset_amount,
    liq_fee_in_rune_amount,
    streaming,
    streaming_count,
    pool
  FROM
    {{ ref('thorchain_silver_swaps') }}
  WHERE block_time >= current_date - interval '7' day
)

SELECT
  a.block_time,
  a.block_date,
  a.block_month,
  a.tx_id,
  a.from_asset as token_sold_symbol,
  a.from_asset_amount as token_sold_amount,
  a.to_asset as token_bought_symbol,
  a.to_asset_amount as token_bought_amount,
  a.from_amount_usd as amount_usd_sold,
  a.to_amount_usd as amount_usd_bought,
  a.pool,
  a.streaming,
  a.streaming_count,
  a.liq_fee_in_rune_amount,
  a.involves_rune,
  a.source_table,
  a.event_id,
  'thorchain' as project,
  '1' as version,
  'AMM' as category,
  GREATEST(a.from_amount_usd, a.to_amount_usd) as volume_usd,
  CASE
    WHEN a.from_asset LIKE 'THOR.RUNE%' THEN 'sell_rune'
    WHEN a.to_asset LIKE 'THOR.RUNE%' THEN 'buy_rune'
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