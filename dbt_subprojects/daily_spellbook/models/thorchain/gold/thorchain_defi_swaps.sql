{{ config(
    schema = 'thorchain',
    alias = 'defi_swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'swaps', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_swaps",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,
    from_asset,
    to_asset,
    from_amount_usd,
    to_amount_usd,
    event_id,
    block_time,
    block_date,
    block_month,
    from_amount as from_asset_amount,
    to_amount as to_asset_amount,
    liq_fee_rune as liq_fee_in_rune_amount,
    streaming_quantity as streaming,
    streaming_count,
    pool_name as pool
  FROM
    {{ ref('thorchain_silver_swaps') }}
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