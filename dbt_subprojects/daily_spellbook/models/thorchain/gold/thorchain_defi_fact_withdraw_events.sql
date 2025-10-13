{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_withdraw_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'withdraw_events', 'fact']
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,
    chain as blockchain,
    from_addr as from_address,
    to_addr as to_address,
    asset,
    asset_e8,
    emit_asset_e8,
    emit_rune_e8,
    memo,
    pool as pool_name,
    stake_units,
    basis_points,
    asymmetry,
    imp_loss_protection_e8,
    emit_asset_in_rune_e8 as _emit_asset_in_rune_e8,
    raw_block_timestamp as block_timestamp,
    tx_type as _tx_type,
    event_id,  -- Add missing event_id
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_withdraw_events') }}
  WHERE block_time >= current_date - interval '15' day
)

SELECT
  a.block_time,
  date(a.block_time) as block_date,
  date_trunc('month', a.block_time) as block_month,
  -1 AS block_height,
  a.tx_id,
  a.event_id,
  a.blockchain,
  a.from_address,
  a.to_address,
  a.asset,
  a.asset_e8,
  a.emit_asset_e8,
  a.emit_rune_e8,
  a.memo,
  a.pool_name,
  a.stake_units,
  a.basis_points,
  a.asymmetry,
  a.imp_loss_protection_e8,
  a._emit_asset_in_rune_e8,
  a._tx_type,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}