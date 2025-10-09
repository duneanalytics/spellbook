{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_withdraw_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'fact_withdraw_events_id',
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
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_withdraw_events') }}
  WHERE block_time >= current_date - interval '10' day
)

SELECT
  concat(
    cast(a.tx_id as varchar), '-',
    cast(a.blockchain as varchar), '-',
    cast(a.from_address as varchar), '-',
    cast(a.to_address as varchar), '-',
    cast(a.asset as varchar), '-',
    cast(a.asset_e8 as varchar), '-',
    cast(a.emit_asset_e8 as varchar), '-',
    cast(a.emit_rune_e8 as varchar), '-',
    cast(a.memo as varchar), '-',
    cast(a.pool_name as varchar), '-',
    cast(a.stake_units as varchar), '-',
    cast(a.basis_points as varchar), '-',
    cast(a.asymmetry as varchar), '-',
    cast(a.imp_loss_protection_e8 as varchar), '-',
    cast(a._emit_asset_in_rune_e8 as varchar), '-',
    cast(a.block_timestamp as varchar)
  ) AS fact_withdraw_events_id,
  COALESCE(b.block_time, a.block_time) as block_time,
  COALESCE(b.block_date, date(a.block_time)) as block_date,
  COALESCE(b.block_month, date_trunc('month', a.block_time)) as block_month,
  COALESCE(b.height, -1) AS block_height,
  a.tx_id,
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
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
  ON a.block_timestamp = b.raw_timestamp

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}