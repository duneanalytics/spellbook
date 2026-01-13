{{ config(
    schema = 'thorchain',
    alias = 'defi_withdraw_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_withdraw_events_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'withdraw_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    e.tx_id,
    e.blockchain,
    e.from_address,
    e.to_address,
    e.asset,
    e.asset_e8,
    e.emit_asset_e8,
    e.emit_rune_e8,
    e.memo,
    e.pool_name,
    e.stake_units,
    e.basis_points,
    e.asymmetry,
    e.imp_loss_protection_e8,
    e._emit_asset_in_rune_e8,
    e.block_timestamp,
    _TX_TYPE,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('thorchain_silver_withdraw_events') }} e
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.tx_id', 'a.blockchain', 'a.from_address', 'a.to_address', 'a.asset', 'a.asset_e8', 'a.emit_asset_e8', 'a.emit_rune_e8', 'a.memo', 'a.pool_name', 'a.stake_units', 'a.basis_points', 'a.asymmetry', 'a.imp_loss_protection_e8', 'a._emit_asset_in_rune_e8','a.block_timestamp']
  ) }} AS fact_withdraw_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.blockchain,
  A.from_address,
  A.to_address,
  A.asset,
  A.asset_e8,
  A.emit_asset_e8,
  A.emit_rune_e8,
  A.memo,
  A.pool_name,
  A.stake_units,
  A.basis_points,
  A.asymmetry,
  A.imp_loss_protection_e8,
  A._emit_asset_in_rune_e8,
  A._TX_TYPE,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base A
JOIN {{ ref('thorchain_core_block') }} as b
  ON A.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}
