{{ config(
    schema = 'thorchain',
    alias = 'defi_liquidity_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_liquidity_actions_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'liquidity', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    block_id,
    tx_id,
    lp_action,
    pool_name,
    from_address,
    to_address,
    rune_amount,
    rune_amount_usd,
    asset_amount,
    asset_amount_usd,
    stake_units,
    asset_tx_id,
    asset_address,
    asset_blockchain,
    il_protection,
    il_protection_usd,
    unstake_asymmetry,
    unstake_basis_points,
    _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('thorchain_silver_liquidity_actions') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_liquidity_actions_id,
  cast(date_trunc('day', b.block_timestamp) AS date) AS day,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  lp_action,
  pool_name,
  from_address,
  to_address,
  rune_amount,
  rune_amount_usd,
  asset_amount,
  asset_amount_usd,
  stake_units,
  asset_tx_id,
  asset_address,
  asset_blockchain,
  il_protection,
  il_protection_usd,
  unstake_asymmetry,
  unstake_basis_points,
  A._INSERTED_TIMESTAMP,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
  JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_id = b.block_id
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}
