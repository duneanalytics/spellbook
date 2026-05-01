{{ config(
    schema = 'thorchain',
    alias = 'defi_swaps',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_swaps_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'swaps', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    block_timestamp,
    block_id,
    tx_id,
    blockchain,
    pool_name,
    from_address,
    native_to_address,
    to_pool_address,
    affiliate_address,
    affiliate_fee_basis_points,
    affiliate_addresses_array,
    affiliate_fee_basis_points_array,
    from_asset,
    to_asset,
    from_amount,
    to_amount,
    min_to_amount,
    from_amount_usd,
    to_amount_usd,
    rune_usd,
    asset_usd,
    to_amount_min_usd,
    swap_slip_bp,
    liq_fee_rune,
    liq_fee_rune_usd,
    liq_fee_asset,
    liq_fee_asset_usd,
    streaming_count,
    streaming_quantity,
    _TX_TYPE,
    _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('thorchain_silver_swaps') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_swaps_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  pool_name,
  from_address,
  native_to_address,
  to_pool_address,
  affiliate_address,
  affiliate_fee_basis_points,
  affiliate_addresses_array,
  affiliate_fee_basis_points_array,
  from_asset,
  to_asset,
  from_amount,
  to_amount,
  min_to_amount,
  from_amount_usd,
  to_amount_usd,
  rune_usd,
  asset_usd,
  to_amount_min_usd,
  swap_slip_bp,
  liq_fee_rune,
  liq_fee_rune_usd,
  liq_fee_asset,
  liq_fee_asset_usd,
  streaming_count,
  streaming_quantity,
  _TX_TYPE,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_id = b.block_id
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}
