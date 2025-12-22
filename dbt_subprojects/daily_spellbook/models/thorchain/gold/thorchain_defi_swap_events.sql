{{ config(
    schema = 'thorchain',
    alias = 'defi_swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_swap_events_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'swap_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    tx_id,
    blockchain,
    from_address,
    to_address,
    from_asset,
    from_e8,
    to_asset,
    to_e8,
    memo,
    pool_name,
    to_e8_min,
    swap_slip_bp,
    liq_fee_e8,
    liq_fee_in_rune_e8,
    _DIRECTION,
    event_id,
    block_timestamp,
    streaming_count,
    streaming_quantity,
    _TX_TYPE,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('thorchain_silver_swap_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.tx_id','a.blockchain','a.to_address','a.from_address','a.from_asset','a.from_e8','a.to_asset','a.to_e8','a.memo','a.pool_name','a._direction']
  ) }} AS fact_swap_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  from_address,
  to_address,
  from_asset,
  from_e8,
  to_asset,
  to_e8,
  memo,
  pool_name,
  to_e8_min,
  swap_slip_bp,
  liq_fee_e8,
  liq_fee_in_rune_e8,
  _DIRECTION,
  event_id,
  _TX_TYPE,
  A._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
  JOIN {{ ref('thorchain_core_block') }} as b
  ON a.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif -%}
