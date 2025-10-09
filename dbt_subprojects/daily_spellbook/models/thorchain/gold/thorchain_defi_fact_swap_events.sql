{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_swap_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'fact_swap_events_id',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'swap_events', 'fact']
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,
    chain as blockchain,
    from_addr as from_address,
    to_addr as to_address,
    from_asset,
    from_e8,
    to_asset,
    to_e8,
    memo,
    pool as pool_name,
    to_e8_min,
    swap_slip_bp,
    liq_fee_e8,
    liq_fee_in_rune_e8,
    direction as _direction,
    event_id,
    raw_block_timestamp as block_timestamp,
    streaming_count,
    streaming_quantity,
    tx_type as _tx_type,
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_swap_events') }}
  WHERE block_time >= current_date - interval '7' day
)

SELECT
  concat(
    cast(a.event_id as varchar), '-',
    cast(a.tx_id as varchar), '-',
    cast(a.blockchain as varchar), '-',
    cast(a.to_address as varchar), '-',
    cast(a.from_address as varchar), '-',
    cast(a.from_asset as varchar), '-',
    cast(a.from_e8 as varchar), '-',
    cast(a.to_asset as varchar), '-',
    cast(a.to_e8 as varchar), '-',
    cast(a.memo as varchar), '-',
    cast(a.pool_name as varchar), '-',
    cast(a._direction as varchar)
  ) AS fact_swap_events_id,
  COALESCE(b.block_time, a.block_time) as block_time,
  COALESCE(b.height, -1) AS block_height,
  a.tx_id,
  a.blockchain,
  a.from_address,
  a.to_address,
  a.from_asset,
  a.from_e8,
  a.to_asset,
  a.to_e8,
  a.memo,
  a.pool_name,
  a.to_e8_min,
  a.swap_slip_bp,
  a.liq_fee_e8,
  a.liq_fee_in_rune_e8,
  a._direction,
  a.event_id,
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