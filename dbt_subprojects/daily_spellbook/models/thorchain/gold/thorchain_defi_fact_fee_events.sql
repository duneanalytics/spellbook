{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_fee_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'fact_fee_events_id',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'fee_events', 'fact']
) }}

WITH base AS (
  SELECT
    tx_hash as tx_id,  -- Convert to FlipsideCrypto naming
    asset,
    pool_deduct,
    asset_e8,
    event_id,
    raw_block_timestamp as block_timestamp,  -- Use raw timestamp for exact JOIN
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_fee_events') }}
  WHERE block_time >= current_date - interval '7' day
)

SELECT
  concat(
    cast(a.event_id as varchar), '-',
    cast(a.asset as varchar), '-', 
    cast(a.asset_e8 as varchar), '-',
    cast(a.pool_deduct as varchar), '-',
    cast(a.block_timestamp as varchar), '-',
    cast(a.tx_id as varchar)
  ) AS fact_fee_events_id,
  COALESCE(b.block_time, a.block_time) as block_time,  -- Use block_time for spellbook compatibility
  COALESCE(b.height, -1) AS block_height,
  a.tx_id,
  a.asset,
  a.pool_deduct,
  a.asset_e8,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp

FROM base a
LEFT JOIN {{ ref('thorchain_core_dim_block') }} b
  ON a.block_timestamp = b.raw_timestamp  -- EXACT match like FlipsideCrypto

{% if is_incremental() %}
WHERE a.block_time >= (
  SELECT MAX(block_time - INTERVAL '1' HOUR)
  FROM {{ this }}
) 
{% endif %}