{{ config(
    schema = 'thorchain',
    alias = 'defi_add_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_id', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'add_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_add_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
  SELECT
    raw_block_timestamp as block_timestamp,
    tx_hash as tx_id,
    rune_e8,
    chain as blockchain,
    asset_e8,
    pool as pool_name,
    memo,
    to_addr as to_address,
    from_addr as from_address,
    asset,
    event_id,
    tx_type as _tx_type,
    block_time  -- Keep for incremental predicate
  FROM
    {{ ref('thorchain_silver_add_events') }}
  WHERE block_time >= current_date - interval '16' day
)

SELECT
  a.block_time,
  date(a.block_time) as block_date,
  date_trunc('month', a.block_time) as block_month,
  -1 AS block_height,
  a.tx_id,
  a.event_id,
  a.rune_e8,
  a.blockchain,
  a.asset_e8,
  a.pool_name,
  a.memo,
  a.to_address,
  a.from_address,
  a.asset,
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
