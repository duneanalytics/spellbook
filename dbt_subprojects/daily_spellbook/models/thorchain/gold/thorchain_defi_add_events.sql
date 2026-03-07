{{ config(
    schema = 'thorchain',
    alias = 'defi_add_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_add_events_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'add_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
  SELECT
    e.block_timestamp,
    e.tx_id,
    e.rune_e8,
    e.blockchain,
    e.asset_e8,
    e.pool_name,
    e.memo,
    e.to_address,
    e.from_address,
    e.asset,
    e.event_id,
    e._TX_TYPE
  FROM
    {{ ref('thorchain_silver_add_events') }} e
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.tx_id','a.blockchain','a.from_address','a.to_address','a.asset','a.memo','a.block_timestamp']
  ) }} AS fact_add_events_id,
  cast(date_trunc('day', b.block_timestamp) AS date) AS day,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.rune_e8,
  A.blockchain,
  A.asset_e8,
  A.pool_name,
  A.memo,
  A.to_address,
  A.from_address,
  A.asset,
  A._TX_TYPE,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base A
JOIN {{ ref('thorchain_core_block') }} as b
  ON A.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE
  {{ incremental_predicate('b.block_timestamp') }}
  OR tx_id IN (
    SELECT
      tx_id
    FROM
      {{ this }}
    WHERE
      dim_block_id = '-1'
  )
{% endif %}
