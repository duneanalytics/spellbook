{{ config(
  materialized = 'incremental',
  meta = { 'database_tags': { 'table': { 'PURPOSE': 'DEX, AMM' } } },
  unique_key = 'fact_rewards_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (
  SELECT
    bond_e8,
    event_id,
    block_timestamp,
    _inserted_timestamp
  FROM
    {{ ref('thorchain_silver_rewards_event_entries') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['a.event_id', 'a.block_timestamp']) }} AS fact_rewards_events_id,
  b.block_timestamp,
  COALESCE(b.dim_block_id, '-1') AS dim_block_id,
  bond_e8,
  A._inserted_timestamp,
  '{{ invocation_id }}' AS _audit_run_id,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base A
  JOIN {{ ref('thorchain_core_dim_block') }} b
  ON A.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE
  {{ incremental_predicate('b.block_timestamp') }}
{% endif %}
