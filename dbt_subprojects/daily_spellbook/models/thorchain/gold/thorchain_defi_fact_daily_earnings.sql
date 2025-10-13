{{ config(
  schema = 'thorchain_defi',
  alias = 'fact_daily_earnings',
  materialized = 'incremental',
  file_format = 'delta',
  unique_key = ['fact_daily_earnings_id'],
  incremental_strategy = 'merge',
  partition_by = ['block_month'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
  tags = ['thorchain', 'defi', 'daily', 'earnings', 'fact']
) }}

WITH base AS (
  SELECT
    block_date,
    block_month,
    liquidity_fees,
    liquidity_fees_usd,
    block_rewards,
    block_rewards_usd,
    total_earnings,
    total_earnings_usd,
    earnings_to_nodes,
    earnings_to_nodes_usd,
    earnings_to_pools,
    earnings_to_pools_usd,
    avg_node_count,
    _inserted_timestamp
  FROM {{ ref('thorchain_silver_daily_earnings') }}
  WHERE block_date >= current_date - interval '14' day
  {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
  {% endif %}
)

SELECT
  to_hex(sha256(to_utf8(cast(a.block_date as varchar)))) AS fact_daily_earnings_id,
  block_date,
  block_month,
  liquidity_fees,
  liquidity_fees_usd,
  block_rewards,
  block_rewards_usd,
  total_earnings,
  total_earnings_usd,
  earnings_to_nodes,
  earnings_to_nodes_usd,
  earnings_to_pools,
  earnings_to_pools_usd,
  avg_node_count,
  A._inserted_timestamp,
  cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM base A
