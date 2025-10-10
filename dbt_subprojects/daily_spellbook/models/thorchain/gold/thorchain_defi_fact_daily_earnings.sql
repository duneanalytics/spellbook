{{ config(
  schema = 'thorchain_defi',
  alias = 'fact_daily_earnings',
  materialized = 'incremental',
  file_format = 'delta',
  unique_key = ['fact_daily_earnings_id'],
  incremental_strategy = 'merge',
  tags = ['thorchain', 'defi', 'daily', 'earnings', 'fact']
) }}

WITH base AS (
  SELECT
    day,
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
  WHERE day >= current_date - interval '7' day
  {% if is_incremental() %}
    AND day >= (
      SELECT MAX(day)
      FROM {{ this }}
    )
  {% endif %}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['a.day']) }} AS fact_daily_earnings_id,
  day,
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
  '{{ invocation_id }}' AS _audit_run_id,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM base A
