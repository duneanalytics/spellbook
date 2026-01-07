{{ config(
  schema = 'thorchain',
  alias = 'defi_daily_earnings',
  materialized = 'incremental',
  file_format = 'delta',
  unique_key = ['day', 'fact_daily_earnings_id'],
  incremental_strategy = 'merge',
  partition_by = ['day'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
  tags = ['thorchain', 'defi', 'daily', 'earnings', 'fact'],
  post_hook='{{ expose_spells(\'["thorchain"]\',
                                "project",
                                "thorchain",
                                \'["jeff-dude"]\') }}'
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
  FROM
    {{ ref('thorchain_silver_daily_earnings') }}
  {% if is_incremental() %}
  WHERE {{ incremental_predicate('day') }}
  {% endif -%}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.day']
  ) }} AS fact_daily_earnings_id,
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
  a._inserted_timestamp,
  current_timestamp AS inserted_timestamp,
  current_timestamp AS modified_timestamp
FROM
  base as a
