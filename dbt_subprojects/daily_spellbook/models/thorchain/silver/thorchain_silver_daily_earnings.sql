{{ config(
  schema = 'thorchain_silver',
  alias = 'daily_earnings',
  materialized = 'incremental',
  file_format = 'delta',
  unique_key = 'block_date',
  incremental_strategy = 'merge',
  partition_by = ['block_month'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
  tags = ['thorchain', 'daily', 'earnings']
) }}

WITH daily_rune_price AS (
  SELECT
    DATE(p.block_time)        AS block_date,
    AVG(p.price)              AS rune_usd
  FROM {{ ref('thorchain_silver_prices') }} p
  WHERE p.symbol = 'RUNE'
    AND p.block_time >= current_date - interval '16' day
  {% if is_incremental() %}
    AND {{ incremental_predicate('p.block_time') }}
  {% endif %}
  GROUP BY DATE(p.block_time)
)

SELECT
  br.block_date,
  date_trunc('month', br.block_date) AS block_month,
  COALESCE(br.liquidity_fee, 0) AS liquidity_fees,
  COALESCE(br.liquidity_fee * drp.rune_usd, 0) AS liquidity_fees_usd,
  br.block_rewards AS block_rewards,
  br.block_rewards * drp.rune_usd AS block_rewards_usd,
  COALESCE(br.earnings, 0) AS total_earnings,
  COALESCE(br.earnings * drp.rune_usd, 0) AS total_earnings_usd,
  br.bonding_earnings AS earnings_to_nodes,
  br.bonding_earnings * drp.rune_usd AS earnings_to_nodes_usd,
  COALESCE(br.liquidity_earnings, 0) AS earnings_to_pools,
  COALESCE(br.liquidity_earnings * drp.rune_usd, 0) AS earnings_to_pools_usd,
  br.avg_node_count,
  br._inserted_timestamp
FROM {{ ref('thorchain_silver_block_rewards') }} br
JOIN daily_rune_price drp
  ON br.block_date = drp.block_date
WHERE br.block_date >= current_date - interval '16' day
{% if is_incremental() %}
  AND {{ incremental_predicate('br.block_date') }}
{% endif %}
