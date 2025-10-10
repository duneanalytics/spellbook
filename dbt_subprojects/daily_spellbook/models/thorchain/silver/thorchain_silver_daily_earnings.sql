{{ config(
  materialized = 'incremental',
  unique_key = 'day',
  incremental_strategy = 'merge',
  cluster_by = ['day'],
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
  tags = ['thorchain', 'daily', 'earnings']
) }}

WITH max_daily_block AS (
  SELECT
    MAX(height) AS block_id,
    DATE_TRUNC('day', cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp)) AS day
  FROM {{ source('thorchain', 'block_log') }} b
  WHERE cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
  GROUP BY day
),

daily_rune_price AS (
  SELECT
    p.block_id,
    day,
    AVG(p.price) AS rune_usd
  FROM {{ source('prices', 'usd') }} p
  JOIN max_daily_block mdb
    ON p.block_id = mdb.block_id
  WHERE p.blockchain = 'thorchain'
    AND p.symbol = 'RUNE'
    AND p.minute >= current_date - interval '7' day
  GROUP BY day, p.block_id
)

SELECT
  br.day,
  COALESCE(liquidity_fee, 0) AS liquidity_fees,
  COALESCE(liquidity_fee * rune_usd, 0) AS liquidity_fees_usd,
  block_rewards AS block_rewards,
  block_rewards * rune_usd AS block_rewards_usd,
  COALESCE(earnings, 0) AS total_earnings,
  COALESCE(earnings * rune_usd, 0) AS total_earnings_usd,
  bonding_earnings AS earnings_to_nodes,
  bonding_earnings * rune_usd AS earnings_to_nodes_usd,
  COALESCE(liquidity_earnings, 0) AS earnings_to_pools,
  COALESCE(liquidity_earnings * rune_usd, 0) AS earnings_to_pools_usd,
  avg_node_count,
  br._inserted_timestamp
FROM {{ ref('thorchain_silver_block_rewards') }} br
JOIN daily_rune_price drp
  ON br.day = drp.day
WHERE br.day >= current_date - interval '7' day
{% if is_incremental() %}
  AND br.day >= (
    SELECT MAX(day)
    FROM {{ this }}
  )
{% endif %}
