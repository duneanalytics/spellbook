{{ config(
  schema = 'thorchain_silver',
  alias = 'daily_earnings',
  materialized = 'incremental',
  incremental_strategy = 'merge',
  file_format = 'delta',
  partition_by = ['day'],
  unique_key = 'day',
  incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
  tags = ['thorchain', 'daily', 'earnings']
) }}

WITH max_daily_block AS (
    SELECT
        MAX(block_id) AS block_id,
        DATE_TRUNC('day', block_timestamp) AS day
    FROM
      {{ ref('thorchain_silver_prices') }}
    {% if is_incremental() -%}
    WHERE
      {{ incremental_predicate('block_timestamp') }}
    {% endif -%}
  GROUP BY
    date_trunc('day', block_timestamp)
)
, daily_rune_price AS (
    SELECT
      p.block_id,
      mdb.day,
      AVG(p.rune_usd) AS rune_usd
    FROM
      {{ ref('thorchain_silver_prices') }} as p
    JOIN max_daily_block as mdb
      ON p.block_id = mdb.block_id
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('p.block_timestamp') }}
    {% endif -%}
  GROUP BY
    p.block_id,
    mdb.day
)
SELECT
  br.day,
  COALESCE(
    br.liquidity_fee,
    0
  ) AS liquidity_fees,
  COALESCE(
    br.liquidity_fee * drp.rune_usd,
    0
  ) AS liquidity_fees_usd,
  br.block_rewards AS block_rewards,
  br.block_rewards * drp.rune_usd AS block_rewards_usd,
  COALESCE(
    br.earnings,
    0
  ) AS total_earnings,
  COALESCE(
    br.earnings * drp.rune_usd,
    0
  ) AS total_earnings_usd,
  br.bonding_earnings AS earnings_to_nodes,
  br.bonding_earnings * drp.rune_usd AS earnings_to_nodes_usd,
  COALESCE(
    br.liquidity_earnings,
    0
  ) AS earnings_to_pools,
  COALESCE(
    br.liquidity_earnings * drp.rune_usd,
    0
  ) AS earnings_to_pools_usd,
  br.avg_node_count,
  br._inserted_timestamp
FROM
  {{ ref('thorchain_silver_block_rewards') }} as br
JOIN daily_rune_price as drp
  ON br.day = drp.day
{% if is_incremental() -%}
WHERE {{ incremental_predicate('br.day') }}
{% endif -%}