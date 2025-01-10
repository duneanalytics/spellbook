{{
  config(
    schema = 'solidly_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

WITH solidly_pools AS (
  SELECT
    CAST(pool AS varchar) AS pool_address,
    CAST(token0 AS varchar) AS token0,
    CAST(token1 AS varchar) AS token1,
    fee,
    tickSpacing,
    evt_block_time AS creation_time
  FROM
    {{ source('solidly_v3_optimism', 'SolidlyV3Factory_evt_PoolCreated') }}
  WHERE
    CAST(token0 AS varchar) = '0x4200000000000000000000000042'
    OR CAST(token1 AS varchar) = '0x4200000000000000000000000042'
),

filtered_balances AS (
  {{ balances_subset_daily(
      blockchain='optimism',
      token_address='0x4200000000000000000000000042',
      start_date='2024-01-30'
    ) }}
)

SELECT
  p.pool_address,
  p.token0,
  p.token1,
  p.fee,
  p.tickSpacing,
  p.creation_time,
  COALESCE(b.token_balance, 0) AS op_balance,
  COALESCE(b.snapshot_day, CURRENT_DATE) AS snapshot_day
FROM
  solidly_pools p
LEFT JOIN
  filtered_balances b ON p.pool_address = b.pool_address