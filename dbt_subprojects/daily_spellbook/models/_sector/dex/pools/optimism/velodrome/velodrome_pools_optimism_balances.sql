{{
  config(
    schema = 'velodrome_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

WITH 
velo_pools AS (
  SELECT 
    CAST(pool AS varchar) AS pool_address,
    CAST(token0 AS varchar) AS token0,
    CAST(token1 AS varchar) AS token1,
    evt_block_time AS creation_time
  FROM 
    {{ source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated') }}
  WHERE
    CAST(token0 AS varchar) = '0x4200000000000000000000000042'
    OR CAST(token1 AS varchar) = '0x4200000000000000000000000042'
),

token_list AS (
  SELECT DISTINCT 
    '0x4200000000000000000000000042' AS token_address
),

balances AS (
  {{
    balances_incremental_subset_daily(
      blockchain='optimism',
      token_list='token_list',
      start_date='2023-06-22'
    )
  }}
)

SELECT 
  p.pool_address,
  p.token0,
  p.token1,
  p.creation_time,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, CURRENT_DATE) AS snapshot_day
FROM 
  velo_pools p
LEFT JOIN 
  balances b ON p.pool_address = b.address;