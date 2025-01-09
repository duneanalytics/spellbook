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

WITH velo_pools AS (
  SELECT 
    pool AS pool_address,
    token0,
    token1,
    evt_block_time AS creation_time
  FROM 
    {{ source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated') }}
  WHERE
    token0 = '0x4200000000000000000000000000000000000042'
    OR token1 = '0x4200000000000000000000000000000000000042'
),

filtered_balances AS (
  {{ balances_subset_daily(
      blockchain='optimism',
      token_address='0x4200000000000000000000000000000000000042',
      start_date='2023-06-22'
    ) }}
)

SELECT 
  p.pool_address,
  p.token0,
  p.token1,
  p.creation_time,
  COALESCE(b.token_balance, 0) AS op_balance,
  COALESCE(b.snapshot_day, CURRENT_DATE) AS snapshot_day
FROM 
  velo_pools p
LEFT JOIN 
  filtered_balances b ON p.pool_address = b.pool_address;
