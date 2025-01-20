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

WITH op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    0x4200000000000000000000000000000000000042 as token_address, 
    fee,
    tickSpacing,
    evt_block_time AS creation_time
  FROM {{ source('solidly_v3_optimism', 'SolidlyV3Factory_evt_PoolCreated') }}
  WHERE
    token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2024-01-30',
       address_token_list='op_addresses',          
  ) }}
)

SELECT 
  p.address AS pool_address,
  p.token0 AS token0,
  p.token1 AS token1,
  p.fee,
  p.tickSpacing,
  p.creation_time,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, current_date) AS snapshot_day
FROM 
  filtered_balances b
LEFT JOIN
  op_addresses p ON b.address = p.address
