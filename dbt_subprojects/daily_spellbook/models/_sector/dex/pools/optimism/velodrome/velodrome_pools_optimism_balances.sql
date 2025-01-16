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

WITH op_addresses AS (
  SELECT
    pool AS address,
    token0,
    token1,
    -- Add a column for token_address where it matches the Optimism token address
    CASE
      WHEN token0 = 0x4200000000000000000000000000000000000042 THEN token0
      WHEN token1 = 0x4200000000000000000000000000000000000042 THEN token1
    END AS token_address,
    evt_block_time AS creation_time
  FROM {{ source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated') }}
  WHERE
    token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042
),


filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2023-06-22',
       address_list='op_addresses',
  ) }}
)

SELECT 
  p.address AS pool_address,
  p.token0 AS token0,
  p.token1 AS token1,
  p.creation_time,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, current_date) AS snapshot_day
FROM 
  filtered_balances b
left JOIN
  op_addresses p ON b.address = p.address;
