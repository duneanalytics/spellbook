{{
  config(
    schema = 'aave_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

WITH raw_supply AS (
  SELECT
    depositor AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    evt_block_time,
    evt_block_number,
    amount
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042
),

raw_borrow AS (
  SELECT
    borrower AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    evt_block_time,
    evt_block_number,
    amount
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = 0x4200000000000000000000000042
),

-- Aggregate supply transactions per day
supply_daily AS (
  SELECT
    pool_address,
    snapshot_day,
    SUM(amount) as total_supplied
  FROM raw_supply
  GROUP BY 1, 2
),

-- Aggregate borrow transactions per day
borrow_daily AS (
  SELECT
    pool_address,
    snapshot_day,
    SUM(amount) as total_borrowed
  FROM raw_borrow
  GROUP BY 1, 2
),

-- Combine supply and borrow data with proper deduplication
combined_daily AS (
  SELECT
    COALESCE(s.pool_address, b.pool_address) as pool_address,
    COALESCE(s.snapshot_day, b.snapshot_day) as snapshot_day,
    COALESCE(s.total_supplied, 0) as total_supplied,
    COALESCE(b.total_borrowed, 0) as total_borrowed,
    COALESCE(s.total_supplied, 0) - COALESCE(b.total_borrowed, 0) as net_balance
  FROM supply_daily s
  FULL OUTER JOIN borrow_daily b
    ON s.pool_address = b.pool_address
    AND s.snapshot_day = b.snapshot_day
),

-- Add final deduplication step
final_balances AS (
  SELECT DISTINCT ON (pool_address, snapshot_day)
    pool_address,
    'aave' as protocol_name,
    'v3' as protocol_version,
    snapshot_day,
    net_balance as op_balance
  FROM combined_daily
  WHERE net_balance > 0
  ORDER BY pool_address, snapshot_day, net_balance DESC
)

SELECT
  pool_address,
  protocol_name,
  protocol_version,
  snapshot_day,
  op_balance
FROM final_balances