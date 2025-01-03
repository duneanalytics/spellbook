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

WITH supply_op AS (
  SELECT
    depositor AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    SUM(amount) AS total_supplied
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042
  GROUP BY 1, 2
),

borrow_op AS (
  SELECT
    borrower AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    SUM(amount) AS total_borrowed
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042
  GROUP BY 1, 2
),

daily_balances AS (
  SELECT
    COALESCE(s.pool_address, b.pool_address) AS pool_address,
    'aave' AS protocol_name,
    'v3' AS protocol_version,
    COALESCE(s.snapshot_day, b.snapshot_day) AS snapshot_day,
    COALESCE(s.total_supplied, 0) - COALESCE(b.total_borrowed, 0) AS op_balance
  FROM supply_op s
  FULL OUTER JOIN borrow_op b
    ON s.pool_address = b.pool_address
    AND s.snapshot_day = b.snapshot_day
)

SELECT
  pool_address,
  protocol_name,
  protocol_version,
  snapshot_day,
  SUM(op_balance) AS op_balance  -- Aggregate in case there are multiple entries per day
FROM daily_balances
WHERE op_balance > 0
GROUP BY 1, 2, 3, 4