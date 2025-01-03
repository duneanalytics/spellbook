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

WITH supply_events AS (
  SELECT
    depositor AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    amount AS supply_amount,
    'supply' as event_type
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042
),

borrow_events AS (
  SELECT
    borrower AS pool_address,
    DATE(evt_block_time) AS snapshot_day,
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    amount AS borrow_amount,
    'borrow' as event_type
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = 0x4200000000000000000000000000000042
),

combined_events AS (
  SELECT
    pool_address,
    snapshot_day,
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    supply_amount AS amount,
    event_type
  FROM supply_events
  
  UNION ALL
  
  SELECT
    pool_address,
    snapshot_day,
    evt_block_time,
    evt_block_number,
    evt_tx_hash,
    -borrow_amount AS amount, -- Negative for borrows
    event_type
  FROM borrow_events
),

-- Calculate running balance per day
daily_balances AS (
  SELECT
    pool_address,
    snapshot_day,
    SUM(amount) as net_balance
  FROM combined_events
  GROUP BY 1, 2
),

-- Final output with metadata
final_balances AS (
  SELECT DISTINCT
    pool_address,
    'aave' AS protocol_name,
    'v3' AS protocol_version,
    snapshot_day,
    net_balance as op_balance
  FROM daily_balances
  WHERE net_balance > 0
)

SELECT 
  pool_address,
  protocol_name,
  protocol_version,
  snapshot_day,
  op_balance
FROM final_balances