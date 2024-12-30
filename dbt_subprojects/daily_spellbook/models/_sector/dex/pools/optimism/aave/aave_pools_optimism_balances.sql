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
    token_address,
    SUM(amount) AS total_supplied,
    CAST(evt_block_time AS DATE) AS snapshot_day
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = '0x4200000000000000000000000000000000000042'
  GROUP BY pool_address, token_address, snapshot_day
),
borrow_op AS (
  SELECT
    borrower AS pool_address,
    token_address,
    SUM(amount) AS total_borrowed,
    CAST(evt_block_time AS DATE) AS snapshot_day
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = '0x4200000000000000000000000000000000000042'
  GROUP BY pool_address, token_address, snapshot_day
)
SELECT *
FROM (
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
) net_balance
WHERE op_balance > 0;
