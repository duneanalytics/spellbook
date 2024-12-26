{{
  config(
    schema = 'top_protocols_op_balances',
    alias = 'aave_v3_op_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['wallet_address', 'snapshot_day']
  )
}}

WITH supply_op AS (
  SELECT
    depositor AS wallet_address,
    token_address,
    SUM(amount) AS total_supplied,
    CAST(evt_block_time AS DATE) AS snapshot_day
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = '0x4200000000000000000000000000000000000042'
  GROUP BY wallet_address, token_address, snapshot_day
),
borrow_op AS (
  SELECT
    borrower AS wallet_address,
    token_address,
    SUM(amount) AS total_borrowed,
    CAST(evt_block_time AS DATE) AS snapshot_day
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = '0x4200000000000000000000000000000000000042'
  GROUP BY wallet_address, token_address, snapshot_day
),
net_balance AS (
  SELECT
    COALESCE(s.wallet_address, b.wallet_address) AS wallet_address,
    'aave' AS protocol_name,
    'v3' AS protocol_version,
    COALESCE(s.snapshot_day, b.snapshot_day) AS snapshot_day,
    COALESCE(s.total_supplied, 0) - COALESCE(b.total_borrowed, 0) AS op_balance
  FROM supply_op s
  FULL OUTER JOIN borrow_op b
    ON s.wallet_address = b.wallet_address
    AND s.snapshot_day = b.snapshot_day
)

SELECT *
FROM net_balance
WHERE op_balance > 0;
