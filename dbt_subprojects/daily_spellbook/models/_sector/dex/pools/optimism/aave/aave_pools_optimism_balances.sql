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

WITH raw_addresses AS (
  SELECT
    depositor AS address,
    0x4200000000000000000000000000000000000042 AS token_address
  FROM {{ source('aave_v3_optimism', 'supply') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042

  UNION ALL

  SELECT
    borrower AS address,
    0x4200000000000000000000000000000000000042 AS token_address
  FROM {{ source('aave_v3_optimism', 'borrow') }}
  WHERE token_address = 0x4200000000000000000000000000000000000042
),

-- Deduplicate pool addresses
aave_addresses AS (
  SELECT
    address,
    token_address,
    ROW_NUMBER() OVER (PARTITION BY address ORDER BY token_address) AS rn
  FROM raw_addresses
),

deduplicated_addresses AS (
  SELECT
    address,
    token_address
  FROM aave_addresses
  WHERE rn = 1
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain = 'optimism',
       start_date = '2021-11-11',
       address_token_list = 'deduplicated_addresses'
  ) }}
)

SELECT
  b.address AS pool_address,
  'aave' AS protocol_name,
  'v3' AS protocol_version,
  COALESCE(b.day, CURRENT_DATE) AS snapshot_day,
  COALESCE(b.balance, 0) AS op_balance
FROM
  filtered_balances b
