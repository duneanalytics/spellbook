{{
  config(
    schema = 'curve_pools_optimism',
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
    token AS token_address,
    version
  FROM {{ source('curve_optimism', 'pools') }}
  WHERE token = 0x4200000000000000000000000000000000000042
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2021-11-11',
       address_list='op_addresses'
  ) }}
)

SELECT 
  p.address AS pool_address,
  p.token_address AS token,
  p.version,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, current_date) AS snapshot_day
FROM 
  filtered_balances b
LEFT JOIN
  op_addresses p ON b.address = p.address;
