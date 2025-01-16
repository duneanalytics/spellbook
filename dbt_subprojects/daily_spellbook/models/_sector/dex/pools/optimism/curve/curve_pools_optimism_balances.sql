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

WITH op_addresses as (
  SELECT
    pool as address,
    token as token_address,
    version
  FROM 
    {{ source('curve_optimism', 'pools') }}
  WHERE
     token = 0x4200000000000000000000000000000000000042
),

filtered_balances as (
   SELECT
    address,
    balance,
    day,
    ROW_NUMBER() OVER (PARTITION BY address, day ORDER BY balance DESC) AS row_num
  FROM 
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2021-11-11',
       address_list='op_addresses'
  ) }}
  WHERE row_num = 1
)

SELECT 
  p.address as pool_address,
  p.token_address as token,
  p.version,
  COALESCE(b.balance, 0) as op_balance,
  COALESCE(b.day, current_date) as snapshot_day
FROM 
  filtered_balances b
LEFT JOIN
  op_addresses p on b.address = p.address
