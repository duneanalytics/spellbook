{{
  config(
    schema = 'swaap_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

WITH op_addresses AS (
  SELECT DISTINCT
    poolId as address,
    tokenIn,
    tokenOut,
    evt_block_time as creation_time
  FROM {{ source('swaap_v2_optimism', 'Vault_evt_Swap') }}
  WHERE
    tokenIn = from_hex('0x4200000000000000000000000000000000000042')
    OR tokenOut = from_hex('0x4200000000000000000000000000000000000042')
),

op_token AS (
  SELECT 
    from_hex('0x4200000000000000000000000000000000000042') as token_address
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2024-06-07',
       address_list='op_addresses',  
       token_list='op_token'         
  ) }}
)

SELECT 
  lower(to_hex(p.address)) as pool_address,
  lower(to_hex(p.tokenIn)) as tokenIn,
  lower(to_hex(p.tokenOut)) as tokenOut,
  p.creation_time,
  COALESCE(b.balance, 0) as op_balance,
  COALESCE(b.day, current_date) as snapshot_day
FROM 
  filtered_balances b
RIGHT JOIN
  op_addresses p on b.address = p.address