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
   SELECT
    pr.poolAddress AS address,
    v.tokenIn,
    v.tokenOut,
    0x4200000000000000000000000000000000000042 AS token_address,
    v.evt_block_time AS creation_time
  FROM {{ source('swaap_v2_optimism', 'Vault_evt_Swap') }} v
  LEFT JOIN {{ source('swaap_v2_optimism', 'Vault_evt_PoolRegistered') }} pr
    ON v.poolId = pr.poolId
  WHERE
    v.tokenIn = 0x4200000000000000000000000000000000000042
    OR v.tokenOut = 0x4200000000000000000000000000000000000042
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2024-06-07',
       address_list='op_addresses',  
  ) }}
)

SELECT 
  p.address AS pool_address,
  p.tokenIn AS tokenIn,
  p.tokenOut AS tokenOut,
  p.creation_time,
  COALESCE(b.balance, 0) AS op_balance,
  COALESCE(b.day, current_date) AS snapshot_day
FROM 
  filtered_balances b
LEFT JOIN
  op_addresses p ON b.address = p.address