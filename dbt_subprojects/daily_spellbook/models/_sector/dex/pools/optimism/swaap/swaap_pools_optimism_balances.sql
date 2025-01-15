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

WITH swaap_pools AS (
  SELECT DISTINCT
    CAST(poolId AS varchar) AS pool_address,
    CAST(tokenIn AS varchar) AS tokenIn,
    CAST(tokenOut AS varchar) AS tokenOut,
    evt_block_time AS creation_time
  FROM
    {{ source('swaap_v2_optimism', 'Vault_evt_Swap') }}
  WHERE
    CAST(tokenIn AS varchar) = '0x4200000000000000000042'
    OR CAST(tokenOut AS varchar) = '0x4200000000000000000042'
),

token_list AS (
  SELECT DISTINCT 
    '0x4200000000000000000042' AS token_address
),

balances AS (
  {{
    balances_incremental_subset_daily(
      blockchain='optimism',
      token_list='token_list',
      start_date='2024-06-07'
    )
  }}
)

SELECT DISTINCT
  p.pool_address,
  p.tokenIn,
  p.tokenOut,
  p.creation_time,
  COALESCE(b.balance, 0) AS op_balance,
  CAST(b.day AS date) AS snapshot_day
FROM
  swaap_pools p
LEFT JOIN
  balances b ON p.pool_address = b.address
WHERE TRUE
ORDER BY p.pool_address, snapshot_day
;