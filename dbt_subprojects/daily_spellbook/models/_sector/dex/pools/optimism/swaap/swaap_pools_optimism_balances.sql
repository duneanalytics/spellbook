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
  SELECT
    poolId AS pool_address,
    tokenIn,
    tokenOut,
    evt_block_time AS creation_time
  FROM
    {{ source('swaap_v2_optimism', 'Vault_evt_Swap') }}
  WHERE
    (tokenIn = '0x4200000000000000000000000000000000000042'
    OR tokenOut = '0x4200000000000000000000000000000000000042')
),

filtered_balances AS (
  {{ balances_subset_daily(
      blockchain='optimism',
      token_address='0x4200000000000000000000000000000000000042',
      start_date='2024-06-07'
    ) }}
)

SELECT
  p.pool_address,
  p.tokenIn,
  p.tokenOut,
  p.creation_time,
  COALESCE(b.token_balance, 0) AS op_balance,
  COALESCE(b.snapshot_day, CURRENT_DATE) AS snapshot_day
FROM
  swaap_pools p
LEFT JOIN
  filtered_balances b ON p.pool_address = b.pool_address;
