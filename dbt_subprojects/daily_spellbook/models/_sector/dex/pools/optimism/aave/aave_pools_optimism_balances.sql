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

-- Aave address that holds OP tokens
WITH aave_op_reserve AS (
  SELECT
    0x513c7E3a9c69cA3e22550eF58AC1C0088e918FFf  AS address,
    0x4200000000000000000000000000000000000042  AS token_address
),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2021-11-11',
       address_token_list = 'aave_op_reserve'
  ) }}
)

SELECT
  p.address AS pool_address,
  'aave' AS protocol_name,
  'v3' AS protocol_version,
  COALESCE(b.day, CURRENT_DATE) AS snapshot_day,
  COALESCE(b.balance, 0) AS op_balance
FROM
  filtered_balances b
left join
  aave_op_reserve p on b.address = p.address