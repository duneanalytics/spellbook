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

WITH op_pools AS (
  SELECT DISTINCT
    CAST(pool AS varchar) AS pool_address
  FROM {{ source('curve_optimism', 'pools') }}
  WHERE token = 0x4200000000000000000000000000000042
),

filtered_balances AS (
  SELECT
    CAST(address AS varchar) AS pool_address,
    balance AS op_balance,
    day AS snapshot_day
  FROM {{ source('tokens_optimism', 'balances_daily') }}
  WHERE token_address = 0x4200000000000000000000000000000042
  {% if is_incremental() %}
    AND {{ incremental_predicate('day') }}
  {% else %}
    AND day >= date '2021-11-11'
  {% endif %}
)

SELECT 
  p.pool_address,
  'curve' as protocol_name,
  'v1' as protocol_version,
  b.snapshot_day,
  COALESCE(b.op_balance, 0) as op_balance
FROM op_pools p
LEFT JOIN filtered_balances b 
  ON p.pool_address = b.pool_address
WHERE COALESCE(b.op_balance, 0) > 0