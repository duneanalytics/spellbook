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
  SELECT
    pool AS pool_address,
    tokenid,
    token,
    creation_time
  FROM {{ source('curve_optimism', 'pools') }}
  WHERE token = 0x4200000000000000000000000000000042
),

filtered_balances AS (
  SELECT
    address AS pool_address,
    balance AS op_balance,
    day AS snapshot_day
  FROM {{ source('tokens_optimism', 'balances_daily') }}
  WHERE token_address = 0x4200000000000000000000000000000042
  {% if is_incremental() %}
    AND {{ incremental_predicate('day') }}
  {% else %}
    AND day >= date '2021-11-11' --first pool initiated
  {% endif %}
)

SELECT 
  p.pool_address,
  p.tokenid,
  p.token,
  p.creation_time,
  COALESCE(b.op_balance, 0) AS op_balance,
  COALESCE(b.snapshot_day, CURRENT_DATE) AS snapshot_day
FROM filtered_balances b
RIGHT JOIN op_pools p 
  ON p.pool_address = b.pool_address