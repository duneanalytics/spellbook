{{
  config(
    schema = 'optimism',
    alias = 'op_token_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day']
  )
}}

WITH op_pools AS (
  SELECT 
    pool AS pool_address,
    token0,
    token1,
    fee AS fee_tier,
    evt_block_time AS creation_time
  FROM 
    uniswap_v3_optimism.UniswapV3Factory_evt_PoolCreated
  WHERE 
    token0 = 0x4200000000000000000000000000000000000042
    OR token1 = 0x4200000000000000000000000000000000000042
),
filtered_balances AS (
  SELECT 
    address AS pool_address,
    balance AS op_balance,
    day AS snapshot_day
  FROM 
    tokens_optimism.balances_daily
  WHERE 
    token_address = 0x4200000000000000000000000000000000000042
)
SELECT 
  p.pool_address,
  p.token0,
  p.token1,
  p.fee_tier,
  p.creation_time,
  COALESCE(b.op_balance, 0) AS op_balance,
  COALESCE(b.snapshot_day, CURRENT_DATE) AS snapshot_day
FROM 
  op_pools p
LEFT JOIN 
  filtered_balances b ON p.pool_address = b.pool_address
