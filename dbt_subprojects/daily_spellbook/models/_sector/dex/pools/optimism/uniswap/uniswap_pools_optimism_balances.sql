{{
  config(
    schema = 'uniswap_pools_optimism',
    alias = 'balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_address', 'snapshot_day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.snapshot_day')]
  )
}}

with op_pools as (
  select 
    pool as pool_address,
    token0,
    token1,
    fee as fee_tier,
    creation_block_time as creation_time
  from 
    {{ source('uniswap_v3_optimism', 'pools') }}
  where
    token0 = '0x4200000000000000000000000000000000000042'
    or token1 = '0x4200000000000000000000000000000000000042'
)

, filtered_balances as (
  {{ balances_subset_daily(
       blockchain='optimism',
       token_address="'0x4200000000000000000000000000000000000042'",
       start_date='2021-11-11'
  ) }}
)

select 
  p.pool_address,
  p.token0,
  p.token1,
  p.fee_tier,
  p.creation_time,
  coalesce(b.token_balance, 0) as op_balance,
  coalesce(b.snapshot_day, current_date) as snapshot_day
from 
  filtered_balances b
right join
  op_pools p on p.pool_address = b.pool_address
