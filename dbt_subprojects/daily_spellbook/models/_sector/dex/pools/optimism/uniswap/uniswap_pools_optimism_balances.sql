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

with op_addresses as (
  select
    lower(hex(pool)) as address,  -- Convert to lowercase hex (varchar)
    lower(hex(token0)) as token0, -- Ensure token0 is varchar
    lower(hex(token1)) as token1, -- Ensure token1 is varchar
    fee as fee_tier,
    creation_block_time as creation_time
  from 
    {{ source('uniswap_v3_optimism', 'pools') }}
  where
    lower(hex(token0)) = '0x4200000000000000000000000000000000000042'
    or lower(hex(token1)) = '0x4200000000000000000000000000000000000042'
),

op_token as (
  select 
    lower('0x4200000000000000000000000000000000000042') as token_address  -- Convert to lowercase (varchar)
),

filtered_balances as (
  {{ balances_incremental_subset_daily(
       blockchain='optimism',
       start_date='2021-11-11',
       address_list='op_addresses',  
       token_list='op_token'         
  ) }}
)

select 
  p.address as pool_address,
  p.token0,
  p.token1,
  p.fee_tier,
  p.creation_time,
  coalesce(b.balance, 0) as op_balance,
  coalesce(b.day, current_date) as snapshot_day
from 
  filtered_balances b
right join
  op_addresses p on p.address = b.address  -- Ensure both columns are varchar
