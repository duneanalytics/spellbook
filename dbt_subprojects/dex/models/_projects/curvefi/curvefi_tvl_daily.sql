{{
  config(
    schema = 'curvefi',
    alias = 'tvl_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address', 'token_address', 'day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

WITH

pool_addresses AS (
  select 
    pool_address as address,
    coin0 as token_address
  from 
  {{ ref('curve_ethereum_view_pools') }}
  where coin0 is not null 

  union all 

  select 
    pool_address as address,
    coin1 as token_address
  from 
  {{ ref('curve_ethereum_view_pools') }}
  where coin1 is not null 

  union all 

  select 
    pool_address as address,
    coin2 as token_address
  from 
  {{ ref('curve_ethereum_view_pools') }}
  where coin2 is not null 

  union all 

  select 
    pool_address as address,
    coin3 as token_address
  from 
  {{ ref('curve_ethereum_view_pools') }}
  where coin3 is not null 

),

filtered_balances AS (
  {{ balances_incremental_subset_daily(
       blockchain='ethereum',
       start_date='2021-11-11',
       address_list='pool_addresses'
  ) }}
)

select 
    * 
from 
filtered_balances

-- test retry 