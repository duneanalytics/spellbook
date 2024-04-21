{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_deposit_extensions',
    materialized = 'view',
    unique_key = ['pool_address', 'token_id', 'init_tranche_id', 'current_tranche_id'],
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with recursive deposit_chain (pool_address, token_id, tranche_id, new_tranche_id, total_amount, is_active, chain_level) as (
  select
    pool_address,
    token_id,
    tranche_id as tranche_id,
    tranche_id as new_tranche_id,
    sum(amount) as total_amount,
    max_by(is_active, block_time) as is_active,
    1 as chain_level
  from {{ ref('nexusmutual_ethereum_staking_events') }}
  where flow_type = 'deposit'
  group by 1,2,3,4
  
  union all
  
  select 
    d.pool_address,
    d.token_id,
    dc.tranche_id,
    d.new_tranche_id,
    dc.total_amount + coalesce(d.topup_amount, 0) as total_amount,
    d.is_active,
    dc.chain_level + 1 as chain_level
  from deposit_chain dc
    inner join {{ ref('nexusmutual_ethereum_staking_events') }} d on dc.pool_address = d.pool_address
      and dc.token_id = d.token_id
      and dc.new_tranche_id = d.init_tranche_id
  where d.flow_type = 'deposit extended'
)

select 
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  total_amount,
  is_active
from (
    select
      *,
      row_number() over (partition by pool_address, token_id, tranche_id order by chain_level desc) as rn
    from deposit_chain
  ) t
where rn = 1
