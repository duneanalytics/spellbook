{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_deposit_extensions',
    materialized = 'view',
    unique_key = ['pool_address', 'token_id', 'init_tranche_id', 'current_tranche_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with recursive deposit_chain (pool_address, token_id, tranche_id, new_tranche_id, total_amount, block_time, is_active, evt_index, tx_hash, chain_level) as (
  select
    pool_address,
    token_id,
    tranche_id as tranche_id,
    tranche_id as new_tranche_id,
    sum(amount) as total_amount,
    max(block_time) as block_time,
    max_by(is_active, block_time) as is_active,
    max_by(evt_index, block_time) as evt_index,
    max_by(tx_hash, block_time) as tx_hash,
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
    d.block_time,
    d.is_active,
    d.evt_index,
    d.tx_hash,
    dc.chain_level + 1 as chain_level
  from deposit_chain dc
    inner join {{ ref('nexusmutual_ethereum_staking_events') }} d on dc.pool_address = d.pool_address
      and dc.token_id = d.token_id
      and dc.new_tranche_id = d.init_tranche_id
  where d.flow_type = 'deposit extended'
)

select 
  block_time,
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  total_amount,
  is_active,
  evt_index,
  tx_hash
from (
    select
      *,
      row_number() over (partition by pool_address, token_id, tranche_id order by chain_level desc) as rn
    from deposit_chain
  ) t
where rn = 1
