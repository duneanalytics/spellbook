{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_staking_deposit_extensions',
    materialized = 'view',
    unique_key = ['pool_id', 'token_id', 'init_tranche_id', 'current_tranche_id', 'chain_level', 'stake_start_date', 'stake_end_date']
  )
}}

with recursive deposit_chain (
  block_time, pool_id, pool_address, token_id, tranche_id, new_tranche_id, amount, 
  stake_start_date, stake_end_date, is_active, evt_index, tx_hash, deposit_rn, chain_level
) as (
  select
    block_time,
    pool_id,
    pool_address,
    token_id,
    tranche_id,
    tranche_id as new_tranche_id,
    amount,
    stake_start_date,
    stake_end_date,
    is_active,
    evt_index,
    tx_hash,
    deposit_rn,
    1 as chain_level
  from {{ ref('nexusmutual_ethereum_base_staking_deposit_ordered') }}
  where flow_type = 'deposit'
  
  union all
  
  select 
    d.block_time,
    d.pool_id,
    d.pool_address,
    d.token_id,
    dc.tranche_id,
    coalesce(d.new_tranche_id, dc.tranche_id) as new_tranche_id,
    dc.amount + coalesce(d.amount, d.topup_amount, 0) as amount,
    d.stake_start_date,
    d.stake_end_date,
    d.is_active,
    d.evt_index,
    d.tx_hash,
    d.deposit_rn,
    dc.chain_level + 1 as chain_level
  from deposit_chain dc
    inner join {{ ref('nexusmutual_ethereum_base_staking_deposit_ordered') }} d on dc.pool_id = d.pool_id and dc.token_id = d.token_id
  where dc.deposit_rn + 1 = d.deposit_rn -- look at next level of deposit chain (dc=base, d=next)
    and ((d.flow_type in ('deposit extended', 'deposit ext addon') and dc.new_tranche_id = d.init_tranche_id)
      or (d.flow_type = 'deposit addon' and dc.new_tranche_id = d.tranche_id))
)

select
  block_time,
  date_trunc('day', block_time) as block_date,
  pool_id,
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  amount,
  stake_start_date,
  stake_end_date,
  is_active,
  chain_level,
  token_tranche_rn,
  evt_index,
  tx_hash
from (
    select
      *,
      row_number() over (partition by pool_id, token_id, tranche_id order by chain_level desc) as token_tranche_rn
    from deposit_chain
  ) t
