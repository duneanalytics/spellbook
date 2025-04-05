{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_deposit_ordered',
    materialized = 'view',
    unique_key = ['flow_type', 'block_time', 'evt_index', 'tx_hash']
  )
}}

with

deposits as (
  select
    flow_type,
    block_time,
    block_date,
    pool_address,
    token_id,
    tranche_id,
    init_tranche_id,
    new_tranche_id,
    tranche_expiry_date,
    is_active,
    amount,
    topup_amount,
    user,
    evt_index,
    tx_hash,
    lead(block_date, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_block_date,
    lag(flow_type, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_flow_type,
    lead(flow_type, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_flow_type,
    lag(token_id, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_token_id,
    lag(tranche_id, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_tranche_id,
    lead(tranche_id, 1) over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_tranche_id,
    row_number() over (partition by pool_address, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as deposit_rn
  from {{ ref('nexusmutual_ethereum_staking_events') }}
  where flow_type in ('deposit', 'deposit extended')
)

select
  block_time,
  case
    when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit' and prev_tranche_id = tranche_id
    then 'deposit addon'
    else flow_type
  end as flow_type,
  block_date as stake_start_date,
  case
    when flow_type = 'deposit' and next_flow_type <> 'deposit extended' and next_tranche_id <> tranche_id then tranche_expiry_date
    when flow_type = 'deposit extended' and next_flow_type = 'deposit' then tranche_expiry_date
    when next_block_date > tranche_expiry_date then tranche_expiry_date
    else coalesce(next_block_date, tranche_expiry_date)
  end as stake_end_date,
  pool_address,
  token_id,
  tranche_id,
  init_tranche_id,
  new_tranche_id,
  tranche_expiry_date,
  is_active,
  amount,
  topup_amount,
  user,
  evt_index,
  tx_hash,
  deposit_rn
from deposits
