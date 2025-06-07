{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_staking_deposit_ordered',
    materialized = 'view',
    unique_key = ['flow_type', 'block_time', 'evt_index', 'tx_hash']
  )
}}

/*
  - this query attempts to re-order deposit events into a chain of deposits with clear start and end dates
  - it serves as base for staking_deposit_extensions.sql
  - base case scenarios are already following the deposit extension logic
  - the query attempts to classify the following scenarios:
    - deposit addon = deposit following another deposit on the same tranche
    - deposit ext addon = deposit or deposit extended following a deposit extended on the same tranche
  
  note: ideally this query would be scrapped - if there was a way to follow the deposit extension logic in a single query without all the exceptions
*/

with

deposits as (
  select
    flow_type,
    block_time,
    block_date,
    pool_id,
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
    -- token_id & flow_type
    lag(token_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_token_id,
    lag(flow_type, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_flow_type,
    lead(flow_type, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_flow_type,
    -- init_tranche_id & new_tranche_id
    lag(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_init_tranche_id,
    lag(new_tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as prev_new_tranche_id,
    lead(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_init_tranche_id,
    lead(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as next_new_tranche_id,
    -- block_date
    lead(block_date, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_init_block_date,
    lead(block_date, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as next_new_block_date,
    -- deposit_rn
    --row_number() over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as deposit_rn, -- v1
    row_number() over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), new_tranche_id nulls first, block_time) as deposit_rn
  from {{ ref('nexusmutual_ethereum_staking_events') }}
  where flow_type in ('deposit', 'deposit extended')
),

deposits_enriched as (
  select
    block_time,
    case
      -- ==================== exceptions ======================
      -- deposits that eventually get extended on both previous and next tranches
      when tx_hash = 0x5b3c8ff0e4f60bee6f836f5573227c4560912fa057b58b6572b77a6492506f9d then 'deposit ext addon' -- token_id=129
      when tx_hash = 0x53b5ce390f858953a5543b2b0d8b82cf55af4f90f08df92cc38af635410eeae4 then 'deposit ext addon' -- token_id=137
      when tx_hash = 0xfba5c755338fae7d0414a4ed8b92c6d705631465c0264e7cf8deb9b76c96bdef then 'deposit ext addon' -- token_id=168
      when tx_hash = 0x6e7a4fe6d0ea7147c3181c78054ea32f77cb3946ecfef78dfe51506b7e4c564b then 'deposit ext addon' -- token_id=214
      -- intercept actual deposit before it gets classified as deposit ext addon:
      -- (ex : token_id=1 & tx=0xdb093afcfca64cf55b9bcdfa34ab3fe2cc8aba7986233c07b56b05680726f40f)
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit extended'
        and tranche_id = next_init_tranche_id and tranche_id <> coalesce(prev_new_tranche_id, tranche_id) then 'deposit'
      -- if there is a deposit following a deposit extended on the same tranche - scenario 1:
      -- (ex 1 : token_id=31 & tx=0x1aabd858abbb4bffb7723c81c9e81206eba924b74ce377c310728607dca5c7aa)
      -- (ex 2 : token_id=39 & tx=0xe232a7ee5a1c20b1d69e499ab1c2f7265f6695b4fad2e49e73549b3faba77c12)
      when token_id = prev_token_id and flow_type = 'deposit' /*and prev_flow_type = 'deposit extended'*/ and tranche_id = prev_new_tranche_id then 'deposit ext addon'
      -- if there is a deposit following a deposit extended on the same tranche - scenario 2:
      -- (ex token_id=39 & tx=0x0131fe7eddf72cfca03c7926dab002061c78f145ed240139d8c2612726735a8d)
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit extended' and tranche_id = next_init_tranche_id then 'deposit ext addon'
      -- if there is a deposit following another deposit on the same tranche:
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit' and tranche_id = prev_init_tranche_id then 'deposit addon'
      -- ==================== regular flow ====================
      else flow_type
    end as flow_type,
    block_date as stake_start_date, -- stays static
    -- adjust stake_end_date to either the next deposit or the tranche expiry date
    case
      -- after deposit extension -> is there deposit ext addon:
      when flow_type = 'deposit extended' and next_flow_type = 'deposit' and new_tranche_id = next_new_tranche_id then coalesce(next_init_block_date, tranche_expiry_date)
      when flow_type = 'deposit' and next_flow_type <> 'deposit extended' and next_init_tranche_id <> tranche_id then tranche_expiry_date
      when flow_type = 'deposit extended' and next_flow_type = 'deposit' then tranche_expiry_date
      when next_init_block_date > tranche_expiry_date then tranche_expiry_date
      else coalesce(next_init_block_date, tranche_expiry_date)
    end as stake_end_date,
    pool_id,
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
    case
      -- ==================== exceptions ======================
      when token_id = 215 and deposit_rn = 3 then 5
      when token_id = 215 and deposit_rn = 4 then 3
      when token_id = 215 and deposit_rn = 5 then 6
      when token_id = 215 and deposit_rn = 6 then 4
      -- ==================== regular flow ====================
      else deposit_rn
    end as deposit_rn
  from deposits
)

select
  block_time,
  flow_type,
  stake_start_date,
  stake_end_date,
  pool_id,
  pool_address,
  token_id,
  -- for 'deposit ext addon' re-shuffle deposit-like fields to emulate deposit extension
  if(flow_type = 'deposit ext addon', null, tranche_id) as tranche_id,
  if(flow_type = 'deposit ext addon', tranche_id, init_tranche_id) as init_tranche_id,
  if(flow_type = 'deposit ext addon', tranche_id, new_tranche_id) as new_tranche_id,
  tranche_expiry_date,
  is_active,
  if(flow_type = 'deposit ext addon', null, amount) as amount,
  if(flow_type = 'deposit ext addon', amount, topup_amount) as topup_amount,
  user,
  evt_index,
  tx_hash,
  deposit_rn
from deposits_enriched
