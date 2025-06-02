{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['flow_type', 'block_time', 'evt_index', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

staking_events as (
  select
    'deposit' as flow_type,
    sd.evt_block_time as block_time,
    sd.evt_block_number as block_number,
    sd.contract_address as pool_address,
    sd.tokenId as token_id,
    sd.trancheId as tranche_id,
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(sd.trancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(sd.amount as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
    sd.user,
    sd.evt_index,
    sd.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_StakeDeposited') }} sd
  {% if is_incremental() %}
  where {{ incremental_predicate('sd.evt_block_time') }}
  {% endif %}
  
  union all

  select
    'deposit extended' as flow_type,
    de.evt_block_time as block_time,
    de.evt_block_number as block_number,
    de.contract_address as pool_address,
    de.tokenId as token_id,
    cast(null as uint256) as tranche_id,
    de.initialTrancheId as init_tranche_id,
    de.newTrancheId as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(de.newTrancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(null as double) as amount,
    cast(de.topUpAmount as double) / 1e18 as topup_amount,
    de.user,
    de.evt_index,
    de.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_DepositExtended') }} de
  {% if is_incremental() %}
  where {{ incremental_predicate('de.evt_block_time') }}
  {% endif %}

  union all

  select
    'withdraw' as flow_type,
    w.evt_block_time as block_time,
    w.evt_block_number as block_number,
    w.contract_address as pool_address,
    w.tokenId as token_id,
    w.tranche as tranche_id,
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(w.tranche + 1 as double)) as date) as tranche_expiry_date,
    -1 * cast((w.amountStakeWithdrawn) as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
    w.user,
    w.evt_index,
    w.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_Withdraw') }} w
  where w.amountStakeWithdrawn > 0
  {% if is_incremental() %}
  and {{ incremental_predicate('w.evt_block_time') }}
  {% endif %}

  union all

  select
    'stake burn' as flow_type,
    eb.evt_block_time as block_time,
    eb.evt_block_number as block_number,
    eb.contract_address as pool_address,
    cast(null as uint256) as token_id,
    cast(floor(date_diff('day', from_unixtime(0), eb.evt_block_time) / 91.0) as uint256) as tranche_id,
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(
      floor(date_diff('day', from_unixtime(0), eb.evt_block_time) / 91.0)
    + 1 as double)) as date) as tranche_expiry_date,
    -1 * cast(eb.amount as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
    cast(null as varbinary) as user,
    eb.evt_index,
    eb.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_StakeBurned') }} eb
  {% if is_incremental() %}
  where {{ incremental_predicate('eb.evt_block_time') }}
  {% endif %}
)

select
  se.flow_type,
  se.block_time,
  date_trunc('day', se.block_time) as block_date,
  se.block_number,
  spl.pool_id,
  se.pool_address,
  se.token_id,
  se.tranche_id,
  se.init_tranche_id,
  se.new_tranche_id,
  se.tranche_expiry_date,
  if(se.tranche_expiry_date > current_date, true, false) as is_active,
  se.amount,
  se.topup_amount,
  se.user,
  se.evt_index,
  se.tx_hash
from staking_events se
  inner join {{ ref('nexusmutual_ethereum_staking_pools_list') }} spl on se.pool_address = spl.pool_address
