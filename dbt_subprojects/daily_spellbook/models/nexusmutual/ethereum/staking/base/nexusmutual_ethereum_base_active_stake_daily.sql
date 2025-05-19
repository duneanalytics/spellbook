{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_active_stake_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

with

staking_pools as (
  select
    pool_id,
    pool_address
  from {{ ref('nexusmutual_ethereum_staking_pools_list') }}
),

active_stake_updates as (
  select
    sp.pool_id,
    sp.pool_address,
    asu.evt_block_time as block_time,
    asu.evt_block_date as block_date,
    asu.evt_block_number as block_number,
    asu.activeStake / 1e18 as active_stake,
    asu.stakeSharesSupply as stake_shares_supply,
    asu.evt_tx_hash as tx_hash
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_ActiveStakeUpdated') }} asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

daily_snapshots as (
  select
    pool_id,
    pool_address,
    block_date,
    max_by(active_stake, block_time) as active_stake,
    max_by(stake_shares_supply, block_time) as stake_shares_supply,
    max_by(tx_hash, block_time) as tx_hash
  from active_stake_updates
  group by 1, 2, 3
),

daily_snapshots_with_next as (
  select
    *,
    lead(block_date) over (partition by pool_id order by block_date) as next_update_date
  from (
    select * from daily_snapshots
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
    union all
    select
      pool_id,
      pool_address,
      max(block_date) as block_date,
      max_by(active_stake, block_date) as active_stake,
      max_by(stake_shares_supply, block_date) as stake_shares_supply,
      max_by(tx_hash, block_date) as tx_hash
    from daily_snapshots
    where not {{ incremental_predicate('block_date') }}
    group by 1, 2
  ) t
),

pool_start_dates as (
  select
    pool_id,
    pool_address,
    min(block_date) as block_date_start
  from daily_snapshots_with_next
  group by 1, 2
),

daily_sequence as (
  select
    s.pool_id,
    s.pool_address,
    d.timestamp as block_date
  from {{ source('utils', 'days') }} d
    inner join pool_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= current_date
),

forward_fill as (
  select
    s.pool_id,
    s.pool_address,
    s.block_date,
    dc.active_stake,
    dc.stake_shares_supply,
    dc.tx_hash
  from daily_sequence s
    left join daily_snapshots_with_next dc
      on s.pool_id = dc.pool_id
      and s.block_date >= dc.block_date
      and (s.block_date < dc.next_update_date or dc.next_update_date is null)
)

select
  pool_id,
  pool_address,
  block_date,
  active_stake,
  stake_shares_supply,
  tx_hash
from forward_fill
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
