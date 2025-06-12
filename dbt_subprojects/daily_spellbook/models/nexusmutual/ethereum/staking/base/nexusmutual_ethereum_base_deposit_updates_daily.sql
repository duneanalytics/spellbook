{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_deposit_updates_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_id', 'token_id', 'block_date'],
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
    asu.activeStake / 1e18 as active_stake,
    asu.evt_index,
    asu.evt_tx_hash as tx_hash,
    row_number() over (partition by asu.evt_block_time, asu.evt_tx_hash order by asu.evt_index desc) as active_stake_rn
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_ActiveStakeUpdated') }} asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

deposit_updates as (
  select
    sp.pool_id,
    sp.pool_address,
    du.evt_block_time as block_time,
    du.evt_block_date as block_date,
    du.trancheId as tranche_id,
    du.tokenId as token_id,
    du.stakeShares as stake_shares,
    du.stakeSharesSupply as stake_shares_supply,
    du.evt_index,
    du.evt_tx_hash as tx_hash,
    count(*) over (partition by du.evt_block_time, du.evt_tx_hash) as deposit_count
  from {{ source('nexusmutual_ethereum', 'StakingPool_evt_DepositUpdated') }} du
    inner join staking_pools sp on du.contract_address = sp.pool_address
  where du.stakeShares > 0
),

updates_combined as (
  select
    du.block_time,
    du.block_date,
    du.pool_id,
    du.pool_address,
    du.tranche_id,
    du.token_id,
    asu.active_stake,
    du.stake_shares,
    du.stake_shares_supply,
    asu.active_stake * du.stake_shares / du.stake_shares_supply as token_stake,
    cast(from_unixtime(91.0 * 86400.0 * cast(du.tranche_id + 1 as double)) as date) as tranche_expiry_date,
    du.evt_index,
    du.tx_hash
  from deposit_updates du
    inner join active_stake_updates asu
      on du.pool_id = asu.pool_id
      and du.block_time = asu.block_time
      and du.tx_hash = asu.tx_hash
  where (du.deposit_count = 1 and asu.active_stake_rn = 1)
    or (du.deposit_count > 1 and du.evt_index between asu.evt_index - 6 and asu.evt_index)
),

daily_snapshots as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    sum(token_stake) as token_stake,
    min(tranche_expiry_date) as tranche_expiry_date -- take closest expiry date for total token stake (not accurate if there are multiple tranches)
  from updates_combined
  group by 1, 2, 3, 4
),

daily_snapshots_with_next as (
  select
    *,
    lead(block_date) over (partition by pool_id, token_id order by block_date) as next_update_date
  from (
    -- regular incremental load
    select *, cast(null as bigint) as rn
    from daily_snapshots
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
    -- find last snapshot for each token pre incremental load window
    union all
    select *
    from (
      select *, row_number() over (partition by pool_id, token_id order by block_date desc) as rn
      from daily_snapshots
      where not {{ incremental_predicate('block_date') }}
    )
    where rn = 1
  ) t
),

pool_start_dates as (
  select
    pool_id,
    token_id,
    min(block_date) as block_date_start
  from daily_snapshots_with_next
  group by 1, 2
),

daily_sequence as (
  select
    s.pool_id,
    s.token_id,
    d.timestamp as block_date
  from {{ source('utils', 'days') }} d
    inner join pool_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= current_date
),

forward_fill as (
  select
    s.block_date,
    s.pool_id,
    dc.pool_address,
    s.token_id,
    dc.token_stake,
    dc.tranche_expiry_date
  from daily_sequence s
    left join daily_snapshots_with_next dc
      on s.pool_id = dc.pool_id
      and s.token_id = dc.token_id
      and s.block_date >= dc.block_date
      and (s.block_date < dc.next_update_date or dc.next_update_date is null)
)

select
  block_date,
  pool_id,
  pool_address,
  token_id,
  token_stake,
  tranche_expiry_date
from forward_fill
{% if is_incremental() %}
where {{ incremental_predicate('block_date') }}
{% endif %}
