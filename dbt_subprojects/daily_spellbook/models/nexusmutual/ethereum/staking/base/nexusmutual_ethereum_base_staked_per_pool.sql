{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'base_staked_per_pool',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'pool_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

with

staking_pools as (
  select
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  from {{ ref('nexusmutual_ethereum_staking_pools_list') }} sp
    inner join (
      select
        pool_id,
        cast(min(block_time) as date) as first_stake_event_date
      from {{ ref('nexusmutual_ethereum_staking_events') }}
      group by 1
    ) se on sp.pool_id = se.pool_id
),

active_stake_daily as (
  select
    pool_id,
    pool_address,
    block_date,
    active_stake as total_staked_nxm,
    row_number() over (partition by pool_id order by block_date) as active_stake_event_rn
  from {{ ref('nexusmutual_ethereum_base_active_stake_daily') }}
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    d.timestamp as block_date,
    if(asu.block_date is null, true, false) as is_pre_active_stake_events
  from {{ source('utils', 'days') }} d
    inner join staking_pools sp on d.timestamp >=sp.first_stake_event_date
    left join active_stake_daily asu
      on sp.pool_id = asu.pool_id
      and asu.active_stake_event_rn = 1
      and d.timestamp >= asu.block_date
  {% if is_incremental() %}
  where {{ incremental_predicate('d.timestamp') }}
  {% endif %}
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(coalesce(total_amount, 0)) as total_staked_nxm
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join {{ ref('nexusmutual_ethereum_base_staking_deposit_extensions') }} se
          on d.pool_id = se.pool_id
         and d.block_date >= se.stake_start_date
         and d.block_date < se.stake_end_date
      where d.is_pre_active_stake_events
        {% if is_incremental() %}
        and {{ incremental_predicate('d.block_date') }}
        {% endif %}
      group by 1, 2, 3
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        inner join {{ ref('nexusmutual_ethereum_staking_events') }} se
          on d.pool_id = se.pool_id
         and d.block_date >= se.block_date
         and d.block_date < coalesce(se.tranche_expiry_date, current_date)
      where se.flow_type in ('withdraw', 'stake burn')
        and d.is_pre_active_stake_events
        {% if is_incremental() %}
        and {{ incremental_predicate('d.block_date') }}
        {% endif %}
      group by 1, 2, 3
    ) t
  group by 1, 2, 3
),

staked_nxm_per_pool_combined as (
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm
  from staked_nxm_per_pool
  union all
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm
  from active_stake_daily
  {% if is_incremental() %}
  where {{ incremental_predicate('block_date') }}
  {% endif %}
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm
from staked_nxm_per_pool_combined
