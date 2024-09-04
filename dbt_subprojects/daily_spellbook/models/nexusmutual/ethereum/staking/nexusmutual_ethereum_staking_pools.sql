{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staking_pools',
    materialized = 'view',
    unique_key = ['pool_id', 'product_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

staking_pool_base as (
  select
    sp.pool_id,
    sp.pool_address,
    sp.manager_address,
    sp.manager_ens,
    sp.manager,
    if(sp.is_private_pool, 'Private', 'Public') as pool_type,
    sp.initial_pool_fee / 100.00 as initial_pool_fee,
    sp.current_pool_fee / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    sp.product_id,
    sp.initial_price,
    sp.target_price,
    sp.initial_weight / 100.00 as initial_weight,
    sp.target_weight / 100.00 as target_weight,
    sp.pool_created_time,
    sp.product_added_time
  from nexusmutual_ethereum.staking_pools sp
),

staking_pool_products as (
  select
    pool_id,
    pool_address,
    product_id,
    coalesce(target_weight, initial_weight) as target_weight
  from staking_pool_base
),

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    sp.pool_created_time,
    sp.pool_type,
    sp.manager,
    sp.initial_pool_fee,
    sp.current_management_fee,
    sp.max_management_fee,
    spp.total_weight as leverage,
    spp.product_count
  from staking_pool_base sp
    inner join (
      select
        pool_id,
        count(distinct product_id) filter (where target_weight > 0) as product_count,
        sum(target_weight) as total_weight
      from staking_pool_products
      group by 1
    ) spp on sp.pool_id = spp.pool_id
),

staked_nxm_per_pool as (
  select
    sp.pool_id,
    sum(t.total_amount) as total_nxm_staked
  from (
      select
        pool_address,
        sum(total_amount) as total_amount
      from nexusmutual_ethereum.staking_deposit_extensions
      where is_active
      group by 1
      union all
      select
        pool_address,
        sum(amount) as total_amount
      from nexusmutual_ethereum.staking_events
      where is_active
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1
    ) t
    inner join staking_pools sp on t.pool_address = sp.pool_address
  group by 1
),

staked_nxm_allocated as (
  select
    w.pool_id,
    sum(w.target_weight * s.total_nxm_staked) as total_nxm_allocated
  from staking_pool_products w
    inner join staked_nxm_per_pool s on w.pool_id = s.pool_id
  group by 1
),

staking_pool_fee_history as (
  select
    sp.pool_id,
    sp.pool_address,
    t.start_time,
    coalesce(
      date_add('second', -1, lead(t.start_time) over (partition by t.pool_address order by t.start_time)),
      now()
    ) as end_time,
    t.pool_fee
  from staking_pools sp
    inner join (
      select
        pool_address,
        pool_created_time as start_time,
        initial_pool_fee as pool_fee
      from staking_pools
      union all
      select
        contract_address as pool_address,
        evt_block_time as start_time,
        newFee / 100.00 as pool_fee
      from nexusmutual_ethereum.StakingPool_evt_PoolFeeChanged
    ) t on sp.pool_address = t.pool_address
),

covers as (
  select
    *,
    date_diff('second', cover_start_time, cover_end_time) as cover_period_seconds
  from nexusmutual_ethereum.covers_v2
),

cover_premiums as (
  select
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.staking_pool_id,
    c.product_id,
    c.premium,
    c.commission as cover_commision,
    c.premium * 0.5 as commission,
    c.commission_ratio * c.premium * 0.5 as commission_distibutor_fee,
    c.premium * 0.5 * h.pool_fee as pool_manager_commission,
    c.premium * 0.5 * (1 - h.pool_fee) as staker_commission,
    c.premium_period_ratio * c.premium * 0.5 as commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * h.pool_fee as pool_manager_commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * (1 - h.pool_fee) as staker_commission_emitted
  from covers c
    inner join staking_pool_fee_history h on c.staking_pool_id = h.pool_id
      and c.cover_start_time between h.start_time and h.end_time
),

cover_premium_commissions as (
  select
    staking_pool_id,
    sum(commission) as total_commission,
    sum(commission_emitted) as total_commission_emitted,
    sum(commission_distibutor_fee) as pool_distributor_commission,
    sum(pool_manager_commission) as pool_manager_commission,
    sum(pool_manager_commission_emitted) as pool_manager_commission_emitted,
    sum(staker_commission) as staker_commission,
    sum(staker_commission_emitted) as staker_commission_emitted
  from cover_premiums
  group by 1
),

staking_rewards as (
  select
    mr.call_block_time as block_time,
    mr.call_block_number as block_number,
    date_trunc('day', mr.call_block_time) as block_date,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    mr.amount / 1e18 as reward_amount_expected_total,
    mr.amount / c.cover_period_seconds / 1e18 as reward_amount_per_second,
    mr.amount / c.cover_period_seconds * 86400.0 / 1e18 as reward_amount_per_day,
    mr.call_tx_hash as tx_hash
  from (
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards
      where call_success
      union all
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController2_call_mintStakingPoolNXMRewards
      where call_success
    ) mr
    inner join covers c on mr.call_tx_hash = c.tx_hash and mr.call_block_number = c.block_number
  where mr.poolId = c.staking_pool_id
    and (c.trace_address is null
      or slice(mr.call_trace_address, 1, cardinality(c.trace_address)) = c.trace_address)
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.pool_created_time) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

daily_rewards as (
  select distinct
    d.pool_id,
    d.block_date,
    sum(r.reward_amount_per_day) over (partition by d.pool_id order by d.block_date) as reward_amount_current_total,
    dense_rank() over (partition by d.pool_id order by d.block_date desc) as rn
  from staking_pool_day_sequence d
    left join staking_rewards r on d.pool_id = r.pool_id and d.block_date between date_add('day', 1, r.cover_start_date) and r.cover_end_date
),

expected_rewards as (
  select
    pool_id,
    sum(reward_amount_expected_total) as reward_amount_expected_total
  from staking_rewards
  group by 1
)

select
  sp.pool_id,
  sp.manager,
  sp.pool_type,
  sp.current_management_fee,
  sp.max_management_fee,
  sp.leverage,
  sp.product_count,
  coalesce(t.total_nxm_staked, 0) as total_nxm_staked,
  coalesce(a.total_nxm_allocated, 0) as total_nxm_allocated,
  dr.reward_amount_current_total,
  er.reward_amount_expected_total,
  c.pool_manager_commission + c.pool_distributor_commission + c.staker_commission as total_commission,
  c.pool_distributor_commission,
  c.staker_commission_emitted,
  c.staker_commission - c.staker_commission_emitted as future_staker_commission,
  c.pool_manager_commission_emitted,
  c.pool_manager_commission - c.pool_manager_commission_emitted as future_pool_manager_commission
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_allocated a on sp.pool_id = a.pool_id
  left join daily_rewards dr on sp.pool_id = dr.pool_id and dr.rn = 1
  left join expected_rewards er on sp.pool_id = er.pool_id
  left join cover_premium_commissions c on sp.pool_id = c.staking_pool_id
