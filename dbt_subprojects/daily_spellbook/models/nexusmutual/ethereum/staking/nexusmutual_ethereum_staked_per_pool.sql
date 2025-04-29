{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'staked_per_pool',
    materialized = 'view',
    unique_key = ['block_date', 'pool_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  from {{ ref('nexusmutual_ethereum_staking_pools') }} sp
    inner join (
      select
        pool_address,
        cast(min(block_time) as date) as first_stake_event_date
      from {{ ref('nexusmutual_ethereum_staking_events') }}
      group by 1
    ) se on sp.pool_address = se.pool_address
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.first_stake_event_date) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    dense_rank() over (partition by pool_id order by block_date desc) as pool_date_rn
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join {{ ref('nexusmutual_ethereum_staking_deposit_extensions') }} se
          on d.pool_address = se.pool_address
         and d.block_date >= se.stake_start_date
         and d.block_date < se.stake_end_date
      group by 1, 2, 3
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join {{ ref('nexusmutual_ethereum_staking_events') }} se
          on d.pool_address = se.pool_address
         and d.block_date >= date_trunc('day', se.block_time)
         and d.block_date < coalesce(se.tranche_expiry_date, current_date)
      where flow_type in ('withdraw', 'stake burn')
      group by 1, 2, 3
    ) t
  group by 1, 2, 3
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from staked_nxm_per_pool
