{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_mcr',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as avg_eth_usd_price
  from {{ source('prices', 'usd') }}
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-11-06'
  group by 1
),

mcr_events as (
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcrEtherx100 as double) / 1e18 as mcr_eth,
    cast(7000 as double) as mcr_floor,
    cast(0 as double) as mcr_cover_min
  from {{ source('nexusmutual_ethereum', 'MCR_evt_MCREvent') }}
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcr as double) / 1e18 as mcr_eth,
    cast(mcrFloor as double) / 1e18 as mcr_floor,
    cast(mcrETHWithGear as double) / 1e18 as mcr_cover_min
  from {{ source('nexusmutual_ethereum', 'MCR_evt_MCRUpdated') }}
),

mcr_daily_avgs as (
  select
    p.block_date,
    p.avg_eth_usd_price,
    avg(me.mcr_eth) as mcr_eth,
    avg(me.mcr_floor) as mcr_floor,
    avg(me.mcr_cover_min) as mcr_cover_min
  from daily_avg_eth_prices p
    left join mcr_events me on p.block_date = me.block_date
  group by 1, 2
),

mcr_filled_null_cnts as (
  select
    block_date,
    avg_eth_usd_price,
    mcr_eth,
    mcr_floor,
    mcr_cover_min,
    count(mcr_eth) over (order by block_date) as mcr_eth_count,
    count(mcr_floor) over (order by block_date) as mcr_floor_count,
    count(mcr_cover_min) over (order by block_date) as mcr_cover_min_count
  from mcr_daily_avgs
),

mcr_daily_totals as (
  select
    block_date,
    avg_eth_usd_price,
    first_value(mcr_eth) over (partition by mcr_eth_count order by block_date) as mcr_eth_total,
    first_value(mcr_floor) over (partition by mcr_floor_count order by block_date) as mcr_floor_total,
    first_value(mcr_cover_min) over (partition by mcr_cover_min_count order by block_date) as mcr_cover_min_total
  from mcr_filled_null_cnts
)

select
  block_date,
  avg_eth_usd_price,
  mcr_eth_total,
  mcr_floor_total,
  mcr_cover_min_total
from mcr_daily_totals
