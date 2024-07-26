{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'capital_pool_prices',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
  )
}}

with

capital_pool as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_capital_pool_eth_total  
  from {{ ref('nexusmutual_ethereum_capital_pool_totals') }}
),

mcr as (
  select
    block_date,
    mcr_eth_total
  from {{ ref('nexusmutual_ethereum_capital_pool_mcr') }}
),

nxm_daily_price_pre_ramm as (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_dai_usd_price,
    cp.avg_usdc_usd_price,
    cp.avg_capital_pool_eth_total,
    mcr.mcr_eth_total,
    cast(
      0.01028 + (mcr.mcr_eth_total / 5800000) * power((cp.avg_capital_pool_eth_total / mcr.mcr_eth_total), 4)
    as double) as avg_nxm_eth_price,
    cast(
      0.01028 + (mcr.mcr_eth_total / 5800000) * power((cp.avg_capital_pool_eth_total / mcr.mcr_eth_total), 4)
    as double) * cp.avg_eth_usd_price as avg_nxm_usd_price
  from capital_pool cp
    left join mcr on cp.block_date = mcr.block_date
  where cp.block_date < timestamp '2023-11-21'
),

nxm_daily_internal_price_avgs AS (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_dai_usd_price,
    cp.avg_usdc_usd_price,
    avg(cast(ramm.output_internalPrice as double)) / 1e18 as avg_nxm_eth_price
  from capital_pool cp -- just to pull already calculated avg usd prices
    left join {{ source('nexusmutual_ethereum', 'Ramm_call_getInternalPriceAndUpdateTwap') }} ramm on cp.block_date = date_trunc('day', ramm.call_block_time)
  where cp.block_date >= timestamp '2023-11-21'
  group by 1, 2, 3, 4
),

nxm_filled_null_cnts as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    count(avg_nxm_eth_price) over (order by block_date) as avg_nxm_eth_price_count
  from nxm_daily_internal_price_avgs
),

nxm_daily_price_post_ramm as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    first_value(avg_nxm_eth_price) over (partition by avg_nxm_eth_price_count order by block_date) as avg_nxm_eth_price
  from nxm_filled_null_cnts
),

nxm_daily_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from nxm_daily_price_pre_ramm
  union all
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_eth_price * avg_eth_usd_price as avg_nxm_usd_price
  from nxm_daily_price_post_ramm
)

select 
  block_date,
  avg_eth_usd_price,
  avg_dai_usd_price,
  avg_usdc_usd_price,
  coalesce(avg_nxm_eth_price, lag(avg_nxm_eth_price) over (order by block_date)) as avg_nxm_eth_price,
  coalesce(avg_nxm_usd_price, lag(avg_nxm_usd_price) over (order by block_date)) as avg_nxm_usd_price
from nxm_daily_prices
