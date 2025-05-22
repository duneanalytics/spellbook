{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'covers_daily_agg',
    materialized = 'view',
    unique_key = ['block_date'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from {{ ref('nexusmutual_ethereum_capital_pool_prices') }}
),

covers as (
  select
    block_date,
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_period,
    staking_pool,
    eth_cover_amount,
    dai_cover_amount,
    usdc_cover_amount,
    cbbtc_cover_amount,
    eth_eth_cover,
    eth_usd_cover,
    dai_eth_cover,
    dai_usd_cover,
    usdc_eth_cover,
    usdc_usd_cover,
    cbbtc_eth_cover,
    cbbtc_usd_cover,
    premium_asset,
    eth_premium_amount,
    dai_premium_amount,
    nxm_premium_amount,
    eth_eth_premium,
    eth_usd_premium,
    dai_eth_premium,
    dai_usd_premium,
    usdc_eth_premium,
    usdc_usd_premium,
    cbbtc_eth_premium,
    cbbtc_usd_premium,
    nxm_eth_premium,
    nxm_usd_premium
  from {{ ref('nexusmutual_ethereum_covers_full_list') }}
  where is_migrated = false
),

day_sequence as (
  select timestamp as block_date
  from {{ source('utils', 'days') }}
  where timestamp >= (select min(block_date) from covers)
),

daily_active_cover as (
  select
    ds.block_date,
    c_period.cover_id,
    c_period.cover_period,
    --== cover ==
    --ETH
    coalesce(c_period.eth_cover_amount, 0) as eth_eth_active_cover,
    coalesce(c_period.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_active_cover,
    --DAI
    coalesce(c_period.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_active_cover,
    coalesce(c_period.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_active_cover,
    --USDC
    coalesce(c_period.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_active_cover,
    coalesce(c_period.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_active_cover,
    --cbBTC
    coalesce(c_period.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price, 0) as cbbtc_eth_active_cover,
    coalesce(c_period.cbbtc_cover_amount * p.avg_cbbtc_usd_price, 0) as cbbtc_usd_active_cover,
    --== active premium in force ==
    --ETH
    case
      when c_period.staking_pool = 'v1' then coalesce(c_period.eth_premium_amount * 365 / c_period.cover_period, 0)
      when c_period.premium_asset = 'ETH' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as eth_eth_active_premium,
    case
      when c_period.staking_pool = 'v1' then coalesce(c_period.eth_premium_amount * 365 / c_period.cover_period * p.avg_eth_usd_price, 0)
      when c_period.premium_asset = 'ETH' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)
      else 0
    end as eth_usd_active_premium,
    --DAI
    case
      when c_period.staking_pool = 'v1' then coalesce(c_period.dai_premium_amount * 365 / c_period.cover_period * p.avg_dai_usd_price / p.avg_eth_usd_price, 0)
      when c_period.premium_asset = 'DAI' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as dai_eth_active_premium,
    case
      when c_period.staking_pool = 'v1' then coalesce(c_period.dai_premium_amount * 365 / c_period.cover_period * p.avg_dai_usd_price, 0)
      when c_period.premium_asset = 'DAI' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)
      else 0
    end as dai_usd_active_premium,
    --USDC
    case
      when c_period.premium_asset = 'USDC' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as usdc_eth_active_premium,
    case
      when c_period.premium_asset = 'USDC' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)
      else 0
    end as usdc_usd_active_premium,
    --cbBTC
    case
      when c_period.premium_asset = 'cbBTC' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as cbbtc_eth_active_premium,
    case
      when c_period.premium_asset = 'cbBTC' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)
      else 0
    end as cbbtc_usd_active_premium,
    --NXM
    case
      when c_period.premium_asset = 'NXM' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as nxm_eth_active_premium,
    case
      when c_period.premium_asset = 'NXM' then coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)
      else 0
    end as nxm_usd_active_premium
  from day_sequence ds
    inner join daily_avg_prices p on ds.block_date = p.block_date
    left join covers c_period on ds.block_date between c_period.cover_start_date and c_period.cover_end_date
),

daily_active_cover_aggs as (
  select
    block_date,
    count(distinct cover_id) as active_cover,
    --== cover ==
    sum(eth_eth_active_cover) as eth_eth_active_cover,
    sum(dai_eth_active_cover) as dai_eth_active_cover,
    sum(usdc_eth_active_cover) as usdc_eth_active_cover,
    sum(cbbtc_eth_active_cover) as cbbtc_eth_active_cover,
    sum(eth_eth_active_cover) + sum(dai_eth_active_cover) + sum(usdc_eth_active_cover) + sum(cbbtc_eth_active_cover) as eth_active_cover,
    approx_percentile(eth_eth_active_cover + dai_eth_active_cover + usdc_eth_active_cover + cbbtc_eth_active_cover, 0.5) as median_eth_active_cover,
    sum(eth_usd_active_cover) as eth_usd_active_cover,
    sum(dai_usd_active_cover) as dai_usd_active_cover,
    sum(usdc_usd_active_cover) as usdc_usd_active_cover,
    sum(cbbtc_usd_active_cover) as cbbtc_usd_active_cover,
    sum(eth_usd_active_cover) + sum(dai_usd_active_cover) + sum(usdc_usd_active_cover) + sum(cbbtc_usd_active_cover) as usd_active_cover,
    approx_percentile(eth_usd_active_cover + dai_usd_active_cover + usdc_usd_active_cover + cbbtc_usd_active_cover, 0.5) as median_usd_active_cover,
    --== fees ==
    sum(eth_eth_active_premium) as eth_eth_active_premium,
    sum(dai_eth_active_premium) as dai_eth_active_premium,
    sum(usdc_eth_active_premium) as usdc_eth_active_premium,
    sum(cbbtc_eth_active_premium) as cbbtc_eth_active_premium,
    sum(nxm_eth_active_premium) as nxm_eth_active_premium,
    sum(eth_eth_active_premium) + sum(dai_eth_active_premium) + sum(usdc_eth_active_premium) + sum(cbbtc_eth_active_premium) + sum(nxm_eth_active_premium) as eth_active_premium,
    approx_percentile(eth_eth_active_premium + dai_eth_active_premium + usdc_eth_active_premium + cbbtc_eth_active_premium + nxm_eth_active_premium, 0.5) as median_eth_active_premium,
    sum(eth_usd_active_premium) as eth_usd_active_premium,
    sum(dai_usd_active_premium) as dai_usd_active_premium,
    sum(usdc_usd_active_premium) as usdc_usd_active_premium,
    sum(cbbtc_usd_active_premium) as cbbtc_usd_active_premium,
    sum(nxm_usd_active_premium) as nxm_usd_active_premium,
    sum(eth_usd_active_premium) + sum(dai_usd_active_premium) + sum(usdc_usd_active_premium) + sum(cbbtc_usd_active_premium) + sum(nxm_usd_active_premium) as usd_active_premium,
    approx_percentile(eth_usd_active_premium + dai_usd_active_premium + usdc_usd_active_premium + cbbtc_usd_active_premium + nxm_usd_active_premium, 0.5) as median_usd_active_premium
  from daily_active_cover
  group by 1
),

daily_cover_sales_aggs as (
  select
    block_date,
    count(distinct cover_id) as cover_sold,
    --== cover ==
    sum(eth_eth_cover) as eth_eth_cover,
    sum(dai_eth_cover) as dai_eth_cover,
    sum(usdc_eth_cover) as usdc_eth_cover,
    sum(cbbtc_eth_cover) as cbbtc_eth_cover,
    sum(eth_eth_cover) + sum(dai_eth_cover) + sum(usdc_eth_cover) + sum(cbbtc_eth_cover) as eth_cover,
    approx_percentile(eth_eth_cover + dai_eth_cover + usdc_eth_cover + cbbtc_eth_cover, 0.5) as median_eth_cover,
    sum(eth_usd_cover) as eth_usd_cover,
    sum(dai_usd_cover) as dai_usd_cover,
    sum(usdc_usd_cover) as usdc_usd_cover,
    sum(cbbtc_usd_cover) as cbbtc_usd_cover,
    sum(eth_usd_cover) + sum(dai_usd_cover) + sum(usdc_usd_cover) + sum(cbbtc_usd_cover) as usd_cover,
    approx_percentile(eth_usd_cover + dai_usd_cover + usdc_usd_cover + cbbtc_usd_cover, 0.5) as median_usd_cover,
    --== fees ==
    sum(eth_eth_premium) as eth_eth_premium,
    sum(dai_eth_premium) as dai_eth_premium,
    sum(usdc_eth_premium) as usdc_eth_premium,
    sum(cbbtc_eth_premium) as cbbtc_eth_premium,
    sum(nxm_eth_premium) as nxm_eth_premium,
    sum(eth_eth_premium) + sum(dai_eth_premium) + sum(usdc_eth_premium) + sum(cbbtc_eth_premium) + sum(nxm_eth_premium) as eth_premium,
    approx_percentile(eth_eth_premium + dai_eth_premium + usdc_eth_premium + cbbtc_eth_premium + nxm_eth_premium, 0.5) as median_eth_premium,
    sum(eth_usd_premium) as eth_usd_premium,
    sum(dai_usd_premium) as dai_usd_premium,
    sum(usdc_usd_premium) as usdc_usd_premium,
    sum(cbbtc_usd_premium) as cbbtc_usd_premium,
    sum(nxm_usd_premium) as nxm_usd_premium,
    sum(eth_usd_premium) + sum(dai_usd_premium) + sum(usdc_usd_premium) + sum(cbbtc_usd_premium) + sum(nxm_usd_premium) as usd_premium,
    approx_percentile(eth_usd_premium + dai_usd_premium + usdc_usd_premium + cbbtc_usd_premium + nxm_usd_premium, 0.5) as median_usd_premium
  from covers
  group by 1
)

select
  ac.block_date,
  --**** ACTIVE COVER ****
  ac.active_cover,
  ac.eth_eth_active_cover,
  ac.dai_eth_active_cover,
  ac.usdc_eth_active_cover,
  ac.cbbtc_eth_active_cover,
  ac.eth_active_cover,
  ac.median_eth_active_cover,
  ac.eth_usd_active_cover,
  ac.dai_usd_active_cover,
  ac.usdc_usd_active_cover,
  ac.cbbtc_usd_active_cover,
  ac.usd_active_cover,
  ac.median_usd_active_cover,
  ac.eth_eth_active_premium,
  ac.dai_eth_active_premium,
  ac.usdc_eth_active_premium,
  ac.cbbtc_eth_active_premium,
  ac.nxm_eth_active_premium,
  ac.eth_active_premium,
  ac.median_eth_active_premium,
  ac.eth_usd_active_premium,
  ac.dai_usd_active_premium,
  ac.usdc_usd_active_premium,
  ac.cbbtc_usd_active_premium,
  ac.nxm_usd_active_premium,
  ac.usd_active_premium,
  ac.median_usd_active_premium,
  --**** COVER SALES ****
  coalesce(cs.cover_sold, 0) as cover_sold,
  coalesce(cs.eth_eth_cover, 0) as eth_eth_cover,
  coalesce(cs.dai_eth_cover, 0) as dai_eth_cover,
  coalesce(cs.usdc_eth_cover, 0) as usdc_eth_cover,
  coalesce(cs.cbbtc_eth_cover, 0) as cbbtc_eth_cover,
  coalesce(cs.eth_cover, 0) as eth_cover,
  coalesce(cs.median_eth_cover, 0) as median_eth_cover,
  coalesce(cs.eth_usd_cover, 0) as eth_usd_cover,
  coalesce(cs.dai_usd_cover, 0) as dai_usd_cover,
  coalesce(cs.usdc_usd_cover, 0) as usdc_usd_cover,
  coalesce(cs.cbbtc_usd_cover, 0) as cbbtc_usd_cover,
  coalesce(cs.usd_cover, 0) as usd_cover,
  coalesce(cs.median_usd_cover, 0) as median_usd_cover,
  coalesce(cs.eth_eth_premium, 0) as eth_eth_premium,
  coalesce(cs.dai_eth_premium, 0) as dai_eth_premium,
  coalesce(cs.usdc_eth_premium, 0) as usdc_eth_premium,
  coalesce(cs.cbbtc_eth_premium, 0) as cbbtc_eth_premium,
  coalesce(cs.nxm_eth_premium, 0) as nxm_eth_premium,
  coalesce(cs.eth_premium, 0) as eth_premium,
  coalesce(cs.median_eth_premium, 0) as median_eth_premium,
  coalesce(cs.eth_usd_premium, 0) as eth_usd_premium,
  coalesce(cs.dai_usd_premium, 0) as dai_usd_premium,
  coalesce(cs.usdc_usd_premium, 0) as usdc_usd_premium,
  coalesce(cs.cbbtc_usd_premium, 0) as cbbtc_usd_premium,
  coalesce(cs.nxm_usd_premium, 0) as nxm_usd_premium,
  coalesce(cs.usd_premium, 0) as usd_premium,
  coalesce(cs.median_usd_premium, 0) as median_usd_premium
from daily_active_cover_aggs ac
  left join daily_cover_sales_aggs cs on ac.block_date = cs.block_date
