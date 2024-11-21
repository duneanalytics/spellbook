{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'active_covers',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    cover_owner,
    staking_pool_id,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    sum(partial_cover_amount) over (partition by cover_id) as total_cover_amount,
    premium_incl_commission as premium_nxm
  from {{ ref('nexusmutual_ethereum_covers_v2') }}
  where cover_end_time >= now()
),

covers_ext as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    cover_owner,
    staking_pool_id,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    total_cover_amount,
    premium_nxm,
    if(cover_asset = 'ETH', sum_assured * partial_cover_amount / total_cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured * partial_cover_amount / total_cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured * partial_cover_amount / total_cover_amount, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured * partial_cover_amount / total_cover_amount, 0) as cbbtc_cover_amount
  from covers
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_eth_usd_price, block_date) as avg_eth_usd_price,
    max_by(avg_dai_usd_price, block_date) as avg_dai_usd_price,
    max_by(avg_usdc_usd_price, block_date) as avg_usdc_usd_price,
    max_by(avg_cbbtc_usd_price, block_date) as avg_cbbtc_usd_price,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  from {{ ref('nexusmutual_ethereum_capital_pool_prices') }}
)

select
  c.cover_id,
  c.cover_start_time,
  c.cover_end_time,
  c.cover_start_date,
  c.cover_end_date,
  c.cover_owner,
  c.staking_pool_id,
  c.product_id,
  c.product_type,
  c.product_name,
  c.cover_asset,
  c.sum_assured,
  c.partial_cover_amount,
  c.total_cover_amount,
  --ETH
  c.eth_cover_amount,
  c.eth_cover_amount * p.avg_eth_usd_price as eth_usd_cover_amount,
  --DAI
  c.dai_cover_amount,
  c.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_amount,
  c.dai_cover_amount * p.avg_dai_usd_price as dai_usd_cover_amount,
  --USDC
  c.usdc_cover_amount,
  c.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_amount,
  c.usdc_cover_amount * p.avg_usdc_usd_price as usdc_usd_cover_amount,
  --cbBTC
  c.cbbtc_cover_amount,
  c.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_cover_amount,
  c.cbbtc_cover_amount * p.avg_cbbtc_usd_price as cbbtc_usd_cover_amount,
  --NXM fees
  c.premium_nxm,
  c.premium_nxm * p.avg_nxm_eth_price as premium_nxm_eth,
  c.premium_nxm * p.avg_nxm_usd_price as premium_nxm_usd
from covers_ext c
  cross join latest_prices p
