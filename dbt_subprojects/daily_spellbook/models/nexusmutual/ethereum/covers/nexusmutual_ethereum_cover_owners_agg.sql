{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'cover_owners_agg',
    materialized = 'view',
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
    cover_id,
    cover_start_date,
    cover_end_date,
    syndicate as staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured as cover_amount,
    premium_asset,
    premium,
    cover_owner
  from {{ ref('nexusmutual_ethereum_covers_v1') }}
  union all
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured * partial_cover_amount / sum(partial_cover_amount) over (partition by cover_id) as cover_amount,
    premium_asset,
    premium_incl_commission as premium,
    cover_owner
  from {{ ref('nexusmutual_ethereum_covers_v2') }}
  where is_migrated = false
),

covers_ext as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    if(cover_asset = 'ETH', cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', cover_amount, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', cover_amount, 0) as cbbtc_cover_amount,
    premium_asset,
    if(staking_pool = 'v1' and cover_asset = 'ETH', premium, 0) as eth_premium_amount,
    if(staking_pool = 'v1' and cover_asset = 'DAI', premium, 0) as dai_premium_amount,
    if(staking_pool <> 'v1', premium, 0) as nxm_premium_amount,
    cover_owner
  from covers
),

cover_sales_per_owner as (
  select
    p.block_date,
    c_start.cover_id,
    c_start.cover_start_date,
    c_start.cover_end_date,
    c_start.staking_pool,
    c_start.product_id,
    c_start.product_type,
    c_start.product_name,
    c_start.cover_owner,
    --== cover ==
    --ETH
    coalesce(c_start.eth_cover_amount, 0) as eth_eth_cover,
    coalesce(c_start.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_cover,
    --DAI
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover,
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_cover,
    --USDC
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover,
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_cover,
    --cbBTC
    coalesce(c_start.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price, 0) as cbbtc_eth_cover,
    coalesce(c_start.cbbtc_cover_amount * p.avg_cbbtc_usd_price, 0) as cbbtc_usd_cover,
    --== fees ==
    --ETH
    coalesce(c_start.eth_premium_amount, 0) as eth_eth_premium,
    coalesce(c_start.eth_premium_amount * p.avg_eth_usd_price, 0) as eth_usd_premium,
    --DAI
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_premium,
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price, 0) as dai_usd_premium,
    --NXM
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_premium,
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0) as nxm_usd_premium
  from daily_avg_prices p
    inner join covers_ext c_start on p.block_date = c_start.cover_start_date
),

cover_sales_per_owner_aggs as (
  select
    cover_owner,
    count(distinct cover_id) as cover_sold,
    count(distinct coalesce(product_id, -1)) as product_sold,
    min(cover_start_date) as first_cover_buy,
    max(cover_start_date) as last_cover_buy,
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
    sum(nxm_eth_premium) as nxm_eth_premium,
    sum(eth_eth_premium) + sum(dai_eth_premium) + sum(nxm_eth_premium) as eth_premium,
    approx_percentile(eth_eth_premium + dai_eth_premium + nxm_eth_premium, 0.5) as median_eth_premium,
    sum(eth_usd_premium) as eth_usd_premium,
    sum(dai_usd_premium) as dai_usd_premium,
    sum(nxm_usd_premium) as nxm_usd_premium,
    sum(eth_usd_premium) + sum(dai_usd_premium) + sum(nxm_usd_premium) as usd_premium,
    approx_percentile(eth_usd_premium + dai_usd_premium + nxm_usd_premium, 0.5) as median_usd_premium
  from cover_sales_per_owner
  group by 1
)

select
  cover_owner,
  cover_sold,
  product_sold,
  coalesce(1.00 * product_sold / nullif(cover_sold, 0), 0) as mean_product_sold,
  first_cover_buy,
  last_cover_buy,
  --== cover ==
  eth_eth_cover,
  dai_eth_cover,
  usdc_eth_cover,
  cbbtc_eth_cover,
  eth_cover,
  coalesce(eth_cover / nullif(cover_sold, 0), 0) as mean_eth_cover,
  median_eth_cover,
  eth_usd_cover,
  dai_usd_cover,
  usdc_usd_cover,
  cbbtc_usd_cover,
  usd_cover,
  coalesce(usd_cover / nullif(cover_sold, 0), 0) as mean_usd_cover,
  median_usd_cover,
  --== fees ==
  eth_eth_premium,
  dai_eth_premium,
  nxm_eth_premium,
  eth_premium,
  coalesce(eth_premium / nullif(cover_sold, 0), 0) as mean_eth_premium,
  median_eth_premium,
  eth_usd_premium,
  dai_usd_premium,
  nxm_usd_premium,
  usd_premium,
  coalesce(usd_premium / nullif(cover_sold, 0), 0) as mean_usd_premium,
  median_usd_premium
from cover_sales_per_owner_aggs
