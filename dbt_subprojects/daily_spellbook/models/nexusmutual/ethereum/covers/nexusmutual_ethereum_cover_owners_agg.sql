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

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_owner,
    product_id,
    eth_eth_cover,
    eth_usd_cover,
    dai_eth_cover,
    dai_usd_cover,
    usdc_eth_cover,
    usdc_usd_cover,
    cbbtc_eth_cover,
    cbbtc_usd_cover,
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
  from covers
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
