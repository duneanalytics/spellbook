{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'covers_full_list',
    materialized = 'view',
    unique_key = ['version', 'cover_id', 'staking_pool'],
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

covers_combined as (
  select
    block_time,
    block_date,
    block_number,
    1 as version,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    syndicate as staking_pool,
    cast(null as int) as product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured as cover_amount,
    premium_asset,
    premium,
    cover_owner,
    false as is_migrated,
    tx_hash
  from {{ ref('nexusmutual_ethereum_covers_v1') }}
  union all
  select
    block_time,
    block_date,
    block_number,
    2 as version,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    staking_pool,
    cast(product_id as int) as product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured * partial_cover_amount / sum(partial_cover_amount) over (partition by cover_id) as cover_amount,
    premium_asset,
    premium_incl_commission as premium,
    cover_owner,
    is_migrated,
    tx_hash
  from {{ ref('nexusmutual_ethereum_covers_v2') }}
),

covers_ext as (
  select
    block_time,
    block_date,
    block_number,
    version,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    date_diff('day', cover_start_date, cover_end_date) as cover_period,
    staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    cover_amount,
    if(cover_asset = 'ETH', cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', cover_amount, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', cover_amount, 0) as cbbtc_cover_amount,
    premium_asset,
    premium,
    if(staking_pool = 'v1' and cover_asset = 'ETH', premium, 0) as eth_premium_amount,
    if(staking_pool = 'v1' and cover_asset = 'DAI', premium, 0) as dai_premium_amount,
    if(staking_pool <> 'v1', premium, 0) as nxm_premium_amount,
    cover_owner,
    is_migrated,
    tx_hash
  from covers_combined
),

covers_base as (
  select
    c.block_time,
    c.block_date,
    c.block_number,
    c.version,
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.cover_start_date,
    c.cover_end_date,
    c.is_migrated,
    c.cover_period,
    c.staking_pool,
    c.product_id,
    c.product_type,
    c.product_name,
    c.cover_owner,
    c.cover_asset,
    c.sum_assured,
    c.cover_amount,
    c.eth_cover_amount,
    coalesce(c.eth_cover_amount, 0) as eth_eth_cover,
    coalesce(c.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_cover,
    c.dai_cover_amount,
    coalesce(c.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover,
    coalesce(c.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_cover,
    c.usdc_cover_amount,
    coalesce(c.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover,
    coalesce(c.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_cover,
    c.cbbtc_cover_amount,
    coalesce(c.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price, 0) as cbbtc_eth_cover,
    coalesce(c.cbbtc_cover_amount * p.avg_cbbtc_usd_price, 0) as cbbtc_usd_cover,
    c.premium_asset,
    c.premium,
    c.eth_premium_amount,
    c.dai_premium_amount,
    c.nxm_premium_amount,
    --ETH
    case
      when c.staking_pool = 'v1' and c.premium_asset = 'ETH' then coalesce(c.eth_premium_amount, 0)
      when c.premium_asset = 'ETH' then coalesce(c.nxm_premium_amount * p.avg_nxm_eth_price, 0)
      else 0
    end as eth_eth_premium,
    case
      when c.staking_pool = 'v1' and c.premium_asset = 'ETH' then coalesce(c.eth_premium_amount * p.avg_eth_usd_price, 0)
      when c.premium_asset = 'ETH' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price, 0)
      else 0
    end as eth_usd_premium,
    --DAI
    case
      when c.staking_pool = 'v1' and c.premium_asset = 'DAI' then coalesce(c.dai_premium_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0)
      when c.premium_asset = 'DAI' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as dai_eth_premium,
    case
      when c.staking_pool = 'v1' and c.premium_asset = 'DAI' then coalesce(c.dai_premium_amount * p.avg_dai_usd_price, 0)
      when c.premium_asset = 'DAI' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price, 0)
      else 0
    end as dai_usd_premium,
    --USDC
    case
      when c.premium_asset = 'USDC' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as usdc_eth_premium,
    case
      when c.premium_asset = 'USDC' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price, 0)
      else 0
    end as usdc_usd_premium,
    --cbBTC
    case
      when c.premium_asset = 'cbBTC' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as cbbtc_eth_premium,
    case
      when c.premium_asset = 'cbBTC' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price, 0)
      else 0
    end as cbbtc_usd_premium,
    --NXM
    case
      when c.premium_asset = 'NXM' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)
      else 0
    end as nxm_eth_premium,
    case
      when c.premium_asset = 'NXM' then coalesce(c.nxm_premium_amount * p.avg_nxm_usd_price, 0)
      else 0
    end as nxm_usd_premium,
    c.tx_hash
  from daily_avg_prices p
    inner join covers_ext c on p.block_date = c.cover_start_date
)

select
  block_time,
  block_date,
  block_number,
  --== cover ==
  version,
  cover_id,
  cover_start_time,
  cover_end_time,
  cover_start_date,
  cover_end_date,
  is_migrated,
  cover_period,
  staking_pool,
  product_id,
  product_type,
  product_name,
  cover_owner,
  cover_asset,
  sum_assured,
  cover_amount,
  tx_hash,
  --== cover amount per asset ==
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
  --== premium ==
  premium_asset,
  premium,
  eth_premium_amount,
  dai_premium_amount,
  nxm_premium_amount,
  --== premium per asset ==
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
from covers_base
