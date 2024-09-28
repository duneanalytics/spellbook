{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'active_covers',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "nexusmutual",
                                \'["tomfutago"]\') }}'
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
    cast(staking_pool as int) as staking_pool_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    sum(partial_cover_amount) over (partition by cover_id) as total_cover_amount
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
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    total_cover_amount,
    if(cover_asset = 'ETH', sum_assured * partial_cover_amount / total_cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured * partial_cover_amount / total_cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured * partial_cover_amount / total_cover_amount, 0) as usdc_cover_amount
  from covers
),

latest_eth_price as (
  select
    minute as block_date,
    price as price_usd
  from {{ source('prices', 'usd_latest') }}
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
),

latest_dai_price as (
  select
    minute as block_date,
    price as price_usd
  from {{ source('prices', 'usd_latest') }}
  where symbol = 'DAI'
    and blockchain = 'ethereum'
    and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f
),

latest_usdc_price as (
  select
    minute as block_date,
    price as price_usd
  from {{ source('prices', 'usd_latest') }}
  where symbol = 'USDC'
    and blockchain = 'ethereum'
    and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
)

select
  c.cover_id,
  c.cover_start_time,
  c.cover_end_time,
  c.cover_start_date,
  c.cover_end_date,
  c.cover_owner,
  c.staking_pool_id,
  c.product_type,
  c.product_name,
  c.cover_asset,
  c.sum_assured,
  c.partial_cover_amount,
  c.total_cover_amount,
  --ETH
  c.eth_cover_amount,
  c.eth_cover_amount * p_eth.price_usd as eth_usd_cover_amount,
  --DAI
  c.dai_cover_amount * p_dai.price_usd / p_eth.price_usd as dai_eth_cover_amount,
  c.dai_cover_amount * p_dai.price_usd as dai_usd_cover_amount,
  --USDC
  c.usdc_cover_amount * p_usdc.price_usd / p_eth.price_usd as usdc_eth_cover_amount,
  c.usdc_cover_amount * p_usdc.price_usd as usdc_usd_cover_amount
from covers_ext c
  cross join latest_eth_price p_eth
  cross join latest_dai_price p_dai
  cross join latest_usdc_price p_usdc
