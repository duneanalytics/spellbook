{{
  config(
    schema = 'nexusmutual_ethereum',
    alias = 'claims_paid',
    materialized = 'view',
    unique_key = ['version', 'claim_id'],
    post_hook = '{{ expose_spells(blockchains = \'["ethereum"]\',
                                  spell_type = "project",
                                  spell_name = "nexusmutual",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

covers as (
  select
    1 as version,
    cover_id,
    syndicate as staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured
  from {{ ref('nexusmutual_ethereum_covers_v1') }}
  union all
  select distinct
    2 as version,
    cover_id,
    staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured
  from {{ ref('nexusmutual_ethereum_covers_v2') }}
),

claims as (
  select
    1 as version,
    claim_id,
    cover_id,
    cast(null as int) as product_id,
    submit_date as claim_date,
    partial_claim_amount as claim_amount
  from {{ ref('nexusmutual_ethereum_claims_v1') }}
  where claim_status = 14
    or claim_id = 102
  union all
  select
    2 as version,
    claim_id,
    cover_id,
    cast(product_id as int) as product_id,
    submit_date as claim_date,
    requested_amount as claim_amount
  from {{ ref('nexusmutual_ethereum_claims_v2') }}
),

claims_paid as (
  select
    cl.version,
    cl.cover_id,
    cl.claim_id,
    cl.claim_date,
    cp.claim_payout_date,
    c.product_type,
    c.product_name,
    c.cover_asset,
    coalesce(cl.claim_amount, c.sum_assured) as claim_amount,
    if(c.cover_asset = 'ETH', coalesce(cl.claim_amount, c.sum_assured), 0) as eth_claim_amount,
    if(c.cover_asset = 'DAI', coalesce(cl.claim_amount, c.sum_assured), 0) as dai_claim_amount,
    if(c.cover_asset = 'USDC', coalesce(cl.claim_amount, c.sum_assured), 0) as usdc_claim_amount,
    if(c.cover_asset = 'cbBTC', coalesce(cl.claim_amount, c.sum_assured), 0) as cbbtc_claim_amount
  from covers c
    inner join claims cl on c.cover_id = cl.cover_id
      and coalesce(c.product_id, cl.product_id, -1) = coalesce(cl.product_id, -1)
      and c.version = cl.version
    left join (
        select
          claimId as claim_id,
          date_trunc('day', call_block_time) as claim_payout_date,
          row_number() over (partition by call_block_time, call_tx_hash, claimId order by call_trace_address desc) as rn
        from {{ source('nexusmutual_ethereum', 'IndividualClaims_call_redeemClaimPayout') }}
        where call_success
      ) cp on cl.claim_id = cp.claim_id and cl.version = 2 and cp.rn = 1
  where cl.version = 1
    or (cl.version = 2 and cp.claim_id is not null)
),

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
)

select
  cp.version,
  cp.cover_id,
  cp.claim_id,
  cp.claim_date,
  cp.claim_payout_date,
  cp.product_type,
  cp.product_name,
  --ETH
  cp.eth_claim_amount as eth_eth_claim_amount,
  cp.eth_claim_amount * p.avg_eth_usd_price as eth_usd_claim_amount,
  --DAI
  cp.dai_claim_amount * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_claim_amount,
  cp.dai_claim_amount * p.avg_dai_usd_price as dai_usd_claim_amount,
  --USDC
  cp.usdc_claim_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_claim_amount,
  cp.usdc_claim_amount * p.avg_usdc_usd_price as usdc_usd_claim_amount,
  --cbBTC
  cp.cbbtc_claim_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_claim_amount,
  cp.cbbtc_claim_amount * p.avg_cbbtc_usd_price as cbbtc_usd_claim_amount
from claims_paid cp
  inner join daily_avg_prices p on coalesce(cp.claim_payout_date, cp.claim_date) = p.block_date
