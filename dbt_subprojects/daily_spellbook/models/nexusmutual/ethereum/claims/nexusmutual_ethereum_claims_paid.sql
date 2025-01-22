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

prices as (
  select
    date_trunc('day', minute) as block_date,
    symbol,
    avg(price) as avg_price_usd
  from {{ source('prices', 'usd') }}
  where minute > timestamp '2019-05-01'
    and ((symbol = 'ETH' and blockchain is null and contract_address is null)
      or (symbol = 'DAI' and blockchain = 'ethereum' and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f)
      or (symbol = 'USDC' and blockchain = 'ethereum' and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)
      or (symbol = 'cbBTC' and blockchain = 'ethereum' and contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf))
  group by 1, 2
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
  cp.eth_claim_amount * p.avg_price_usd as eth_usd_claim_amount,
  --DAI
  cp.dai_claim_amount * p.avg_price_usd / p.avg_price_usd as dai_eth_claim_amount,
  cp.dai_claim_amount * p.avg_price_usd as dai_usd_claim_amount,
  --USDC
  cp.usdc_claim_amount * p.avg_price_usd / p.avg_price_usd as usdc_eth_claim_amount,
  cp.usdc_claim_amount * p.avg_price_usd as usdc_usd_claim_amount,
  --cbBTC
  cp.cbbtc_claim_amount * p.avg_price_usd / p.avg_price_usd as cbbtc_eth_claim_amount,
  cp.cbbtc_claim_amount * p.avg_price_usd as cbbtc_usd_claim_amount
from claims_paid cp
  inner join prices p on coalesce(cp.claim_payout_date, cp.claim_date) = p.block_date and cp.cover_asset = p.symbol
