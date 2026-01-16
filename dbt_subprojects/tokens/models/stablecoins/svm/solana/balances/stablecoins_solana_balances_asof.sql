{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'balances_asof',
    materialized = 'view'
  )
}}

-- stablecoin balances with token metadata and USD prices
-- uses ASOF pattern for balance computation (benchmark)

with base_balances as (
  select
    blockchain,
    day,
    address,
    token_mint_address,
    balance_raw,
    last_updated
  from {{ ref('stablecoins_' ~ chain ~ '_core_balances_asof') }}
),

enriched_balances as (
  select
    b.blockchain,
    b.day,
    b.address,
    b.token_mint_address as token_address,
    'spl_token' as token_standard,
    cast(null as uint256) as token_id,
    b.balance_raw,
    cast(b.balance_raw as double) / power(10, coalesce(p.decimals, 0)) as balance,
    cast(b.balance_raw as double) / power(10, coalesce(p.decimals, 0)) * p.price as balance_usd,
    b.last_updated
  from base_balances b
  left join {{ source('prices_external', 'day') }} p
    on cast(b.day as timestamp) = p.timestamp
    and from_base58(b.token_mint_address) = p.contract_address
)

select
  b.blockchain,
  b.day,
  b.address,
  m.symbol as token_symbol,
  b.token_address,
  b.token_standard,
  b.token_id,
  m.backing as token_backing,
  m.name as token_name,
  b.balance_raw,
  b.balance,
  b.balance_usd,
  b.last_updated
from enriched_balances b
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on b.blockchain = m.blockchain
  and b.token_address = m.token_mint_address
