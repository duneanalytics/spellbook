{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chain ~ '"]\',
                                 spell_type = "sector",
                                 spell_name = "stablecoins",
                                 contributors = \'["tomfutago"]\') }}'
  )
}}

-- union of core and extended enriched balances with token metadata

with enriched_balances as (
  select
    blockchain,
    day,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    balance,
    balance_usd,
    last_updated,
  from {{ ref('stablecoins_' ~ chain ~ '_core_balances_enriched') }}
  union all
  select
    blockchain,
    day,
    address,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    balance,
    balance_usd,
    last_updated,
  from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched') }}
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
  b.last_updated,
from enriched_balances b
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on b.blockchain = m.blockchain
  and b.token_address = m.token_mint_address
