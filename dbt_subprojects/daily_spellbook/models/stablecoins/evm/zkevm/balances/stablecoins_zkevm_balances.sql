{% set chain = 'zkevm' %}

{{
  config(
    tags = ['prod_exclude', 'stablecoins'],
    schema = 'stablecoins_' ~ chain,
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
  )
}}

select
  blockchain,
  day,
  address,
  token_symbol,
  token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_core_balances_enriched') }}
union all
select
  blockchain,
  day,
  address,
  token_symbol,
  token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched') }}
