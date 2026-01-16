{% set chain = 'arbitrum' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'balances_test',
    materialized = 'view'
  )
}}

-- union of core and extended enriched balances (test)

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
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_core_balances_enriched_test') }}

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
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched_test') }}
