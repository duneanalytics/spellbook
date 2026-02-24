{{
  config(
    tags = ['stablecoins'],
    schema = 'stablecoins',
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
from {{ ref('stablecoins_evm_balances') }}
