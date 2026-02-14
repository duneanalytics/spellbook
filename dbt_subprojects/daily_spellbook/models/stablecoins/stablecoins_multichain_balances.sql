{{
  config(
    schema = 'stablecoins_multichain',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
  )
}}

select
  blockchain,
  day,
  cast(address as varchar) as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_evm_balances') }}
union all
select
  blockchain,
  day,
  cast(address as varchar) as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_svm_balances') }}
union all
select
  blockchain,
  day,
  cast(address as varchar) as address,
  token_symbol,
  cast(token_address as varchar) as token_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_tron_balances') }}
