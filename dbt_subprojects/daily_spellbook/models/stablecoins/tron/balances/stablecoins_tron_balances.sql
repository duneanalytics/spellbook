{% set chain = 'tron' %}

{{
  config(
    tags = ['stablecoins'],
    schema = 'stablecoins_' ~ chain,
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["tron"]\',
        "sector",
        "stablecoins_tron",
        \'["tomfutago"]\') }}'
  )
}}

select
  blockchain,
  day,
  address,
  address_varchar,
  token_symbol,
  token_address,
  contract_address,
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
  address_varchar,
  token_symbol,
  token_address,
  contract_address,
  token_standard,
  token_id,
  balance_raw,
  balance,
  balance_usd,
  currency,
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched') }}
