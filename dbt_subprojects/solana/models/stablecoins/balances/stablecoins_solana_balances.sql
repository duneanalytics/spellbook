{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "stablecoins",
                                    \'["tomfutago"]\') }}'
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
  currency,
  balance_raw,
  balance,
  balance_usd,
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
  currency,
  balance_raw,
  balance,
  balance_usd,
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched') }}
