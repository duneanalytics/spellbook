{% set chain = 'linea' %}

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

-- union of seed and latest enriched balances

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
  last_updated
from {{ ref('stablecoins_' ~ chain ~ '_extended_balances_enriched') }}
