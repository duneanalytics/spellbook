{% set chain = 'solana' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'spl_stablecoins',
    materialized = 'view',
    tags = ['static'],
    unique_key = ['token_mint_address']
  )
}}

-- union view combining core (frozen) and extended (new additions) stablecoin lists

select blockchain, token_mint_address, currency
from {{ ref('tokens_' ~ chain ~ '_spl_stablecoins_core') }}

union all

select blockchain, token_mint_address, currency
from {{ ref('tokens_' ~ chain ~ '_spl_stablecoins_extended') }}
