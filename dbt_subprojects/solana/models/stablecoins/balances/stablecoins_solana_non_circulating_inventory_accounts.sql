{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'non_circulating_inventory_accounts',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'token_account']
  )
}}

-- compatibility helper that mirrors the core-specific helper table

select
  blockchain,
  token_mint_address,
  token_account,
  source_class,
  excluded,
  observed_owners
from {{ ref('stablecoins_' ~ chain ~ '_core_non_circulating_inventory_accounts') }}
