{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'non_circulating_inventory_owners',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'owner_address']
  )
}}

-- non-circulating inventory owner wallets for spl stablecoins on solana (core + extended)
-- approach: curate known non-circulating owner addresses inline via values()
-- any token account whose owner appears here is treated as non-circulating
-- complements the token-account-keyed inventory list for cases where the owner
-- holds balances across multiple token accounts (e.g. Circle treasury wallets)
-- ref: https://github.com/DefiLlama/peggedassets-server/blob/master/src/adapters/peggedAssets/usd-coin/config.ts

with seeded_rows as (
  select token_mint_address, owner_address, source_class
  from (
    values
      -- usdc: circle treasury / mint wallets (also tracked by DefiLlama as non-circulating)
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE', 'circle_treasury'),
      -- usdc: additional non-circulating owners tracked by DefiLlama
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '41zCUJsKk6cMB94DDtm99qWmyMZfp4GkAhhuz4xTwePu', 'defillama_excluded'),
      ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', '42qwJUTbKf3D8ULfWadUSjnHf6pkJ4H1VjCcfSKHvDTN', 'defillama_excluded')
  ) as t(token_mint_address, owner_address, source_class)
),

stablecoin_mints as (
  select token_mint_address
  from {{ ref('tokens_' ~ chain ~ '_spl_stablecoins') }}
)

-- defensive filter: seed entries must reference a known stablecoin mint
select
  '{{ chain }}' as blockchain,
  s.token_mint_address,
  s.owner_address,
  s.source_class,
  true as excluded
from seeded_rows as s
inner join stablecoin_mints as m
  on m.token_mint_address = s.token_mint_address
