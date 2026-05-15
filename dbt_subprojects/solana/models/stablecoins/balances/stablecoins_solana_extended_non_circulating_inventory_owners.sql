{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_non_circulating_inventory_owners',
    materialized = 'table',
    file_format = 'delta',
    tags = ['static'],
    unique_key = ['token_mint_address', 'owner_address']
  )
}}

-- non-circulating inventory owner wallets for spl stablecoins on solana extended lineage
-- approach: curate known non-circulating owner addresses inline via values() (currently none)
-- any token account whose owner appears here is treated as non-circulating

with owners as (
  select token_mint_address, owner_address, source_class
  from (
    values
      (cast(null as varchar), cast(null as varchar), cast(null as varchar))
  ) as t(token_mint_address, owner_address, source_class)
  where false
)

select
  '{{ chain }}' as blockchain,
  o.token_mint_address,
  o.owner_address,
  o.source_class,
  true as excluded
from owners as o
inner join {{ ref('tokens_' ~ chain ~ '_spl_stablecoins_extended') }} as s
  on s.token_mint_address = o.token_mint_address
