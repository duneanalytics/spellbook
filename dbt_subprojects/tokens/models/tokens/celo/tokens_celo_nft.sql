{{ config(
        alias = 'nft'
        , materialized = 'table'
        , post_hook='{{ hide_spells() }}'
        )
}}

select
  c.contract_address,
  t.name,
  t.symbol,
  c.standard
from {{ ref('tokens_celo_nft_standards') }} c
left join {{ ref('tokens_celo_nft_curated') }} t on c.contract_address = t.contract_address
