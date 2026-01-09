{% set chains = [
    'solana',
] %}

{{
  config(
    schema = 'tokens',
    alias = 'spl_stablecoins',
    materialized = 'view',
    tags = ['static'],
  )
}}

select
  s.blockchain,
  s.token_mint_address,
  m.backing,
  coalesce(fungible.symbol, m.symbol) as symbol,
  coalesce(fungible.decimals, m.decimals) as decimals,
  m.name
from (
  {% for chain in chains %}
  select
    blockchain,
    token_mint_address
  from {{ ref('tokens_' ~ chain ~ '_spl_stablecoins') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
) s
inner join {{ source('tokens_solana', 'fungible') }} fungible
  on s.token_mint_address = fungible.token_mint_address
left join {{ ref('tokens_spl_stablecoins_metadata') }} m
  on s.blockchain = m.blockchain
  and s.token_mint_address = m.token_mint_address

