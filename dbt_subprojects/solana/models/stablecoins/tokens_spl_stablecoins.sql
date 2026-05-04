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
  s.currency,
  fungible.symbol,
  fungible.decimals
from (
  {% for chain in chains %}
  select
    blockchain,
    token_mint_address,
    currency
  from {{ ref('tokens_' ~ chain ~ '_spl_stablecoins') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
) s
inner join {{ source('tokens_solana', 'fungible') }} fungible
  on s.token_mint_address = fungible.token_mint_address

