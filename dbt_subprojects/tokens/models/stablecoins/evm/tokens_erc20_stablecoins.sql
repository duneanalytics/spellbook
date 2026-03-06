{% set chains = [
    'abstract',
    'arbitrum',
    'avalanche_c',
    'base',
    'berachain',
    'bnb',
    'bob',
    'celo',
    'ethereum',
    'fantom',
    'flare',
    'gnosis',
    'hemi',
    'hyperevm',
    'ink',
    'kaia',
    'katana',
    'linea',
    'mantle',
    'monad',
    'opbnb',
    'optimism',
    'plasma',
    'plume',
    'polygon',
    'ronin',
    'scroll',
    'sei',
    'somnia',
    'sonic',
    'story',
    'taiko',
    'unichain',
    'worldchain',
    'xlayer',
    'zksync',
] %}

{{
  config(
    schema = 'tokens',
    alias = 'erc20_stablecoins',
    materialized = 'view',
    tags = ['static'],
    post_hook = '{{ hide_spells() }}'
  )
}}

select
  s.blockchain,
  s.contract_address,
  s.currency,
  erc20.symbol,
  erc20.decimals
from (
  {% for chain in chains %}
  select
    blockchain,
    contract_address,
    currency
  from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
) s
left join {{ source('tokens', 'erc20') }} erc20
  on s.blockchain = erc20.blockchain
  and s.contract_address = erc20.contract_address
