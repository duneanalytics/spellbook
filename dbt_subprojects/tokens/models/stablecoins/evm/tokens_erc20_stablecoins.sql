{% set chains = [
    'abstract',
    'apechain',
    'arbitrum',
    'avalanche_c',
    'b3',
    'base',
    'berachain',
    'bnb',
    'bob',
    'boba',
    'celo',
    'corn',
    'degen',
    'ethereum',
    'fantom',
    'flare',
    'flow',
    'gnosis',
    'hemi',
    'henesys',
    'hyperevm',
    'ink',
    'kaia',
    'katana',
    'linea',
    'mantle',
    'megaeth',
    'monad',
    'nova',
    'opbnb',
    'optimism',
    'peaq',
    'plasma',
    'plume',
    'polygon',
    'ronin',
    'scroll',
    'sei',
    'sepolia',
    'shape',
    'somnia',
    'sonic',
    'sophon',
    'story',
    'superseed',
    'tac',
    'taiko',
    'tron',
    'unichain',
    'viction',
    'worldchain',
    'xlayer',
    'zkevm',
    'zksync',
] %}

{{
  config(
    schema = 'tokens',
    alias = 'erc20_stablecoins',
    materialized = 'view',
    tags = ['static'],
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
                                  spell_type = "sector",
                                  spell_name = "tokens",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select
  s.blockchain,
  s.contract_address,
  m.backing,
  coalesce(erc20.symbol, m.symbol) as symbol,
  coalesce(erc20.decimals, m.decimals) as decimals,
  m.name
from (
  {% for chain in chains %}
  select
    blockchain,
    contract_address
  from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
) s
left join {{ source('tokens', 'erc20') }} erc20
  on s.blockchain = erc20.blockchain
  and s.contract_address = erc20.contract_address
left join {{ ref('tokens_erc20_stablecoins_metadata') }} m
  on s.blockchain = m.blockchain
  and s.contract_address = m.contract_address
