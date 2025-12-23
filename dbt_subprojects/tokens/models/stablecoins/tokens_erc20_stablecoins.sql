{% set chains = [
    'arbitrum',
    'avalanche_c',
    'base',
    'bnb',
    'bob',
    'celo',
    'ethereum',
    'fantom',
    'gnosis',
    'kaia',
    'linea',
    'mantle',
    'optimism',
    'polygon',
    'scroll',
    'worldchain',
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
