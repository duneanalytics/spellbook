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
    schema = 'stablecoins_evm',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ hide_spells() }}'
  )
}}

select *
from (
  {% for chain in chains %}
  select
    blockchain,
    day,
    address,
    token_symbol,
    token_address,
    token_standard,
    token_id,
    balance_raw,
    balance,
    balance_usd,
    currency,
    last_updated
  from {{ ref('stablecoins_' ~ chain ~ '_balances') }}
  {% if not loop.last %}
  union all
  {% endif %}
  {% endfor %}
)
