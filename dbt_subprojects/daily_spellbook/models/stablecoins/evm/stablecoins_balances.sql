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
    schema = 'stablecoins',
    alias = 'balances',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
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
        last_updated
    from {{ ref('stablecoins_' ~ chain ~ '_balances') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
