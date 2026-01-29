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
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

select *
from {{ ref('stablecoins_balances') }}
