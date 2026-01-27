{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'b3'
    , 'base'
    , 'berachain'
    , 'bnb'
    , 'bob'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'degen'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'henesys'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'monad'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'sepolia'
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'tron'
    , 'unichain'
    , 'viction'
    , 'worldchain'
    , 'xlayer'
    , 'zkevm'
    , 'zksync'
] %}

{{
  config(
    schema = 'stablecoins_evm',
    alias = 'transfers',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

select *
from {{ ref('stablecoins_transfers') }}

