{{ config(
        schema = 'tokens'
        , alias = 'net_transfers_daily'
        , materialized = 'view'
        )
}}

{% set chains = [
      'arbitrum'
    , 'avalanche_c'
    , 'b3'
    , 'base'
    , 'blast'
    , 'bnb'
    , 'bob'
    , 'boba'
    , 'celo'
    , 'ethereum'
    , 'fantom'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'mezo'
    , 'monad'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'tron'
    , 'unichain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
    , 'abstract'
    , 'apechain'
    , 'berachain'
    , 'corn'
    , 'degen'
    , 'flare'
    , 'henesys'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'lens'
    , 'nova'
    , 'peaq'
    , 'plume'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'viction'
    , 'worldchain'
] %}

SELECT *
FROM (
        {% for blockchain in chains %}
        SELECT
        blockchain
        , block_date
        , net_transfer_amount_usd
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)

