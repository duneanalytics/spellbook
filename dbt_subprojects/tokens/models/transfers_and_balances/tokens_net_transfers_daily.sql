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
    , 'gnosis'
    , 'linea'
    , 'mantle'
    , 'opbnb'
    , 'optimism'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'tron'
    , 'unichain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
    , 'abstract'
    , 'apechain'
    , 'berachain'
    , 'boba'
    , 'corn'
    , 'degen'
    , 'flare'
    , 'ink'
    , 'kaia'
    , 'nova'
    , 'sonic'
    , 'sophon'
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

