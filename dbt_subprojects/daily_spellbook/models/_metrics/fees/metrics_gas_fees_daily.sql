{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_daily'
        , materialized = 'view'
        )
}}

{% set chains = [
     
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'b3'
    , 'base'
    , 'berachain'
    , 'bitcoin'
    , 'blast'
    , 'bnb'
    , 'bob'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'degen'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'gnosis'
    , 'ink'
    , 'kaia'
    , 'lens'
    , 'linea'
    , 'mantle'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'solana'
    , 'sonic'
    , 'sophon'
    , 'ton'
    , 'tron'
    , 'unichain'
    , 'worldchain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

SELECT *
FROM (
        {% for blockchain in chains %}
        SELECT
        blockchain
        ,block_date
        ,gas_fees_usd
        FROM {{ ref('metrics_' + blockchain + '_gas_fees_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
