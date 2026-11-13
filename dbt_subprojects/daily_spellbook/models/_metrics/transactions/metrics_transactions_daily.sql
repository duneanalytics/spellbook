{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily'
        , materialized = 'view'
        )
}}

{% set chains = [
     
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'bitcoin'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'ethereum'
    , 'flare'
    , 'gnosis'
    , 'ink'
    , 'kaia'
    , 'lens'
    , 'linea'
    , 'mantle'
    , 'opbnb'
    , 'optimism'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'solana'
    , 'sonic'
    , 'ton'
    , 'tron'
    , 'unichain'
    , 'worldchain'
    , 'xrpl'
    , 'zksync'
] %}

SELECT *
FROM (
        {% for blockchain in chains %}
        SELECT
        blockchain
        ,block_date
        ,tx_count
        FROM {{ ref('metrics_' + blockchain + '_transactions_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
