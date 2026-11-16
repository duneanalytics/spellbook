{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily'
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
        , block_date
        , net_transfer_amount_usd
        FROM {{ ref('metrics_' + blockchain + '_transfers_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
