{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

{% set chains = [
     'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'bitcoin'
    , 'blast'
    , 'bnb'
    , 'celo'
    , 'ethereum'
    , 'fantom'
    , 'gnosis'
    , 'linea'
    , 'mantle'
    , 'optimism'
    , 'polygon'
    , 'scroll'
    , 'sei'
    , 'solana'
    , 'tron'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

SELECT *
FROM (
        {% for blockchain in chains %}
        SELECT
        blockchain
        , block_date
        , transfer_amount_usd_sent
        , transfer_amount_usd_received
        , transfer_amount_usd
        , net_transfer_amount_usd
        FROM {{ ref('metrics_' + blockchain + '_transfers_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
