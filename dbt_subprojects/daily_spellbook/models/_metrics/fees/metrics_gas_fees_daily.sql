{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_daily'
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
    , 'ronin'
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
        ,block_date
        ,gas_fees_usd
        FROM {{ ref('metrics_' + blockchain + '_gas_fees_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
