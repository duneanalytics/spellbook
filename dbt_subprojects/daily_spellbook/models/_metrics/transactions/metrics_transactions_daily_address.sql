{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily_address'
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

SELECT
    *
FROM (
    {% for blockchain in chains %}
    SELECT
        blockchain
        , block_date
        , address
        , name
        , primary_category
        , hq_country
        , tx_count
    FROM {{ ref('metrics_' + blockchain + '_transactions_daily_address') }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
