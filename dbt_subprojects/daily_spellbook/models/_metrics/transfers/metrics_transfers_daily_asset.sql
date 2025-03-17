{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily_asset'
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
        , block_date
        , contract_address
        , symbol
        , net_transfer_amount_usd
        FROM {{ ref('metrics_' + blockchain + '_transfers_daily_asset') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
