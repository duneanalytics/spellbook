{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily_address'
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
                , address
                /* since solana labels are not used at this time, coalesce again in final view */
                , coalesce(name, 'Unknown') as name
                , coalesce(primary_category, 'Uncategorized') as primary_category
                , coalesce(hq_country, 'Unknown') as hq_country
                , transfer_amount_usd_sent
                , transfer_amount_usd_received
                , transfer_amount_usd
                , net_transfer_amount_usd
        FROM {{ ref('metrics_' + blockchain + '_transfers_daily_address') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
