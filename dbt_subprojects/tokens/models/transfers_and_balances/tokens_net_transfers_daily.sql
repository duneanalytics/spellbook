{{ config(
        schema = 'tokens'
        , alias = 'net_transfers_daily'
        , materialized = 'view'
        )
}}

{% set chains = [
      'arbitrum'
    , 'avalanche_c'
    , 'base'
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
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)

