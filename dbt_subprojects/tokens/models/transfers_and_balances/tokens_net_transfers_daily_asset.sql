{{ config(
        schema = 'tokens'
        , alias = 'net_transfers_daily_asset'
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
                
                /* since solana labels are not used at this time, coalesce again in final view */
                blockchain
                , block_date
                , contract_address
                , symbol
                , transfer_amount_usd_sent
                , transfer_amount_usd_received
                , transfer_amount_usd
                , net_transfer_amount_usd
                , transfer_count
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily_asset') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)