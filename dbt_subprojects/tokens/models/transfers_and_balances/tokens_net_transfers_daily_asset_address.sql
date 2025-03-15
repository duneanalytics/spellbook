{{ config(
        schema = 'tokens'
        , alias = 'net_transfers_daily_asset_address'
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
    , 'abstract'
    , 'apechain'
    , 'berachain'
    , 'boba'
    , 'corn'
    , 'flare'
    , 'ink'
    , 'kaia'
    , 'nova'
    , 'sonic'
    , 'sophon'
    , 'viction'
    , 'worldchain'
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
                , net_transfer_amount_usd
                , address
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily_asset_address') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)