{{ config(
        schema = 'tokens'
        , alias = 'net_transfers_daily_asset'
        , materialized = 'view'
        )
}}

{% set chains = [
     'arbitrum'
    , 'avalanche_c'
    , 'b3'
    , 'base'
    , 'blast'
    , 'bnb'
    , 'bob'
    , 'boba'
    , 'celo'
    , 'ethereum'
    , 'fantom'
    , 'gnosis'
    , 'linea'
    , 'mantle'
    , 'opbnb'
    , 'optimism'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'shape'
    , 'tron'
    , 'unichain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
    , 'abstract'
    , 'apechain'
    , 'berachain'
    , 'boba'
    , 'corn'
    , 'degen'
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
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily_asset') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)