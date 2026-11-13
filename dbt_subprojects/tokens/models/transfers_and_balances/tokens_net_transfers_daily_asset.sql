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
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'ethereum'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'mezo'
    , 'monad'
    , 'morph'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'polygon'
    , 'robinhood'
    , 'ronin'
    , 'rise'
    , 'scroll'
    , 'sei'
    , 'somnia'
    , 'taiko'
    , 'tempo'
    , 'tezos_evm'
    , 'tron'
    , 'unichain'
    , 'zksync'
    , 'abstract'
    , 'apechain'
    , 'berachain'
    , 'cronos'
    , 'flare'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'lens'
    , 'plume'
    , 'sonic'
    , 'viction'
    , 'worldchain'
    , 'xlayer'
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
