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
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'ethereum'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'mezo'
    , 'monad'
    , 'morph'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'polygon'
    , 'robinhood'
    , 'ronin'
    , 'rise'
    , 'scroll'
    , 'sei'
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
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'lens'
    , 'peaq'
    , 'plume'
    , 'somnia'
    , 'sonic'
    , 'viction'
    , 'worldchain'
    , 'xlayer'
] %}

SELECT *
FROM (
        {% for blockchain in chains %}
        SELECT
        blockchain
        , block_date
        , net_transfer_amount_usd
        FROM {{ ref('tokens_' + blockchain + '_net_transfers_daily') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
)
