{% set chains = [
    'abstract'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'bnb'
    , 'bob'
    , 'celo'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'monad'
    , 'opbnb'
    , 'optimism'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'somnia'
    , 'sonic'
    , 'story'
    , 'taiko'
    , 'unichain'
    , 'worldchain'
    , 'xlayer'
    , 'zksync'
] %}

{{
  config(
    schema = 'stablecoins',
    alias = 'transfers',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
  )
}}

select *
from (
    {% for chain in chains %}
    select
        blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , token_address
        , token_symbol
        , token_backing
        , token_name
        , amount_raw
        , amount
        , price_usd
        , amount_usd
        , "from"
        , "to"
        , unique_key
    from {{ ref('stablecoins_' ~ chain ~ '_transfers') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

