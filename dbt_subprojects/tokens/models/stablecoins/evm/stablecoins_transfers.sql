{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'b3'
    , 'base'
    , 'berachain'
    , 'bnb'
    , 'bob'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'degen'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'henesys'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'megaeth'
    , 'monad'
    , 'nova'
    , 'opbnb'
    , 'optimism'
    , 'peaq'
    , 'plasma'
    , 'plume'
    , 'polygon'
    , 'ronin'
    , 'scroll'
    , 'sei'
    , 'sepolia'
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'tron'
    , 'unichain'
    , 'viction'
    , 'worldchain'
    , 'xlayer'
    , 'zkevm'
    , 'zksync'
] %}

{{
  config(
    schema = 'stablecoins',
    alias = 'transfers',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['blockchain', 'block_month', 'block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
                                  spell_type = "sector",
                                  spell_name = "stablecoins",
                                  contributors = \'["tomfutago"]\') }}'
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
    {% if is_incremental() %}
    where {{ incremental_predicate('block_date') }}
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
