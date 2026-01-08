{% set chains = [
    'abstract'
    , 'apechain'
    , 'arbitrum'
    , 'avalanche_c'
    , 'base'
    , 'berachain'
    , 'blast'
    , 'bnb'
    , 'boba'
    , 'celo'
    , 'corn'
    , 'ethereum'
    , 'fantom'
    , 'flare'
    , 'flow'
    , 'gnosis'
    , 'hemi'
    , 'hyperevm'
    , 'ink'
    , 'kaia'
    , 'katana'
    , 'linea'
    , 'mantle'
    , 'mezo'
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
    , 'shape'
    , 'somnia'
    , 'sonic'
    , 'sophon'
    , 'story'
    , 'superseed'
    , 'tac'
    , 'taiko'
    , 'unichain'
    , 'worldchain'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{{ config(
    schema = 'dex'
    , alias = 'trades_mat_test'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}


{% for chain in chains %}
SELECT
     blockchain
    , project
    , version
    , block_month
    , block_date
    , block_time
    , block_number
    , token_bought_symbol
    , token_sold_symbol
    , token_pair
    , token_bought_amount
    , token_sold_amount
    , token_bought_amount_raw
    , token_sold_amount_raw
    , amount_usd
    , token_bought_address
    , token_sold_address
    , taker
    , maker
    , project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
FROM
    {{ ref('dex_'~chain~'_trades_mat_test') }}
{% if var('dev_dates', false) -%}
WHERE block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
{%- else -%}
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{%- endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}

