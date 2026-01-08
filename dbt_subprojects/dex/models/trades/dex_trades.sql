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
    , 'xlayer'
    , 'zkevm'
    , 'zksync'
    , 'zora'
] %}

{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
                                    spell_type = "sector",
                                    spell_name = "dex",
                                    contributors = \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh", "kryptaki"]\') }}'
    )
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage.
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

WITH chain_trades AS (
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
        {{ ref('dex_'~chain~'_trades') }}
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
)
, as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , CAST(block_date AS date) AS block_date
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
        , CAST(evt_index AS bigint) AS evt_index
    FROM
        {{ model }}
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
)

{% set cte_to_union = [
    'chain_trades'
    , 'as_is_dexs'
    ]
%}

{% for cte in cte_to_union %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , CAST(block_date AS date) AS block_date
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
        , CAST(evt_index AS bigint) AS evt_index
    FROM
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
