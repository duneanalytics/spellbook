{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

WITH curve AS (
    -- due to curve having increased complexity to determine token_bought_amount / token_sold_amount, enrich separately
    {{
        enrich_curve_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project = 'curve'"
            , curve_ethereum = ref('curvefi_ethereum_base_trades')
            , curve_optimism = ref('curvefi_optimism_base_trades')
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
, dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_base_trades')
            , filter = "project != 'curve'"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)
, as_is_dexs AS (
    {% for model in as_is_models %}
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
        {{ model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)


{% set cte_to_union = [
    'curve'
    , 'as_is_dexs'
    , 'dexs'
    ] 
%}

{% for cte in cte_to_union %}
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
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}