{{ config(
    schema = 'dex_fantom'
    , alias = 'trades_mat_test'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_fantom_base_trades')
            , filter = "1=1"
            , tokens_erc20_model = source('tokens', 'erc20')
            , blockchain = 'fantom'
        )
    }}
)
, oneinch_lop AS (
    SELECT
        *
    FROM
        {{ ref('oneinch_lop_own_trades') }}
    WHERE
        blockchain = 'fantom'
    {% if var('dev_dates', false) -%}
    AND block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
    {%- else -%}
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    {%- endif %}
)
, zeroex_native AS (
    SELECT
        *
    FROM
        {{ ref('zeroex_native_trades') }}
    WHERE
        blockchain = 'fantom'
    {% if var('dev_dates', false) -%}
    AND block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
    {%- else -%}
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    {%- endif %}
)

{% set cte_to_union = [
    'dexs'
    , 'oneinch_lop'
    , 'zeroex_native'
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

