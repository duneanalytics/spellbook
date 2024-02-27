{{ config(
    schema = 'dex_base'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_base_base_trades')
    , ref('sushiswap_v1_base_base_trades')
    , ref('sushiswap_v2_base_base_trades')
    , ref('aerodrome_base_base_trades')
    , ref('pancakeswap_v2_base_base_trades')
    , ref('pancakeswap_v3_base_base_trades')
    , ref('balancer_v2_base_base_trades')
    , ref('dodo_base_base_trades')
    , ref('maverick_base_base_trades')
    , ref('smardex_base_base_trades')
    , ref('dackieswap_base_base_trades')
    , ref('rubicon_base_base_trades')
    , ref('baseswap_base_base_trades')
    , ref('scale_base_base_trades')
    , ref('kyberswap_base_base_trades')
    , ref('woofi_base_base_trades')
    , ref('velocimeter_v2_base_base_trades')
    , ref('moonbase_base_base_trades')
    , ref('horizondex_base_base_trades')
    , ref('plantbaseswap_base_base_trades')
    , ref('sobal_base_base_trades')
    , ref('derpdex_base_base_trades')
    , ref('torus_base_base_trades')
    , ref('throne_exchange_v2_base_base_trades')
    , ref('throne_exchange_v3_base_base_trades')
    , ref('sharkswap_base_base_trades')
    , ref('citadelswap_base_base_trades')
    , ref('autotronic_base_base_trades')
    , ref('uniswap_v2_base_base_trades')
    , ref('elk_finance_base_base_trades')
    , ref('oasisswap_base_base_trades')
    , ref('leetswap_v2_base_base_trades')
    , ref('wombat_exchange_base_base_trades')
    , ref('openocean_base_base_trades')
    , ref('rocketswap_base_base_trades')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_number
            , token_bought_amount_raw
            , token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
        FROM
            {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

{{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'base'
        , columns = ['from', 'to', 'index']
    )
}}