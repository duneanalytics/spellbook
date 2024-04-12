{{ config(
    schema = 'dex_polygon'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_polygon_base_trades')
    , ref('apeswap_polygon_base_trades')
    , ref('airswap_polygon_base_trades')
    , ref('sushiswap_v1_polygon_base_trades')
    , ref('sushiswap_v2_polygon_base_trades')
    , ref('honeyswap_v2_polygon_base_trades')
    , ref('quickswap_v2_polygon_base_trades')
    , ref('quickswap_v3_polygon_base_trades')
    , ref('balancer_v2_polygon_base_trades')
    , ref('fraxswap_polygon_base_trades')
    , ref('dodo_polygon_base_trades')
    , ref('kyberswap_polygon_base_trades')
    , ref('clipper_polygon_base_trades')
    , ref('xchange_polygon_base_trades')
    , ref('dooar_polygon_base_trades')
    , ref('uniswap_v2_polygon_base_trades')
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
        , blockchain = 'polygon'
        , columns = ['from', 'to', 'index']
    )
}}