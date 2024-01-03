{{ config(
    schema = 'dex_arbitrum'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_arbitrum_base_trades')
    , ref('camelot_v2_arbitrum_base_trades')
    , ref('camelot_v3_arbitrum_base_trades')
    , ref('airswap_arbitrum_base_trades')
    , ref('sushiswap_v1_arbitrum_base_trades')
    , ref('sushiswap_v2_arbitrum_base_trades')
    , ref('arbswap_arbitrum_base_trades')
    , ref('trader_joe_v2_arbitrum_base_trades')
    , ref('trader_joe_v2_1_arbitrum_base_trades')
    , ref('pancakeswap_v2_arbitrum_base_trades')
    , ref('pancakeswap_v3_arbitrum_base_trades')
    , ref('balancer_v2_arbitrum_base_trades')
    , ref('dodo_arbitrum_base_trades')
    , ref('gmx_arbitrum_base_trades')
    , ref('integral_arbitrum_base_trades')
    , ref('clipper_arbitrum_base_trades')
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
        , blockchain = 'arbitrum'
        , columns = ['from', 'to', 'index']
    )
}}