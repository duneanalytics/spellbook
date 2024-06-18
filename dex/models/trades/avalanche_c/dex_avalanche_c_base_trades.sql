{{ config(
    schema = 'dex_avalanche_c'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v3_avalanche_c_base_trades')
    , ref('airswap_avalanche_c_base_trades')
    , ref('sushiswap_v1_avalanche_c_base_trades')
    , ref('sushiswap_v2_avalanche_c_base_trades')
    , ref('fraxswap_avalanche_c_base_trades')
    , ref('trader_joe_v1_avalanche_c_base_trades')
    , ref('trader_joe_v2_avalanche_c_base_trades')
    , ref('trader_joe_v2_1_avalanche_c_base_trades')
    , ref('balancer_v2_avalanche_c_base_trades')
    , ref('glacier_v2_avalanche_c_base_trades')
    , ref('glacier_v3_avalanche_c_base_trades')
    , ref('gmx_avalanche_c_base_trades')
    , ref('pharaoh_avalanche_c_base_trades')
    , ref('kyberswap_avalanche_c_base_trades')
    , ref('platypus_finance_avalanche_c_base_trades')
    , ref('openocean_avalanche_c_base_trades')
    , ref('woofi_avalanche_c_base_trades')
    , ref('curvefi_avalanche_c_base_trades')
    , ref('hashflow_avalanche_c_base_trades')
    , ref('uniswap_v2_avalanche_c_base_trades')
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
        , blockchain = 'avalanche_c'
        , columns = ['from', 'to', 'index']
    )
}}
