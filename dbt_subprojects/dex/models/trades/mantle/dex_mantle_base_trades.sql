{{ config(
    schema = 'dex_mantle'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('merchant_moe_mantle_base_trades')
    , ref('fusionx_mantle_base_trades')
    , ref('agni_mantle_base_trades')
    , ref('swaap_v2_mantle_base_trades')
    , ref('clipper_mantle_base_trades')
    , ref('uniswap_v3_mantle_base_trades')
    , ref('tropicalswap_mantle_base_trades')
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
        , blockchain = 'mantle'
        , columns = ['from', 'to', 'index']
    )
}}
