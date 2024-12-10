{{ config(
    schema = 'dex_flare'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('enosys_v2_flare_base_trades')
    , ref('enosys_v3_flare_base_trades')
    , ref('sparkdex_v2_flare_base_trades')
    , ref('sparkdex_v3_flare_base_trades')
    , ref('blazeswap_flare_base_trades')
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
        , blockchain = 'flare'
        , columns = ['from', 'to', 'index']
    )
}}
