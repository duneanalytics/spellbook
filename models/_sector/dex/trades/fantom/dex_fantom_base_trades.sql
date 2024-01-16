{{ config(
    schema = 'dex_fantom'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('sushiswap_v1_fantom_base_trades')
    , ref('sushiswap_v2_fantom_base_trades')
    , ref('spiritswap_fantom_base_trades')
    , ref('spookyswap_fantom_base_trades')
    , ref('wigoswap_fantom_base_trades')
    , ref('equalizer_fantom_base_trades')
    , ref('spartacus_exchange_fantom_base_trades')
    , ref('openocean_fantom_base_trades')
    , ref('beethoven_x_fantom_base_trades')
    , ref('curvefi_fantom_base_trades')
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
        , blockchain = 'fantom'
        , columns = ['from', 'to', 'index']
    )
}}