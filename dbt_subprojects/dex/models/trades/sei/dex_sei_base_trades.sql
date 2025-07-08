{{ config(
    schema = 'dex_sei'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('jelly_swap_sei_base_trades')
    , ref('oku_sei_base_trades')
    , ref('dragon_swap_sei_base_trades')
    , ref('xei_finance_sei_base_trades')
    , ref('carbon_defi_sei_base_trades')
    , ref('yaka_sei_base_trades')
    , ref('sailor_finance_sei_base_trades')
    , ref('yei_swap_sei_base_trades')
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
        , blockchain = 'sei'
        , columns = ['from', 'to', 'index']
    )
}}
