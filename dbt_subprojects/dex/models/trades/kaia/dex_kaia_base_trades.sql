{{ config(
    schema = 'dex_kaia'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

--exclude kaiaswap until enabled ref('kaia_swap_v3_kaia_base_trades')
{% set base_models = [
    ref('dragon_swap_v2_kaia_base_trades')
    , ref('dragon_swap_v3_kaia_base_trades')
    , ref('klay_swap_v3_kaia_base_trades')
    , ref('neopin_kaia_base_trades')
    , ref('defi_kingdoms_kaia_base_trades')
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
        , blockchain = 'kaia'
        , columns = ['from', 'to', 'index']
    )
}}
