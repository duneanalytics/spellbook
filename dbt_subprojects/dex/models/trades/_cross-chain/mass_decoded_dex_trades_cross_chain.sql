{{ config(
    schema = 'dex_mass_decoding_cross_chain'
    , alias = 'trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    
    ref('uniswap_v2_forks_base_trades_cross_chain')
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
            , factory_address
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
    add_tx_columns_cross_chain(
        model_cte = 'base_union'
        , blockchain = 'evms'
        , columns = ['from', 'to', 'index']
    )
}}

