{{ config(
    schema = 'dex_berachain'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('kodiak_v3_berachain_base_trades')
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
            , token_bought_symbol
            , token_sold_symbol
            , token_pair
            , token_bought_amount
            , token_sold_amount
            , token_bought_amount_raw
            , token_sold_amount_raw
            , amount_usd
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
        FROM {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT *
FROM base_union
