{{ config(
    schema = 'dex_ethereum'
    , alias = 'base_pools'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v2_ethereum_base_pools')
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
            , tx_hash
            , evt_index
            , contract_address
            , pair
            , fee
            , token0
            , token1
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
        , blockchain = 'ethereum'
        , columns = ['from', 'to']
    )
}}