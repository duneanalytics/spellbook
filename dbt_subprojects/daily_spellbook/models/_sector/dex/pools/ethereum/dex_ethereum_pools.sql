{{ config(
    schema = 'dex_ethereum'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v2_ethereum_pools')
    ,ref('uniswap_v3_ethereum_pools')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            blockchain
            , project
            , version
            , pool
            , fee
            , tokens
            , tokens_in_pool
            , creation_block_time
            , creation_block_number
            , contract_address
        FROM 
            {{ base_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT * FROM base_union