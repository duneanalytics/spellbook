{{ config(
    schema = 'dex'
    , alias = 'pools_beta'
    , materialized = 'view'
    )
}}

{% set base_models = [
     ref('dex_ethereum_pools')
    ,ref('dex_optimism_pools')
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
            , token_symbols
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