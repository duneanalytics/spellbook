{{ config(
    schema = 'dex_optimism'
    , alias = 'pools'
    , materialized = 'view'
    )
}}

{% set base_models = [
    ref('uniswap_v2_optimism_pools')
    ,ref('uniswap_v3_optimism_pools_array')
    ,ref('solidly_v3_optimism_pools')
    ,ref('sushiswap_v2_optimism_pools')
    ,ref('fraxswap_optimism_pools')
    ,ref('dackieswap_v2_optimism_pools')
    ,ref('dackieswap_v3_optimism_pools')
    ,ref('elk_finance_optimism_pools')
    ,ref('gridex_optimism_pools')
    ,ref('velodrome_v1_optimism_pools')
    ,ref('velodrome_v2_optimism_pools')
    ,ref('zipswap_optimism_pools')
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