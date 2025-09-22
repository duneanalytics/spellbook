{{ config(
        schema = 'velodrome',
        alias = 'pools'
        )
}}

{% set velodrome_models = [
ref('velodrome_optimism_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in velodrome_models %}
    SELECT
        blockchain
        , project
        , version
        , id as pool
        , fee
        , token0
        , token1
        , creation_block_time
        , creation_block_number
        , contract_address
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)