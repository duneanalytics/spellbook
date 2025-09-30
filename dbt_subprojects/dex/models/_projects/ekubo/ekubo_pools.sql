{{ config(
        schema = 'ekubo',
        alias = 'pools'
        )
}}

{% set ekubo_models = [
ref('ekubo_ethereum_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in ekubo_models %}
    SELECT
        blockchain
        , project
        , version
        , id as pool
        , fee_decimal
        , tick_spacing_decimal
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