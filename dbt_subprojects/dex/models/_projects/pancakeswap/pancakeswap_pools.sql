{{ config(
        schema = 'pancakeswap',
        alias = 'pools'
        )
}}

{% set pancakeswap_models = [
ref('pancakeswap_arbitrum_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in pancakeswap_models %}
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