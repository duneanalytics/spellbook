{{ config(
    schema = 'pancakeswap_bnb',
    alias = 'pools',
    materialized = 'view'
    )
}}

{% set version_models = [
ref('pancakeswap_infinity_cl_bnb_pools')
, ref('pancakeswap_infinity_lb_bnb_pools')
, ref('pancakeswap_stableswap_bnb_pools')
, ref('pancakeswap_v3_bnb_pools')
, ref('pancakeswap_v2_bnb_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in version_models %}
    SELECT
        blockchain
        , project
        , version
        , id
        , fee
        , token0
        , token1
        , creation_block_time
        , creation_block_number
        , contract_address
        , tx_hash 
        , evt_index 
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
