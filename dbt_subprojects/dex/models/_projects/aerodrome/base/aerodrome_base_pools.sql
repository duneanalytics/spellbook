{{ config(
    schema = 'aerodrome_base',
    alias = 'pools',
    materialized = 'view'
    )
}}

{% set version_models = [
ref('aerodrome_v1_base_pools')
, ref('aerodrome_slipstream_base_pools')
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
