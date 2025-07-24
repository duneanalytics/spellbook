{{ config(
    schema = 'uniswap_worldchain',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['id', 'version'],
    post_hook='{{ expose_spells(\'["worldchain"]\',
                                "project",
                                "uniswap",
                                \'["Henrystats"]\') }}'
    )
}}

{% set version_models = [
ref('uniswap_v4_worldchain_pools')
, ref('uniswap_v3_worldchain_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in uniswap_models %}
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
    FROM {{ version_models }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)