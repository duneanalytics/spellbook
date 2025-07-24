{{ config(
    schema = 'uniswap_zora',
    alias = 'pools',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['id', 'version'],
    post_hook='{{ expose_spells(\'["zora"]\',
                                "project",
                                "uniswap",
                                \'["Henrystats"]\') }}'
    )
}}

{% set version_models = [
ref('uniswap_v4_zora_pools')
, ref('uniswap_v3_zora_pools')
, ref('uniswap_v2_zora_pools')
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