{{ config(
        
        alias = 'pools',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
        )
}}

{% set uniswap_models = [
ref('uniswap_ethereum_pools')
, ref('uniswap_arbitrum_pools')
, ref('uniswap_polygon_pools')
, ref('uniswap_optimism_pools')
, ref('uniswap_bnb_pools')
, ref('uniswap_celo_pools')
] %}


SELECT *
FROM (
    {% for dex_pool_model in uniswap_models %}
    SELECT
        blockchain
        , project
        , version
        , pool
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
;