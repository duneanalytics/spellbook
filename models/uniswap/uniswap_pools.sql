{{ config(
        alias ='pools',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap",
                                \'["hildobby"]\') }}'
        )
}}

{% set uniswap_models = [
'uniswap_ethereum_pools'
,'uniswap_arbitrum_pools'
,'uniswap_polygon_pools'
] %}


SELECT *
FROM (
    {% for dex_model in uniswap_models %}
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
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;