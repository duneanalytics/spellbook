{{ config(
        alias ='liquidity_manager_pools',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "uniswap",
                                \'["msilb7"]\') }}'
        )
}}

{% set uniswap_models = [
'uniswap_ethereum_trades'
,'uniswap_optimism_trades'
,'uniswap_arbitrum_trades'
,'uniswap_polygon_trades'
] %}


SELECT *
FROM (
    {% for lp_lm_model in uniswap_models %}
    SELECT
        
        
    FROM {{ ref(dex_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;