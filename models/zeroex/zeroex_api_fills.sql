{{ config(
        alias ='api_fills',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","fantom","avalanche_c"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [  
ref('zeroex_arbitrum_api_fills')
,ref('zeroex_avalanche_c_api_fills')
,ref('zeroex_ethereum_api_fills')
,ref('zeroex_fantom_api_fills')
,ref('zeroex_optimism_api_fills')
,ref('zeroex_polygon_api_fills')
] %}


SELECT *
FROM (
    {% for model in zeroex_models %}
    SELECT
    *
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
