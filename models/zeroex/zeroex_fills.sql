{{ config(
        alias ='fills',
        post_hook='{{ expose_spells(\'["ethereum","arbitrum", "optimism", "polygon","fantom","avalanche_c"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [  
ref('zeroex_ethereum_fills')
,ref('zeroex_polygon_fills')
,ref('zeroex_optimism_fills')
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
