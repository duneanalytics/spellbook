{{ config(
        tags=['dunesql']
        , schema = 'zeroex'
        , alias = alias('api_fills_deduped')
        , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "optimism", "polygon"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}

{% set zeroex_models = [  
  ref('zeroex_arbitrum_api_fills_deduped')
  ,ref('zeroex_avalanche_c_api_fills_deduped')
  ,ref('zeroex_base_api_fills_deduped')
  ,ref('zeroex_bnb_api_fills_deduped')
  ,ref('zeroex_celo_api_fills_deduped')
  ,ref('zeroex_ethereum_api_fills_deduped')
  ,ref('zeroex_fantom_api_fills_deduped')
  ,ref('zeroex_optimism_api_fills_deduped')
  ,ref('zeroex_polygon_api_fills_deduped')
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