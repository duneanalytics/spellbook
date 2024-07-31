{{ config(
     schema = 'zeroex'
        , alias = 'api_fills_deduped'
        , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "optimism", "polygon"]\',
                                "project",
                                "zeroex",
                                \'["rantum","bakabhai993"]\') }}'
        )
}}


{% set zeroex_models = [  
  ref('zeroex_bnb_api_fills_deduped')
  ,ref('zeroex_base_settler_trades')

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