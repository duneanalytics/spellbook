{{ config(
        schema='tokens',
        alias = 'erc20_stablecoins',
        materialized='table',
        tags = ['static'],
        post_hook = '{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "polygon", "fantom", "base"]\',
                                    "sector",
                                    "stablecoins",
                                    \'["synthquest"]\') }}'
        )
}}

{% set stables_models = [
  ref('tokens_arbitrum_erc20_stablecoins')
, ref('tokens_avalanche_c_erc20_stablecoins')
, ref('tokens_bnb_erc20_stablecoins')
, ref('tokens_ethereum_erc20_stablecoins')
, ref('tokens_fantom_erc20_stablecoins')
, ref('tokens_gnosis_erc20_stablecoins')
, ref('tokens_optimism_erc20_stablecoins')
, ref('tokens_polygon_erc20_stablecoins')
, ref('tokens_base_erc20_stablecoins')
] %}


SELECT *
FROM
(
    {% for model in stables_models %}
    SELECT
          blockchain
        , contract_address
        , backing
        , symbol
        , decimals
        , name
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
