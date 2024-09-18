{{ config(
        schema='tokens',
        alias = 'stablecoins',
        materialized='table',
        tags = ['static'],
        post_hook = '{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "polygon", "fantom", "base"]\',
                                    "sector",
                                    "stablecoins",
                                    \'["synthquest"]\') }}'
        )
}}

{% set stables_models = [
  ref('arbitrum_erc20_stablecoins')
, ref('avalanche_c_erc20_stablecoins')
, ref('prices_bitcoin_tokens')
, ref('bnb_erc20_stablecoins')
, ref('prices_cardano_tokens')
, ref('ethereum_erc20_stablecoins')
, ref('fantom_erc20_stablecoins')
, ref('gnosis_erc20_stablecoins')
, ref('optimism_erc20_stablecoins')
, ref('polygon_erc20_stablecoins')
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
