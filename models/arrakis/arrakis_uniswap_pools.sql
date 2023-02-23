 {{
  config(
        schema='arrakis',
        alias='uniswap_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','lp_name', 'contract_address', 'pool'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "arrakis",
                                    \'["msilb7"]\') }}'
  )
}}

{% set lp_models = [
  ref('arrakis_optimism_uniswap_pools')
] %}


SELECT *
FROM (
    {% for a_lp_lm_model in lp_models %}
    SELECT
      blockchain
    , 'arrakis' AS project
    , contract_address
    , pool_contract
    , fee
    , token0
    , token1
        
    FROM {{ a_lp_lm_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;