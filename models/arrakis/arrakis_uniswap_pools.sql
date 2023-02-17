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

{% set uniswap_models = [
  'arrakis_optimism_uniswap_pools'
] %}


SELECT *
FROM (
    {% for lp_lm_model in uniswap_models %}
    SELECT
      blockchain
    , 'arrakis' AS project
    , contract_address, 
    , pool, 
    , fee, 
    , token0, 
    . token1
        
    FROM {{ ref(lp_lm_model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;