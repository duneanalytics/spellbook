 {{
  config(
	tags=['legacy'],
	
        schema='gamma',
        alias = alias('uniswap_pools', legacy_model=True),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','contract_address', 'pool_contract'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "gamma",
                                    \'["msilb7"]\') }}'
  )
}}

{% set lp_models = [
  ref('gamma_optimism_uniswap_pools_legacy')
] %}


SELECT *
FROM (
    {% for g_lp_lm_model in lp_models %}
    SELECT
      blockchain
    , 'gamma' AS project
    , lp_name
    , contract_address
    , pool_contract
    , fee
    , token0
    , token1
        
    FROM {{ g_lp_lm_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;