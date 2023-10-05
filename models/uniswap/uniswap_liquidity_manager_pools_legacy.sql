{{ config(
	tags=['legacy'],
	
        alias = alias('liquidity_manager_pools', legacy_model=True),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "uniswap",
                                \'["msilb7"]\') }}'
        )
}}

{% set uniswap_models = [
     ref('gamma_uniswap_pools_legacy')
    ,ref('arrakis_uniswap_pools_legacy')
] %}


SELECT *
FROM (
    {% for lp_lm_model in uniswap_models %}
    SELECT
      blockchain
    , 'uniswap' as dex_project_name
    , '3' as dex_project_version
    , project
    , contract_address
    , pool_contract
    , fee
    , token0
    , token1
        
    FROM {{ lp_lm_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;