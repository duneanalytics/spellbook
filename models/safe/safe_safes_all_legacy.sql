{{ config(
	tags=['legacy'],
	
        alias = alias('safes_all', legacy_model=True),
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","celo","ethereum","fantom","gnosis","goerli","optimism","polygon"]\',
                                "project",
                                "safe",
                                \'["tschubotz", "danielpartida"]\') }}'
        )
}}

{% set safe_safes_models = [
 ref('safe_arbitrum_safes_legacy')
,ref('safe_avalanche_c_safes_legacy')
,ref('safe_celo_safes_legacy')
,ref('safe_bnb_safes_legacy')
,ref('safe_ethereum_safes_legacy')
,ref('safe_fantom_safes_legacy')
,ref('safe_gnosis_safes_legacy')
,ref('safe_goerli_safes_legacy')
,ref('safe_optimism_safes_legacy')
,ref('safe_polygon_safes_legacy')
] %}


SELECT *
FROM (
    {% for safes_model in safe_safes_models %}
    SELECT
        blockchain, 
        address, 
        creation_version, 
        block_date, 
        creation_time, 
        tx_hash
    FROM {{ safes_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
