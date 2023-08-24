{{ config(
        alias = alias('safes_all'),
        tags = ['dunesql'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","celo","ethereum","fantom","gnosis","goerli","optimism","polygon"]\',
                                "project",
                                "safe",
                                \'["tschubotz", "danielpartida"]\') }}'
        )
}}

{% set safe_safes_models = [
 ref('safe_arbitrum_safes')
,ref('safe_avalanche_c_safes')
,ref('safe_bnb_safes')
,ref('safe_celo_safes')
,ref('safe_ethereum_safes')
,ref('safe_fantom_safes')
,ref('safe_gnosis_safes')
,ref('safe_goerli_safes')
,ref('safe_optimism_safes')
,ref('safe_polygon_safes')
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
