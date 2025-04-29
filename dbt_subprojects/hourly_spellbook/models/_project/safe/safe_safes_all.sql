{{ config(
        schema = 'safe',
        alias = 'safes_all',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","base","blast","bnb","celo","ethereum","fantom","gnosis","goerli","linea","mantle","optimism","polygon","scroll","worldchain","zkevm","zksync"]\',
                                "project",
                                "safe",
                                \'["tschubotz", "danielpartida", "kryptaki", "safeintern"]\') }}'
        )
}}

{% set safe_safes_models = [
 ref('safe_arbitrum_safes')
,ref('safe_avalanche_c_safes')
,ref('safe_base_safes')
,ref('safe_berachain_safes')
,ref('safe_blast_safes')
,ref('safe_bnb_safes')
,ref('safe_celo_safes')
,ref('safe_ethereum_safes')
,ref('safe_fantom_safes')
,ref('safe_gnosis_safes')
,ref('safe_goerli_safes')
,ref('safe_linea_safes')
,ref('safe_mantle_safes')
,ref('safe_optimism_safes')
,ref('safe_polygon_safes')
,ref('safe_ronin_safes')
,ref('safe_scroll_safes')
,ref('safe_worldchain_safes')
,ref('safe_zkevm_safes')
,ref('safe_zksync_safes')
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
