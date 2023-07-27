{{ config(
	tags=['legacy'],
	
    alias = alias('dao_addresses', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "project",
                                "aragon",
                                \'["Henrystats"]\') }}')
}}

{% set aragon_models = [
ref('aragon_ethereum_dao_addresses_legacy')
,ref('aragon_gnosis_dao_addresses_legacy')
,ref('aragon_polygon_dao_addresses_legacy')
] %}


SELECT *

FROM (
    {% for dao_model in aragon_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date,
        product
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;