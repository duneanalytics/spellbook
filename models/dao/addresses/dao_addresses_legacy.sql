{{ config(
	tags=['legacy'],
    alias = alias('addresses', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis", "polygon"]\',
                                "sector",
                                "dao",
                                \'["Henrystats"]\') }}')
}}

{% set addresses_models = [
ref('dao_addresses_ethereum_legacy')
,ref('dao_addresses_gnosis_legacy')
,ref('dao_addresses_polygon_legacy')
] %}


SELECT *

FROM (
    {% for dao_model in addresses_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)