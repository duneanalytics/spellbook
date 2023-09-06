{{ config(
	tags=['legacy'],
    alias = alias('dao_addresses', legacy_model=True),
    materialized = 'view',
    file_format = 'delta',
    post_hook='{{ expose_spells(\'["ethereum", "gnosis"]\',
                                "project",
                                "daohaus",
                                \'["Henrystats"]\') }}')
}}

{% set daohaus_models = [
ref('daohaus_ethereum_dao_addresses_legacy')
,ref('daohaus_gnosis_dao_addresses_legacy')
] %}


SELECT *

FROM (
    {% for dao_model in daohaus_models %}
    SELECT
        blockchain,
        dao_creator_tool, 
        dao, 
        dao_wallet_address,
        created_block_time,
        created_date,
        block_month
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;