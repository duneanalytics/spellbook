{{config(
	tags=['legacy'],
	alias = alias('addresses_polygon', legacy_model=True))}}

{% set addresses_models = [
ref('aragon_polygon_dao_addresses_legacy')
,ref('syndicate_polygon_dao_addresses_legacy')
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