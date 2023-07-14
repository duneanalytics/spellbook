{{config(
	tags=['legacy'],
	alias = alias('dao_addresses', legacy_model=True))}}

{% set aragon_models = [
ref('aragon_gnosis_client_dao_addresses_legacy')
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