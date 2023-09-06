{{config(
	tags=['legacy'],
	alias = alias('dao_addresses', legacy_model=True))}}
{% set aragon_models = [
ref('aragon_ethereum_app_dao_addresses_legacy'),
ref('aragon_ethereum_client_dao_addresses_legacy')
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
        block_month,
        product
    FROM {{ dao_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;