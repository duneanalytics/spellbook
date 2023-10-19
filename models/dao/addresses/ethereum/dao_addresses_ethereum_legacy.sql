{{config(
	tags=['legacy'],
	alias = alias('addresses_ethereum', legacy_model=True))}}

{% set addresses_models = [
ref('aragon_ethereum_dao_addresses_legacy')
,ref('daohaus_ethereum_dao_addresses_legacy')
,ref('syndicate_ethereum_dao_addresses_legacy')
,ref('zodiac_ethereum_dao_addresses_legacy')
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
