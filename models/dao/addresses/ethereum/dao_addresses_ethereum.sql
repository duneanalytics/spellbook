{{config(
        tags = ['dunesql'],
        alias = alias('addresses_ethereum'))}}

{% set addresses_models = [
ref('aragon_ethereum_dao_addresses')
,ref('daohaus_ethereum_dao_addresses')
,ref('syndicate_ethereum_dao_addresses')
,ref('zodiac_ethereum_dao_addresses')
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


