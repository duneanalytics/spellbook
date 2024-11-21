{{config(
        
        alias = 'addresses_gnosis')}}


{% set addresses_models = [
ref('aragon_gnosis_dao_addresses')
,ref('daohaus_gnosis_dao_addresses')
,ref('colony_gnosis_dao_addresses')
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