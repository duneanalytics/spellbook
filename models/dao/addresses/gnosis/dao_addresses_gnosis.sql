{{config(
	tags=['legacy'],
	alias = alias('addresses_gnosis', legacy_model=True))}}

WITH 

mapping as (
        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('aragon_gnosis_dao_addresses') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_gnosis_daohaus') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_gnosis_colony') }}
)

SELECT * FROM mapping
