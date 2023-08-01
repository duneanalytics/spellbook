{{config(
	tags=['legacy'],
	alias = alias('addresses_ethereum', legacy_model=True))}}

WITH 

mapping as (
        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('aragon_ethereum_dao_addresses_legacy') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_ethereum_daohaus_legacy') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_ethereum_zodiac_legacy') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_ethereum_syndicate_legacy') }}


)
 
SELECT * FROM mapping

