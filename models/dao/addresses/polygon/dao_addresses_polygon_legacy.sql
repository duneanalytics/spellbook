{{config(
	tags=['legacy'],
	alias = alias('addresses_polygon', legacy_model=True))}}

WITH 

mapping as (
        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('aragon_polygon_dao_addresses_legacy') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_polygon_syndicate_legacy') }}


)

SELECT * FROM mapping