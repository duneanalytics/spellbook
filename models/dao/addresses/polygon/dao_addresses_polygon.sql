{{config(alias = alias('addresses_polygon'))}}

WITH 

mapping as (
        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('aragon_polygon_dao_addresses') }}

        UNION ALL 

        SELECT blockchain, dao_creator_tool, dao, dao_wallet_address, created_block_time, created_date
        FROM {{ ref('dao_addresses_polygon_syndicate') }}


)

SELECT * FROM mapping