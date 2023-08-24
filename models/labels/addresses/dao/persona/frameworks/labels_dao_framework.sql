{{config(
    tags=['dunesql'],
    alias = alias('dao_framework')
)}}

WITH dao_address_w_name AS (
    SELECT
        blockchain,
        dao as address,
        CASE
            WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
            WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
            WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
            WHEN dao_creator_tool = 'syndicate' THEN 'DAO: Syndicate Investment Club'
        END as name

    FROM {{ ref('dao_addresses') }}
    WHERE dao_creator_tool != 'zodiac' -- excluding zodiac since they're gnosis safes
      AND dao != 0x  -- excluding empty addresses

    UNION  -- using a union because there are daos whose contract address also receives and send funds

    SELECT
    blockchain,
    dao_wallet_address as address,
    CASE
        WHEN dao_creator_tool = 'aragon' THEN 'DAO: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAO: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAO: DAO Haus'
    END as name
    FROM {{ ref('dao_addresses') }}
    WHERE dao_creator_tool NOT IN ('zodiac', 'syndicate') -- excluding syndicate since their wallet addresses are controlled by EOAs
                                                         -- excluding zodiac since they're gnosis safes
      AND dao_wallet_address != 0x  -- excluding empty addresses
)
SELECT 
    blockchain,
    address,
    name,
    'dao' as category,
    'henrystats' as contributor,
    'query' as source, 
    TIMESTAMP '2022-11-05' as created_at,
    now() as updated_at,
    'dao_framework' as model_name,
    'persona' as label_type
FROM dao_address_w_name
