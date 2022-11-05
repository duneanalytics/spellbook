{{config(alias='daos')}}

SELECT 
    array(blockchain) as blockchain,
    dao as address, 
    CASE 
        WHEN dao_creator_tool = 'aragon' THEN 'DAOs: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAOs: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAOs: DAO Haus'
        WHEN dao_creator_tool = 'syndicate' THEN 'DAOs: Syndicate Investment Club'
    END as name,
    'daos' as category,
    'henrystats' as contributor,
    'daos-addresses-spell' as source, 
    timestamp('2022-11-05') as created_at,
    now() as updated_at
FROM 
{{ ref('daos_addresses') }}
WHERE dao_creator_tool != 'zodiac' -- excluding zodiac since they're gnosis safes

UNION  -- using a union because there are daos whose contract address also receives and send funds

SELECT 
    array(blockchain) as blockchain,
    dao_wallet_address as address, 
    CASE 
        WHEN dao_creator_tool = 'aragon' THEN 'DAOs: Aragon'
        WHEN dao_creator_tool = 'colony' THEN 'DAOs: Colony'
        WHEN dao_creator_tool = 'dao-haus' THEN 'DAOs: DAO Haus' 
    END as name,
    'daos' as category,
    'henrystats' as contributor,
    'daos-addresses-spell' as source, 
    timestamp('2022-11-05') as created_at,
    now() as updated_at
FROM 
{{ ref('daos_addresses') }}
WHERE dao_creator_tool NOT IN ('zodiac', 'syndicate') -- excluding syndicate since their wallet addresses are controlled by EOAs
                                                     -- excluding zodiac since they're gnosis safes






