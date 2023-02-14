{{config(alias='miners',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c","fantom","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT DISTINCT array('ethereum') as blockchain,
       miner, 
       'Ethereum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('ethereum','blocks') }} 
UNION 
SELECT DISTINCT array('gnosis') as blockchain,
       miner, 
       'Gnosis Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('gnosis','blocks') }} 
UNION 
SELECT DISTINCT array('avalanche_c') as blockchain,
       miner, 
       'Avalanche Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','blocks') }} 
UNION 
SELECT DISTINCT array('arbitrum') as blockchain,
       miner, 
       'Arbitrum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','blocks') }} 
UNION 
SELECT DISTINCT array('bnb') as blockchain,
       miner, 
       'BNB Chain Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('bnb','blocks') }} 
UNION 
SELECT DISTINCT array('optimism') as blockchain,
       miner, 
       'Optimism Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('optimism','blocks') }} 
UNION 
SELECT DISTINCT array('fantom') as blockchain,
       miner, 
       'Fantom Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2023-01-25') as created_at,
       now() as modified_at
FROM {{ source('fantom','blocks') }} 
UNION 
SELECT DISTINCT array('polygon') as blockchain,
       miner, 
       'Polygon Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2023-01-25') as created_at,
       now() as modified_at
FROM {{ source('polygon','blocks') }} 