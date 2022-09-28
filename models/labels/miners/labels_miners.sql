{{config(alias='miners',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT array('ethereum') as blockchain,
       miner, 
       'Ethereum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('ethereum','blocks') }} 
UNION 
SELECT array('gnosis') as blockchain,
       miner, 
       'Gnosis Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('gnosis','blocks') }} 
UNION 
SELECT array('avalanche_c') as blockchain,
       miner, 
       'Avalanche Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','blocks') }} 
UNION 
SELECT array('arbitrum') as blockchain,
       miner, 
       'Arbitrum Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','blocks') }} 
UNION 
SELECT array('bnb') as blockchain,
       miner, 
       'BNB Chain Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('bnb','blocks') }} 
UNION 
SELECT array('optimism') as blockchain,
       miner, 
       'Optimism Miner' as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as modified_at
FROM {{ source('optimism','blocks') }} 