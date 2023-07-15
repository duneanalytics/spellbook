{{config(tags=['dunesql'],
        alias = alias('miners'),
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c","fantom","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT DISTINCT 'ethereum' as blockchain,
       miner as address,
       'Ethereum Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
       'miners' AS model_name,
       'persona' as label_type
FROM {{ source('ethereum','blocks') }} 
UNION 
SELECT DISTINCT 'gnosis' as blockchain,
       miner as address,
       'Gnosis Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
       'miners' AS model_name,
       'persona' as label_type
FROM {{ source('gnosis','blocks') }} 
UNION 
SELECT DISTINCT 'avalanche_c' as blockchain,
       miner as address,
       'Avalanche Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
       'miners' AS model_name,
       'persona' as label_type
FROM {{ source('avalanche_c','blocks') }} 
UNION 
SELECT DISTINCT 'arbitrum' as blockchain,
       miner as address,
       'Arbitrum Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
       'miners' AS model_name,
       'persona' as label_type
FROM {{ source('arbitrum','blocks') }} 
UNION 
SELECT DISTINCT 'bnb' as blockchain,
       miner as address,
       'BNB Chain Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
        'miners' AS model_name,
       'persona' as label_type
FROM {{ source('bnb','blocks') }} 
UNION 
SELECT DISTINCT 'optimism' as blockchain,
       miner as address,
       'Optimism Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-28') as created_at,
       now() as updated_at,
        'miners' AS model_name,
       'persona' as label_type
FROM {{ source('optimism','blocks') }} 
UNION 
SELECT DISTINCT 'fantom' as blockchain,
       miner as address,
       'Fantom Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2023-01-25') as created_at,
       now() as updated_at,
        'miners' AS model_name,
       'persona' as label_type
FROM {{ source('fantom','blocks') }} 
UNION 
SELECT DISTINCT 'polygon' as blockchain,
       miner as address,
       'Polygon Miner' as name,
       'infrastructure' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2023-01-25') as created_at,
       now() as updated_at,
       'miners' AS model_name,
       'persona' as label_type
FROM {{ source('polygon','blocks') }} 