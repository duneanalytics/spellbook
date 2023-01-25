{{config(alias='contracts',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT array('ethereum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('ethereum','contracts') }} 
UNION 
SELECT array('gnosis') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('gnosis','contracts') }} 
UNION 
SELECT array('avalanche_c') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','contracts') }} 
UNION 
SELECT array('arbitrum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','contracts') }} 
UNION 
SELECT array('bnb') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('bnb','contracts') }} 
UNION 
SELECT array('optimism') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as modified_at
FROM {{ source('optimism','contracts') }} 
UNION 
SELECT array('fantom') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'Henrystats' as contributor,
       'query' AS source,
       date('2022-12-18') as created_at,
       now() as modified_at
FROM {{ source('fantom','contracts') }} 