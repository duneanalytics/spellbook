{{config(
	tags=['legacy'],
	alias = alias('contracts', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "fantom", "polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT 'ethereum' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('ethereum','contracts') }} 
UNION 
SELECT 'gnosis' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('gnosis','contracts') }} 
UNION 
SELECT 'avalanche_c' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
        'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('avalanche_c','contracts') }} 
UNION 
SELECT 'arbitrum' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('arbitrum','contracts') }} 
UNION 
SELECT 'bnb' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('bnb','contracts') }} 
UNION 
SELECT 'optimism' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-26') as created_at,
       now() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('optimism','contracts') }} 
UNION 
SELECT 'fantom' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'Henrystats' as contributor,
       'query' AS source,
       date('2022-12-18') as created_at,
       now() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('fantom','contracts') }} 
UNION 
SELECT 'polygon' as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'contracts' as category,
       'Henrystats' as contributor,
       'query' AS source,
       date('2023-01-27') as created_at,
       now() as updated_at,
       'contracts' as model_name,
       'identifier' as label_type
FROM {{ source('polygon','contracts') }} 