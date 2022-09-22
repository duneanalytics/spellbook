{{config(alias='submitted_contracts',
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')
}}

SELECT array('ethereum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('ethereum','contracts_submitted') }} 
UNION 
SELECT array('gnosis') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('gnosis','contracts_submitted') }} 
UNION 
SELECT array('avalanche_c') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','contracts_submitted') }} 
UNION 
SELECT array('arbitrum') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','contracts_submitted') }} 
UNION 
SELECT array('bnb') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('bnb','contracts_submitted') }} 
UNION 
SELECT array('optimism') as blockchain,
       address, 
       concat(upper(substring(namespace,1,1)),substring(namespace,2)) || ': ' || name as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'query' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('optimism','contracts_submitted') }} 