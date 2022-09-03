{{config(alias='submitted_contracts')}}

SELECT array('ethereum') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('ethereum','contracts_submitted') }} 
UNION 
SELECT array('gnosis') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('gnosis','contracts_submitted') }} 
UNION 
SELECT array('avalanche_c') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('avalanche_c','contracts_submitted') }} 
UNION 
SELECT array('arbitrum') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('arbitrum','contracts_submitted') }} 
UNION 
SELECT array('bnb') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('bnb','contracts_submitted') }} 
UNION 
SELECT array('optimism') as blockchain,
       address, 
       name || '_' || namespace as name,
       'submitted_contracts' as category,
       'soispoke' as contributor,
       'static' AS source,
       date('2022-09-23') as created_at,
       now() as modified_at
FROM {{ source('optimism','contracts_submitted') }} 