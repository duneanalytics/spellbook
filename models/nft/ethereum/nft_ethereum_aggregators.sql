{{config(alias='aggregators', materialized='table', file_format = 'delta')}}

SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_manual')}}
UNION -- no union all to resolve any duplicates
SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_gem')}}

