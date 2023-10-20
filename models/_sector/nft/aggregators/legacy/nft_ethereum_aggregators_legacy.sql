{{config(
	tags=['legacy'],
	schema = 'nft_ethereum',
	alias = alias('aggregators',legacy_model=True),
	 materialized='table',
	  file_format = 'delta'
	  )}}

SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_manual_legacy')}}
UNION -- no union all to resolve any duplicates
SELECT contract_address, name
FROM {{ ref('nft_ethereum_aggregators_gem_legacy')}}

