{{config(
    tags = ['dunesql'],
    schema = 'nft_ethereum',
    alias=alias('aggregators'),
     materialized='table',
      file_format = 'delta'
)}}


SELECT DISTINCT *
FROM(
    SELECT contract_address, name
    FROM {{ ref('nft_ethereum_aggregators_manual')}}
    UNION ALL
    SELECT contract_address, name
    FROM {{ ref('nft_ethereum_aggregators_gem')}}
)

