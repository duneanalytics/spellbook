{{config(tags=['dunesql'],alias = alias('cex'))}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION All

SELECT * FROM {{ ref('labels_cex_bnb') }}

UNION All

-- add address list from CEXs
SELECT 
    'optimism'
     , address
     , distinct_name
     , 'institution'
     , 'msilb7'
     , 'static'
     , DATE '2022-10-10'
     , now()
     , 'cex_optimism'
     , 'identifier'
FROM { ref('addresses_optimism_cex_legacy') } -- {{ ref('addresses_optimism_cex') }}

UNION All

SELECT * FROM {{ ref('labels_cex_fantom') }}