{{config(alias='cex')}}

SELECT * FROM {{ ref('labels_cex_ethereum') }}

UNION All

SELECT * FROM {{ ref('labels_cex_bnb') }}

UNION All

-- add address list from CEXs
SELECT 
array("optimism"), address, distinct_name, 'cex', 'msilb7','static','2022-10-10'::timestamp,now()
FROM {{ ref('addresses_optimism_cex') }}