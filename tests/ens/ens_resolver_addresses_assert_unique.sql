-- Check if each ENS is only present once
WITH ens_occurence AS (
    SELECT ens_name
    , COUNT(*) AS occurence
    FROM {{ ref('ens_resolver_addresses') }}
    GROUP BY ens_name
)

SELECT *
FROM ens_occurence
WHERE occurence > 1