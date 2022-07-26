-- Checking that all tokens from prices.usd_latest are in prices.usd

WITH unit_tests AS (
    SELECT CASE WHEN pu.minute IS NOT NULL THEN true ELSE false END AS presence_test
    FROM {{ ref('prices_latest') }} latest
    LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain=latest.blockchain
        AND pu.contract_address=latest.contract_address
    )

SELECT COUNT(CASE WHEN presence_test = false THEN 1 ELSE NULL END)/COUNT(*) AS pct_mismatch, COUNT(*) AS count_rows
FROM unit_tests
HAVING COUNT(CASE WHEN presence_test = false THEN 1 ELSE NULL END) > COUNT(*)*0.05