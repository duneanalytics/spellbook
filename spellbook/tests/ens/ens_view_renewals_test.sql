-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT 
        CASE WHEN test_data.label = ens_vr.label THEN True 
        ELSE False 
        END AS evt_block_number_test
    FROM {{ ref('ens_view_renewals') }} AS ens_vr
    JOIN {{ ref('ens_view_renewals_postgres') }} AS test_data 
        ON test_data.evt_block_number = ens_vr.evt_block_number
        AND test_data.evt_index = ens_vr.evt_index
)
SELECT
    COUNT(*) AS count_rows,
    COUNT(CASE WHEN evt_block_number_test = FALSE THEN 1 ELSE NULL END)/COUNT(*) AS pct_mismatch
FROM unit_tests 
HAVING COUNT(CASE WHEN evt_block_number_test = FALSE THEN 1 ELSE NULL END) > COUNT(*)*0.05