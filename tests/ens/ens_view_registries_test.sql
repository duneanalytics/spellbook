-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS (
    SELECT 
        CASE WHEN test_data.label = ens_vr.label THEN True 
        ELSE False 
        END AS min_evt_block_time_test
    FROM {{ ref('ens_view_registries') }} AS ens_vr
    JOIN {{ ref('ens_view_registries_postgres') }} AS test_data 
        ON test_data.label = ens_vr.label and test_data.min_evt_block_time = ens_vr.min_evt_block_time  
)
SELECT
    COUNT(*) AS count_rows,
    COUNT(CASE WHEN min_evt_block_time_test = FALSE THEN 1 ELSE NULL END)/COUNT(*) AS pct_mismatch
FROM unit_tests 
HAVING COUNT(CASE WHEN min_evt_block_time_test = FALSE THEN 1 ELSE NULL END) > COUNT(*)*0.05