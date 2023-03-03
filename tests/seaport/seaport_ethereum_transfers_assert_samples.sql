-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests AS
(SELECT CASE WHEN test_data.original_amount = seaport_transfers.amount_original 
    AND test_data.tx_hash = seaport_transfers.tx_hash 
    AND test_data.block_time = seaport_transfers.block_time 
    THEN True ELSE False END AS amount_test
FROM {{ ref('seaport_ethereum_transfers') }} seaport_transfers
    JOIN {{ ref('seaport_ethereum_transfers_postgres') }} test_data 
    ON test_data.tx_hash = seaport_transfers.tx_hash
    AND seaport_transfers.block_time > '2022-06-14' AND seaport_transfers.block_time < '2022-06-16'

)
SELECT count(CASE WHEN amount_test = false THEN 1 ELSE NULL END)/count(*) AS pct_mismatch, count(*) AS COUNT_ROWS
FROM unit_tests
HAVING count(CASE WHEN amount_test = false THEN 1 ELSE NULL END) > count(*)*0.05