-- Bootstrapped correctness test against legacy v1 values.
-- Also manually check etherscan info for the first 5 rows

-- Note: there are data discrepancies on V1. Some values in this test seed could be incorrect.
-- We estimate there should be a min. 95% match. If you are confident, the discrepancy rate is higher, reach out to us.
WITH unit_tests as
(SELECT case when test_data.original_amount = trades.amount_original then True else False end as price_test
FROM {{ ref('kyber_polygon_trades') }} trades
JOIN {{ ref('kyber_polygon_trades_postgres') }} test_data ON test_data.tx_hash = trades.tx_hash
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05