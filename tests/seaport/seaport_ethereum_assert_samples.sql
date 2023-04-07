-- Bootstrapped correctness test against legacy Postgres values.

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.original_amount = seaport_trades.original_amount AND test_data.tx_hash = seaport_trades.tx_hash then True else False end as amount_test
FROM {{ ref('seaport_ethereum_view_transactions') }} seaport_trades
JOIN {{ ref('seaport_ethereum_view_transactions_postgres') }} test_data ON test_data.tx_hash = seaport_trades.tx_hash
)
select count(case when amount_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when amount_test = false then 1 else null end) > count(*)*0.05