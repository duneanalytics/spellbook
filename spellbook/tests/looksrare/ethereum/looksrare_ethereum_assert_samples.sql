-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.original_amount = lr_trades.amount_original then True else False end as price_test
FROM {{ ref('looksrare_ethereum_trades') }} lr_trades
JOIN {{ ref('looksrare_ethereum_trades_postgres') }} test_data ON test_data.tx_hash = lr_trades.tx_hash
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05