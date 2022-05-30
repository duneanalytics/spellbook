-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check solscan info for the first 5 rows

WITH unit_tests as
(SELECT case when test_data.amount = os_trades.amount then True else False end as price_test
FROM {{ ref('magiceden_solana_trades') }} os_trades
JOIN {{ ref('magiceden_solana_trades_postgres') }} test_data ON test_data.tx_hash = os_trades.tx_hash
AND test_data.block_time = os_trades.block_time
)

select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.1


