-- Bootstrapped correctness test against legacy Postgres values.
-- Postgres query: "SELECT block_time, tx_hash, token_a_amount, token_b_amount FROM dex.trades 
-- WHERE project = 'Uniswap' AND version = '3' 
-- ORDER BY block_time DESC LIMIT 1000"

-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.token_a_amount = us_trades.token_a_amount AND test_data.token_b_amount = us_trades.token_b_amount then True else False end as price_test
FROM {{ ref('uniswap_v3_ethereum_trades') }} us_trades
JOIN {{ ref('uniswap_v3_ethereum_trades_postgres') }} test_data ON test_data.tx_hash = us_trades.tx_hash
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05