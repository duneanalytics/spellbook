-- Bootstrapped correctness test against legacy Postgres values.
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.amount_original = x2y2_trades.amount_original then True else False end as price_test
FROM {{ ref('nft_ethereum_trades_beta_ported') }} x2y2_trades
JOIN {{ ref('x2y2_ethereum_trades_etherscan') }} test_data ON test_data.tx_hash = x2y2_trades.tx_hash
WHERE project = 'x2y2' and x2y2_trades.block_time = '2022-07-20' or x2y2_trades.block_time = '2022-06-09'
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05
