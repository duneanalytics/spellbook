-- Bootstrapped correctness test against legacy Caladan values.
-- Caladan query: "SELECT evt_block_time, evt_tx_hash, price FROM opensea."WyvernExchange_evt_OrdersMatched" ORDER BY evt_block_time DESC LIMIT 100"
-- Also manually check solscan info for the first 5 rows

WITH unit_tests as
(SELECT case when test_data.amount = os_trades.amount then True else False end as price_test
FROM {{ ref('opensea_solana_trades') }} os_trades
JOIN {{ ref('opensea_solana_trades_caladan') }} test_data ON test_data.tx_hash = os_trades.tx_hash
AND test_data.block_time = os_trades.block_time
)

select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.1


