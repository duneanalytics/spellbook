-- Bootstrapped correctness test against legacy Postgres values.
-- Postgres query: "SELECT evt_block_time, evt_tx_hash, price FROM opensea."WyvernExchange_evt_OrdersMatched" ORDER BY evt_block_time DESC LIMIT 100"
-- Also manually check etherscan info for the first 5 rows
WITH unit_tests as
(SELECT case when test_data.price = os_trades.amount_raw then True else False end as price_test
FROM {{ ref('nft_trades') }} os_trades
JOIN {{ ref('opensea_ethereum_trades_postgres') }} test_data ON test_data.evt_tx_hash = os_trades.tx_hash
WHERE os_trades.block_time > '2022-05-22' and os_trades.block_time < '2022-05-24'
and os_trades.project = 'opensea' and os_trades.blockchain = 'ethereum'
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05