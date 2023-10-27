-- Bootstrapped correctness test against values downloaded from the Dune App
-- The first 10 values were also manually checked using Solscan API

WITH unit_tests as
(SELECT case when test_data.amount = os_trades.amount_original then True else False end as price_test
FROM {{ ref('opensea_solana_events') }} os_trades
JOIN {{ ref('opensea_solana_trades_solscan') }} test_data ON from_base58(test_data.tx_hash) = os_trades.tx_hash
AND test_data.block_time = os_trades.block_time
WHERE os_trades.block_time > timestamp '2022-05-01' and os_trades.block_time < timestamp '2022-05-03'
and os_trades.project = 'opensea' and os_trades.blockchain = 'solana'
)

select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.1


