-- Bootstrapped correctness test against values downloaded from the Dune App
-- The first 10 values were also manually checked using Solscan API

WITH unit_tests as
(SELECT case when test_data.amount = me_trades.amount_original then True else False end as price_test
FROM {{ ref('magiceden_solana_events') }} me_trades
JOIN {{ ref('magiceden_solana_trades_solscan') }} test_data ON from_base58(test_data.tx_hash) = me_trades.tx_hash
AND test_data.block_time = me_trades.block_time
WHERE me_trades.block_time > timestamp '2021-10-23' and me_trades.block_time < timestamp '2021-10-25'
and me_trades.project = 'magiceden' and me_trades.blockchain = 'solana'
)

select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.1


