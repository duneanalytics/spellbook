WITH unit_tests as
(SELECT case when test_data.amount = sharky_events.amount_original then True else False end as amount_test
FROM {{ ref('sharky_solana_events') }} sharky_events
JOIN {{ ref('sharky_solana_loans_solscan') }} test_data ON test_data.tx_hash = sharky_events.tx_hash
AND test_data.block_time = sharkyfi_events.block_time
WHERE sharky_events.block_time > '2023-01-06' and sharky_events.block_time < '2023-01-05'
and sharky_events.project = 'sharkify' and sharky_events.blockchain = 'solana'
)

select count(case when amount_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when amount_test = false then 1 else null end) > count(*)*0.1


