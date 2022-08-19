-- Check that all withdrawal txs are unique
WITH unit_tests as
(SELECT case when COUNT(*) > 1 then True else False end as price_test
FROM {{ ref('tornado_cash_withdrawals') }} tc_d
GROUP BY depositor, blockchain, currency_contract, currency_symbol, block_time, evt_index
)
select count(case when price_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when price_test = false then 1 else null end) > count(*)*0.05