WITH unit_tests as
(SELECT case when test.premium = actual.premium
then True else False end as test
FROM {{ ref('nexusmutual_ethereum_quotation_trades') }} actual
JOIN {{ ref('nexusmutual_ethereum_trades_seed') }} test 
    ON test.evt_tx_hash = actual.evt_tx_hash AND test.evt_block_number = actual.evt_block_number
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1