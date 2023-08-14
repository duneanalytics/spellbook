WITH unit_tests as
(SELECT case when test.evt_tx_hash = actual.evt_tx_hash 

                and test.evt_block_number = actual.evt_block_number
then True else False end as test
FROM {{ ref('nexusmutual_ethereum_quotation_trades') }} actual
JOIN {{ ref('nexusmutual_ethereum_trades_seed') }} test 
    ON test.tx_hash = actual.tx_hash AND test.evt_index = actual.evt_index
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1