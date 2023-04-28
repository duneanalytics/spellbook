WITH unit_tests as
(SELECT case when test.taker_symbol = actual.taker_symbol 
               
                and test.taker = actual.taker
then True else False end as test
FROM {{ ref('zeroex_api_fills_deduped') }} actual 
JOIN {{ ref('zeroex_api_fills_deduped_sample') }} test 
    ON test.tx_hash = actual.tx_hash AND test.evt_index = actual.evt_index
    where test.taker_symbol is not null and test.taker is not null
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1