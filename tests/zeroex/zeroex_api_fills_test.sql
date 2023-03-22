WITH unit_tests as
(SELECT case when test.taker_symbol = actual.taker_symbol 
               
                and test.maker_symbol = actual.maker_symbol
then True else False end as test
FROM {{ ref('zeroex_api_fills') }} actual 
JOIN {{ ref('zeroex_api_fills') }} test 
    ON test.tx_hash = actual.tx_hash AND test.evt_index = actual.evt_index
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1