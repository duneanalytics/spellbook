WITH unit_tests as
(SELECT case when test.token_bought_symbol = actual.token_bought_symbol 
               
                and test.token_sold_symbol = actual.token_sold_symbol
then True else False end as test
FROM {{ ref('zeroex_api_fills_deduped') }} actual 
JOIN {{ ref('zeroex_api_fills_deduped') }} test 
    ON test.tx_hash = actual.tx_hash AND test.evt_index = actual.evt_index
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1