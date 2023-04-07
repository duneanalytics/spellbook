WITH unit_tests as
(SELECT case when test.maker_token = actual.maker_token 
                and test.taker = actual.taker 
                and test.taker_token = actual.taker_token
then True else False end as test
FROM {{ ref('zeroex_polygon_api_fills_deduped') }} actual
JOIN {{ ref('zeroex_polygon_api_fills_deduped_sample') }} test 
    ON test.tx_hash = actual.tx_hash AND test.evt_index = actual.evt_index
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1