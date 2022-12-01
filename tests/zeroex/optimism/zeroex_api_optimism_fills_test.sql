WITH unit_tests as
(SELECT case when test.maker_token_address = actual.maker_token_address 
                and test.taker_token_amount = actual.taker_token_amount 
                and test.taker_token_address = actual.taker_token_address 
then True else False end as test
FROM {{ ref('zeroex_api_optimism_fills_sample') }} actual
JOIN {{ ref('zeroex_api_optimism_fills_sample') }} test ON test.tx_hash = actual.tx_hash
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1