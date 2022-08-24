-- Bootstrapped correctness test against legacy Caladan values.

WITH unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 22), 3) = round(token_balances.amount_raw/power(10, 22), 3) then True else False end as amount_raw_test
FROM {{ ref('balances_ethereum_erc20_specific_wallet') }} as test_data
JOIN (select * from {{ ref('balances_ethereum_erc20_hour') }} where wallet_address = '0xff0cefdbd6bf757cc0cc361ddfbde432186ccaa6') as token_balances
ON test_data.timestamp = token_balances.hour
AND test_data.wallet_address = token_balances.wallet_address
AND test_data.token_address = token_balances.token_address)


select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 1% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.01

