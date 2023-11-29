-- Bootstrapped correctness test against legacy Caladan values.

WITH unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 22), 3) = round(token_balances.amount_raw/power(10, 22), 3) then True else False end as amount_raw_test
FROM {{ ref('balances_base_erc20_specific_wallet') }} as test_data
JOIN (select * from {{ ref('balances_base_erc20_hour') }} where wallet_address = 0x1b72bac3772050fdcaf468cce7e20deb3cb02d89) as token_balances
ON test_data.timestamp = token_balances.block_hour
AND cast(test_data.wallet_address as varbinary) = token_balances.wallet_address
AND cast(test_data.token_address as varbinary)  = token_balances.token_address)


select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 1% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.01

