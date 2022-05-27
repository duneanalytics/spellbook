-- Bootstrapped correctness test against legacy Caladan values.

with sampled_wallets as
 (
     select *
     from {{ ref('balances_ethereum_erc20_hour') }}
     where wallet_address in (select distinct wallet_address from {{ ref('balances_ethereum_erc20_latest_entries') }})
     and symbol in ('USDT', 'LINK', 'DAI', 'USDC')
 )

, unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 22), 3) = round(token_balances.amount_raw/power(10, 22), 3) then True else False end as amount_raw_test
FROM {{ ref('balances_ethereum_erc20_latest_entries') }} as test_data
JOIN sampled_wallets as token_balances
ON test_data.timestamp = token_balances.hour
AND test_data.wallet_address = token_balances.wallet_address
AND test_data.token_address = token_balances.token_address)

select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 5% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.05


