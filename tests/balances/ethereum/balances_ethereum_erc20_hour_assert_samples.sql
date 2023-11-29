-- Bootstrapped correctness test against legacy Caladan values.

with sampled_wallets as
 (
     select *
     from {{ ref('balances_ethereum_erc20_hour') }} bal
     where wallet_address in (select distinct cast(wallet_address as varbinary) from {{ ref('balances_ethereum_erc20_latest_entries') }})
     and bal.token_address in (0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9, 
                        0x6b175474e89094c44da98b954eedeac495271d0f,
                        0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
                         0xe41d2489571d322189246dafa5ebde1f4699f498) --'AAVE', 'DAI', 'UNI', 'LINK'
     and bal.block_hour > cast('2022-05-04' as date) and bal.block_hour < cast('2022-05-06' as date)
 )

, unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 22), 3) = round(token_balances.amount_raw/power(10, 22), 3) then True else False end as amount_raw_test
FROM {{ ref('balances_ethereum_erc20_latest_entries') }} as test_data
JOIN sampled_wallets as token_balances
ON test_data.timestamp = token_balances.hour
AND cast(test_data.wallet_address as varbinary) = cast(token_balances.wallet_address as varbinary)
AND cast(test_data.token_address as varbinary) = cast(token_balances.token_address as varbinary))

select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 5% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.05


