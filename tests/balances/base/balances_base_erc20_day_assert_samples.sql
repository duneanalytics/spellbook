-- Bootstrapped correctness test against legacy Caladan values.



with sampled_wallets as
 (
     select *
     from {{ ref('balances_base_erc20_day') }} bal
     where wallet_address in (select distinct cast(wallet_address as varbinary) from {{ ref('balances_base_erc20_daily_entries')  }})
     and bal.block_day > cast('2023-09-04' as date) and bal.block_hour < cast('2023-09-06' as date)
 )

, unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 18), 4) = round(token_balances.amount_raw/power(10, 18), 4) then True else False end as amount_raw_test
FROM {{ ref('balances_base_erc20_daily_entries') }} as test_data
JOIN sampled_wallets as token_balances
ON test_data.timestamp = token_balances.block_day
AND cast(test_data.wallet_address as varbinary) = cast(token_balances.wallet_address as varbinary)
AND cast(test_data.token_address as varbinary) = cast(token_balances.token_address as varbinary))

select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 1% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.01
     select *
     from {{ ref('balances_base_erc20_hour') }} bal
     where wallet_address in (select distinct wallet_address from {{ ref('balances_base_erc20_latest_entries') }})
     and bal.token_address in ( 0x50c5725949a6f0c72e6c4a641f24049a917db0cb,
                        0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca,
                         0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913) --'DAI', 'USDbc', 'USDC'
     and bal.block_hour > cast('2023-09-04' as date) and bal.block_hour < cast('2023-09-06' as date)
 

, unit_tests as
(SELECT case when round(test_data.amount_raw/power(10, 22), 3) = round(token_balances.amount_raw/power(10, 22), 3) then True else False end as amount_raw_test
FROM {{ ref('balances_base_erc20_latest_entries') }} as test_data
JOIN sampled_wallets as token_balances
ON test_data.timestamp = token_balances.hour
AND cast(test_data.wallet_address as varbinary) = cast(token_balances.wallet_address as varbinary)
AND cast(test_data.token_address as varbinary) = cast(token_balances.token_address as varbinary))

select count(case when amount_raw_test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
-- Having mismatches less than 5% of rows
having count(case when amount_raw_test = false then 1 else null end) > count(*)*0.05


