-- Bootstrapped correctness test against legacy Caladan values.

with sampled_wallets as (
    select *
    from {{ ref('balances_ethereum_erc20_hour') }}
    where
        wallet_address in (
            select distinct wallet_address
            from {{ ref('balances_ethereum_erc20_latest_entries') }}
        )
        and symbol in ('USDT', 'LINK', 'DAI', 'USDC')
),

unit_tests as (
    select coalesce(
            round(
                test_data.amount_raw / power(10, 22), 3
            ) = round(sampled_wallets.amount_raw / power(10, 22), 3),
            false) as amount_raw_test
    from {{ ref('balances_ethereum_erc20_latest_entries') }} as test_data
    inner join sampled_wallets
        on test_data.timestamp = sampled_wallets.hour
            and test_data.wallet_address = sampled_wallets.wallet_address
            and test_data.token_address = sampled_wallets.token_address
)

select
    count(
        case when amount_raw_test = false then 1 end
    ) / count(*) as pct_mismatch,
    count(*) as count_rows
from unit_tests
-- Having mismatches less than 5% of rows
having
    count(case when amount_raw_test = false then 1 end) > count(*) * 0.05
