-- Check that Eth transfers amount for safes on a specific date is correct

with unit_tests as (
    select round(sum(amount_raw) / 1e18, 0) as total
    from {{ ref('safe_ethereum_eth_transfers') }}
    where date between '2022-01-01' and '2022-01-03'
)

select case when total = 2996 then true else false end as test
from unit_tests
where test = false
