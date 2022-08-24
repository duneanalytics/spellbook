-- Check that Eth transfers amount for safes on a specific date is correct

with test_data as (
    select round(sum(amount_raw) / 1e18, 0) as total
    from {{ ref('safe_ethereum_eth_transfers') }}
    where block_time between '2022-01-01' and '2022-01-03'
),

test_result as (
    select case when total = 2996 then true else false end as success
    from test_data    
)

select *
from test_result
where success = false
