-- Check that SUM of ETH transfers during specific time frame is correct.

with test_data as (
    select round(sum(amount_raw) / 1e18, 0) as total
    from {{ ref('safe_optimism_eth_transfers') }}
    where block_time between TIMESTAMP '2022-11-01' and TIMESTAMP '2022-11-03'
),

test_result as (
    select case when total = -3 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
