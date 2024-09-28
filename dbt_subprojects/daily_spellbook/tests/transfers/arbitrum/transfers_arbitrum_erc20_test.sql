-- Check that number of transfers in data range is correct

with test_data as (
    select count(*) as total
    from {{ ref('transfers_arbitrum_erc20') }}
    where evt_block_time between timestamp '2023-01-01' and timestamp '2023-02-01'
),

test_result as (
    select case when total = 37276602 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
