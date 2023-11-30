-- Check that number of transfers in data range is correct

with test_data as (
    select count(*) as total
    from {{ ref('transfers_optimism_erc20') }}
    where evt_block_time between (cast '2023-01-01' as date) and cast('2023-02-01' as date)
),

test_result as (
    select case when total = 57679468 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
