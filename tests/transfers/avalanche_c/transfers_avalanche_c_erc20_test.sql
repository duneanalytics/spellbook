-- Check that number of transfers in data range is correct

with test_data as (
    select count(*) as total
    from {{ ref('transfers_avalanche_c_erc20') }}
    where evt_block_time between '2023-01-01' and '2023-02-01'
),

test_result as (
    select case when total = 15071548 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
