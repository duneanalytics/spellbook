-- Check that number of transfers in data range is correct

with test_data as (
    select count(*) as total
    from {{ ref('transfers_polygon_erc20') }}
    where evt_block_time between TIMESTAMP '2023-01-01' and TIMESTAMP '2023-02-01'
),

test_result as (
    select case when total = 132014788 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
