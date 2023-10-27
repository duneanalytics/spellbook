-- Check that number of transfers in data range is correct
with test_data as (
    select count(*) as total
    from {{ ref('transfers_celo_erc20') }}
    where date(block_time) between date('2023-01-01') and date('2023-02-01')
),

test_result as (
    select case when total = 31091176 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
