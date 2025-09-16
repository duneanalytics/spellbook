-- Check that number of Safe transactions in specific date range is correct.

with test_data as (
    select count(*) as total
    from {{ ref('safe_gnosis_transactions') }}
    where block_time > TIMESTAMP '2023-01-01'
        and block_time < TIMESTAMP '2023-02-01'
),

test_result as (
    select case when total = 21535 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
