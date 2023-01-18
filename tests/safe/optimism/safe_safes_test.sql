-- Check that safes count on a specific date is correct

with test_data as (
    select count(distinct address) as total
    from {{ ref('safe_optimism_safes') }}
    where creation_time < '2022-01-01'
),

test_result as (
    select case when total = 36 then true else false end as success
    from test_data
)

select *
from test_result
where success = false