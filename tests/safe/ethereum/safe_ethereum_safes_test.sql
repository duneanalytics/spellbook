-- Check that safes count on a specific date is correct

with test_data as (
    select count(*) as total
    from {{ ref('safe_ethereum_safes') }}
    where creation_time < TIMESTAMP '2022-08-03'
),

test_result as (
    select case when total = 85080 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
