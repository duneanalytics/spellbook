-- Check that number of Safes on a specific date is correct.

with test_data as (
    select count(*) as total
    from {{ ref('safe_goerli_safes') }}
    where creation_time < TIMESTAMP '2023-02-14'
),

test_result as (
    select case when total = 23059 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
