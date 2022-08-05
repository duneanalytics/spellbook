-- Check that safes count on a specific date is correct

with unit_tests as (
    select count(*) as total
    from {{ ref('safe_ethereum_safes') }}
    where creation_time between '2022-01-01' and '2022-08-03'
)

select case when total = 43704 then true else false end as test
from unit_tests
where test = false
