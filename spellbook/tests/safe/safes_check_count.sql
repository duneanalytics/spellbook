-- Check that safes count on a specific date is correct

with unit_tests as (
    select count(*)
    from {{ ref('safes_ethereum') }}
    where creation_time between '2022-08-02' and '2022-08-03'
)

select case when count(*) = 206 then true else false end as test
from unit_tests
where test = false
