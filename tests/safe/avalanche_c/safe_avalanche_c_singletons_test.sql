-- Check that at least the official singletons are part of the return set.

with test_data as (
    select count(*)
    from safe_avalanche_c.singletons
    where address in (
        '0xfb1bffc9d739b8d520daf37df666da4c687191ea',
        '0x69f4D1788e39c87893C980c06EdF4b7f686e2938')
),

test_result as (
    select case when total = 2 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
