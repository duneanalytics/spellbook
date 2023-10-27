-- Check that at least the official singletons are part of the return set.

with test_data as (
    select count(*) as num_official
    from {{ ref('safe_avalanche_c_singletons') }}
    where address in (
        0xfb1bffc9d739b8d520daf37df666da4c687191ea,
        0x69f4d1788e39c87893c980c06edf4b7f686e2938)
),

test_result as (
    select case when num_official = 2 then true else false end as success
    from test_data
)

select *
from test_result
where success = false
