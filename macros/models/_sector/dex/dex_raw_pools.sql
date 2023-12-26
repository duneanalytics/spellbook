{% macro dex_raw_pools() %}

select * from {{ ref('dex_raw_pool_creations') }}
where pool not in (
    select pool from {{ ref('dex_raw_pool_initializations') }}
    group by pool 
    having count(*) > 1
)

{% endmacro %}