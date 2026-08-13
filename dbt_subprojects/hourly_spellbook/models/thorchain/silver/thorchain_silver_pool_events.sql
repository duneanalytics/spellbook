{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_events',
    tags = ['thorchain', 'pool_events', 'silver']
) }}

with base as (
    SELECT
        asset,
        status,
        event_id,
        block_timestamp,
        row_number() over(PARTITION BY event_id ORDER BY _updated_at DESC) as rn
    FROM
        {{ source('thorchain', 'pool_events') }}
)
select *
from base
where rn = 1