{{ config(
    schema = 'thorchain_silver',
    alias = 'transfer_events',
    tags = ['thorchain', 'transfer_events', 'silver']
) }}

with base as (
    SELECT
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        amount_e8,
        event_id,
        block_timestamp,
        _updated_at as _inserted_timestamp,
        row_number() over(PARTITION BY event_id, from_addr, to_addr, asset, amount_e8, block_timestamp ORDER BY _updated_at DESC) as rn
    FROM
    {{ source('thorchain', 'transfer_events') }}
)
select *
from base
where rn = 1