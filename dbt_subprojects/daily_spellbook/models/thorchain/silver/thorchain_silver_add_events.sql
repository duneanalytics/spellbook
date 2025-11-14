{{ config(
    schema = 'thorchain_silver',
    alias = 'add_events',
    tags = ['thorchain', 'liquidity', 'add_events']
) }}

with base as (
    SELECT
        tx AS tx_id,
        chain AS blockchain,
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        asset_e8,
        memo,
        rune_e8,
        pool AS pool_name,
        event_id,
        block_timestamp,
        _TX_TYPE,
        row_number() over(PARTITION BY event_id, tx, chain, from_addr, to_addr, asset, memo, pool, block_timestamp ORDER BY _updated_at DESC) as rn
    FROM
        {{ source('thorchain', 'add_events') }}
)
SELECT *
FROM base
WHERE rn = 1