{{ config(
    schema = 'thorchain_silver',
    alias = 'rewards_event_entries',
    tags = ['thorchain', 'rewards', 'silver']
) }}

with base as (
    SELECT
        pool AS pool_name,
        rune_e8,
        saver_e8,
        event_id,
        block_timestamp,        
        _ingested_at AS _inserted_timestamp,        
        ROW_NUMBER() OVER(
            PARTITION BY event_id, pool, block_timestamp
            ORDER BY _ingested_at DESC
        ) as rn
    FROM {{ source('thorchain', 'rewards_event_entries') }}
)

SELECT 
    pool_name,
    rune_e8,
    saver_e8,
    event_id,
    block_timestamp,
    _inserted_timestamp
FROM base
WHERE rn = 1