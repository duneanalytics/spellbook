{{ config(
    schema = 'thorchain_silver',
    alias = 'block_pool_depths',
    tags = ['thorchain', 'pool_depths', 'silver']
) }}

with base as (
    SELECT
        pool AS pool_name,
        asset_e8,
        rune_e8,
        synth_e8,
        block_timestamp,
        _ingested_at as _inserted_timestamp,
        ROW_NUMBER() OVER(
            PARTITION BY pool, block_timestamp 
            ORDER BY _ingested_at DESC
        ) as rn
    FROM {{ source('thorchain', 'block_pool_depths') }}
)

SELECT 
    pool_name,
    asset_e8,
    rune_e8,
    synth_e8,
    block_timestamp,
    _inserted_timestamp
FROM base
WHERE rn = 1