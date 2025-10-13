{{ config(
    schema = 'thorchain_silver',
    alias = 'block_pool_depths',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool_name', 'block_time'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'pool_depths', 'silver']
) }}

-- CRITICAL: Use CTE pattern to avoid column resolution issues
with base as (
    SELECT
        pool AS pool_name,
        asset_e8,
        rune_e8,
        synth_e8,
        
        -- Timestamp conversion (CRITICAL: nanoseconds to seconds for block_timestamp)
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        block_timestamp as raw_block_timestamp,
        
        -- Asset scaling (human readable versions)
        asset_e8 / 1e8 as asset_amount,
        rune_e8 / 1e8 as rune_amount,
        synth_e8 / 1e8 as synth_amount,
        
        -- Use actual Dune source audit timestamps
        _updated_at AS _inserted_timestamp,
        _ingested_at,
        _updated_at,
        
        -- Row deduplication logic (using actual available column)
        ROW_NUMBER() OVER(
            PARTITION BY pool, block_timestamp 
            ORDER BY _ingested_at DESC
        ) as rn

    FROM {{ source('thorchain', 'block_pool_depths') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '15' day
)

SELECT 
    pool_name,
    asset_e8,
    rune_e8,
    synth_e8,
    asset_amount,
    rune_amount,
    synth_amount,
    block_time,
    block_date,
    block_month,
    block_hour,
    raw_block_timestamp,
    _inserted_timestamp
FROM base
WHERE rn = 1  -- Trino equivalent of QUALIFY
{% if is_incremental() %}
  AND {{ incremental_predicate('base.block_time') }}
{% endif %}
