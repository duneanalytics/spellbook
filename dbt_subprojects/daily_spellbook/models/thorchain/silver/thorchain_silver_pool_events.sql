{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'pool_events', 'silver']
) }}

-- CRITICAL: Use CTE pattern to avoid column resolution issues
with base as (
    SELECT
        asset,
        status,
        event_id,
        block_timestamp,
        
        -- Timestamp conversion (CRITICAL: nanoseconds to seconds)
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        
        -- Hevo ingestion timestamp conversion (milliseconds to timestamp)
        from_unixtime(cast(__HEVO__LOADED_AT / 1000 as bigint)) AS _inserted_timestamp,
        __HEVO__LOADED_AT,
        
        -- Row deduplication logic (convert QUALIFY to ROW_NUMBER for Trino)
        ROW_NUMBER() OVER(
            PARTITION BY event_id 
            ORDER BY __HEVO__LOADED_AT DESC
        ) as rn

    FROM {{ source('thorchain', 'pool_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
)

SELECT 
    asset,
    status,
    event_id,
    block_timestamp,
    block_time,
    block_date,
    block_month,
    block_hour,
    _inserted_timestamp
FROM base
WHERE rn = 1  -- Trino equivalent of QUALIFY
{% if is_incremental() %}
  AND {{ incremental_predicate('base.block_time') }}
{% endif %}
