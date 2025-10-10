{{ config(
    schema = 'thorchain_silver',
    alias = 'update_node_account_status_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'node_status', 'silver']
) }}

-- CRITICAL: Use CTE pattern to avoid column resolution issues
with base as (
    SELECT
        node_addr AS node_address,
        current_flag AS current_status,
        former AS former_status,
        block_timestamp,
        event_id,
        
        -- Timestamp conversion (CRITICAL: nanoseconds to seconds)
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        
        -- Hevo ingestion timestamp conversion (milliseconds to timestamp)
        from_unixtime(cast(__hevo__loaded_at / 1000 as bigint)) AS _inserted_timestamp,
        __hevo__loaded_at,
        
        -- Row deduplication logic (convert QUALIFY to ROW_NUMBER for Trino)
        ROW_NUMBER() OVER(
            PARTITION BY event_id
            ORDER BY __hevo__loaded_at DESC
        ) as rn

    FROM {{ source('thorchain', 'update_node_account_status_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
)

SELECT 
    node_address,
    current_status,
    former_status,
    block_timestamp,
    event_id,
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
