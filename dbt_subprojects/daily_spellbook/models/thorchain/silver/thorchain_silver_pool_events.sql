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

with base as (
    SELECT
        asset,
        status,
        event_id,
        block_timestamp,
        
        cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_month,
        date_trunc('hour', from_unixtime(cast(block_timestamp / 1e9 as bigint))) as block_hour,
        
        _updated_at AS _inserted_timestamp,
        _ingested_at,
        _updated_at,
        
        ROW_NUMBER() OVER(
            PARTITION BY event_id 
            ORDER BY _ingested_at DESC
        ) as rn

    FROM {{ source('thorchain', 'pool_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '16' day
)

SELECT 
    asset,
    status,
    event_id,
    block_timestamp,  -- Added for gold layer join
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
