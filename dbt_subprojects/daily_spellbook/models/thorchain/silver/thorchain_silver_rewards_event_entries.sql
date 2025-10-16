{{ config(
    schema = 'thorchain_silver',
    alias = 'rewards_event_entries',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id', 'pool_name', 'block_timestamp'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'rewards', 'silver']
) }}

with base as (
    SELECT
        pool AS pool_name,
        rune_e8,
        saver_e8,
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
            PARTITION BY event_id, pool, block_timestamp 
            ORDER BY _ingested_at DESC
        ) as rn

    FROM {{ source('thorchain', 'rewards_event_entries') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
)

SELECT 
    pool_name,
    rune_e8,
    saver_e8,
    event_id,
    block_timestamp,
    block_time,
    block_date,
    block_month,
    block_hour,
    _inserted_timestamp
FROM base
WHERE rn = 1
