{{ config(
    schema = 'thorchain_silver',
    alias = 'update_node_account_status_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'event_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'node_status', 'silver']
) }}

with base as (
    SELECT
        node_addr AS node_address,
        current AS current_status,  -- FIXED: Actual column name is 'current' not 'current_flag'
        former AS former_status,
        block_timestamp,
        event_id,
        
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

    FROM {{ source('thorchain', 'update_node_account_status_events') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '17' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
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
WHERE rn = 1
