{{ config(
    schema = 'thorchain_silver',
    alias = 'update_node_account_status_events',
    tags = ['thorchain', 'node_status', 'silver']
) }}

with base as (
    SELECT
        node_addr AS node_address,
        current AS current_status,  -- FIXED: Actual column name is 'current' not 'current_flag'
        former AS former_status,
        block_timestamp,
        event_id,        
        _ingested_at AS _inserted_timestamp,        
        ROW_NUMBER() OVER(
            PARTITION BY event_id
            ORDER BY _ingested_at DESC
        ) as rn
    FROM {{ source('thorchain', 'update_node_account_status_events') }}
)

SELECT 
    node_address,
    current_status,
    former_status,
    block_timestamp,
    event_id,
    _inserted_timestamp
FROM base
WHERE rn = 1