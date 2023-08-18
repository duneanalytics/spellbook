{{ config(
    tags=['dunesql'],
    schema = 'tigris_polygon',
    alias = alias('positions_leverage')
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_polygon_events_open_position') }}
    WHERE open_type = 'open_position'

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_polygon_events_modify_margin') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage,
        project_contract_address,
        version,
        positions_contract
    FROM 
    {{ ref('tigris_polygon_events_limit_order') }}
)

SELECT 
    l.*
FROM 
leverage l 
