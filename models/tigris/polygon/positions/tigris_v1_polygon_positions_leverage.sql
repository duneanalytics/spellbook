{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_polygon',
    alias = alias('positions_leverage')
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_open_position') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_modify_margin') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_limit_order') }}
)

SELECT * FROM leverage
