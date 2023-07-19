{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_polygon',
    alias = alias('positions_margin')
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_add_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_modify_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_open_position') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_positions_close') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_limit_order') }}

)

SELECT * FROM margin  
