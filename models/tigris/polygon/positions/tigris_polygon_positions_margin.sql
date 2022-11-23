{{ config(
    materialized = 'view',
    alias = 'polygon_positions_margin',
    unique_key = ['evt_block_time', 'position_id', 'margin', 'version']
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin,
        version
    FROM 
    {{ ref('tigris_polygon_events_add_margin') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version
    FROM 
    {{ ref('tigris_polygon_events_modify_margin') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version
    FROM 
    {{ ref('tigris_polygon_events_open_position') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin,
        version
    FROM 
    {{ ref('tigris_polygon_positions_close') }}

)

SELECT * FROM margin  
