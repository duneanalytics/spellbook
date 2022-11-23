{{ config(
    materialized = 'view',
    alias = 'polygon_positions_leverage',
    unique_key = ['evt_block_time', 'position_id', 'leverage', 'version']
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version
    FROM 
    {{ ref('tigris_polygon_events_open_position') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version 
    FROM 
    {{ ref('tigris_polygon_events_modify_margin') }}
)

SELECT * FROM leverage 