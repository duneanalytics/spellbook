{{ config(
    alias = 'positions_leverage'
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
    {{ ref('tigris_polygon_events_open_position') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_polygon_events_modify_margin') }}
)

SELECT * FROM leverage
; 