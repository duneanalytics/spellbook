{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_polygon',
    alias = alias('positions_leverage', legacy_model=True)
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
    {{ ref('tigris_v1_polygon_events_open_position_legacy') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_modify_margin_legacy') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        leverage, 
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_limit_order_legacy') }}
)

SELECT * FROM leverage
; 