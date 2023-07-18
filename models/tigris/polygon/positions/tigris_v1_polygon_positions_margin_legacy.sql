{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_polygon',
    alias = alias('positions_margin', legacy_model=True)
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
    {{ ref('tigris_v1_polygon_events_add_margin_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_modify_margin_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_open_position_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_positions_close_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin,
        version,
        evt_index
    FROM 
    {{ ref('tigris_v1_polygon_events_limit_order_legacy') }}

)

SELECT * FROM margin  
;