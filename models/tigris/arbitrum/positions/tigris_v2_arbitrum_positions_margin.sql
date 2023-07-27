{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v2_arbitrum',
    alias = alias('positions_margin', legacy_model=True)
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v2_arbitrum_events_add_margin_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v2_arbitrum_events_modify_margin_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v2_arbitrum_events_open_position_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin 
    FROM 
    {{ ref('tigris_v2_arbitrum_positions_close_legacy') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v2_arbitrum_events_limit_order_legacy') }}

)

SELECT * FROM margin  
;