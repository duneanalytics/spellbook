{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_arbitrum',
    alias = alias('positions_leverage', legacy_model=True)
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v1_arbitrum_events_open_position_legacy') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v1_arbitrum_events_modify_margin_legacy') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v1_arbitrum_events_limit_order_legacy') }}

)

SELECT * FROM leverage 
;