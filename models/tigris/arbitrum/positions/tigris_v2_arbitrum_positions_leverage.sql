{{ config(
    tags=['dunesql'],
    schema = 'tigris_v2_arbitrum',
    alias = alias('positions_leverage')
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v2_arbitrum_events_open_position') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v2_arbitrum_events_modify_margin') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_v2_arbitrum_events_limit_order') }}

)

SELECT * FROM leverage 
