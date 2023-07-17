{{ config(
    schema = 'tigris_v1_arbitrum',
    alias = alias('positions_margin')
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v1_arbitrum_events_add_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v1_arbitrum_events_modify_margin') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v1_arbitrum_events_open_position') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin 
    FROM 
    {{ ref('tigris_v1_arbitrum_positions_close') }}

    UNION ALL 

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_v1_arbitrum_events_limit_order') }}

)

SELECT * FROM margin  
;