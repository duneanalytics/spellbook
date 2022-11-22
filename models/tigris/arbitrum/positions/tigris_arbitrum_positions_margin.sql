{{ config(
    materialized = 'view',
    alias = 'margin',
    unique_key = ['evt_block_time', 'position_id', 'margin']
    )
 }}

WITH 

margin as (
    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_arbitrum_events_add_margin') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        margin
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}

    UNION 

    SELECT 
        evt_block_time,
        position_id,
        new_margin as margin 
    FROM 
    {{ ref('tigris_arbitrum_positions_close') }}

)

SELECT * FROM margin  
