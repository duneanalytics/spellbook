{{ config(
    alias = 'positions_leverage'
    )
 }}

WITH 

leverage as (
    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_arbitrum_events_open_position') }}

    UNION ALL

    SELECT 
        evt_block_time,
        position_id,
        leverage 
    FROM 
    {{ ref('tigris_arbitrum_events_modify_margin') }}
)

SELECT * FROM leverage 
;