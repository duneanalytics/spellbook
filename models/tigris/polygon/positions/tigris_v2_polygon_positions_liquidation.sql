{{ config(
    tags=['dunesql'],
    schema = 'tigris_v2_polygon',
    alias = alias('positions_liquidation')
    )
 }}

WITH 

last_margin as (
        SELECT 
            xx.evt_block_time,
            xx.position_id,
            xy.margin 
        FROM 
        (
        SELECT 
            MAX(evt_block_time) as evt_block_time,
            position_id
        FROM 
        {{ ref('tigris_v2_polygon_positions_margin') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v2_polygon_positions_margin') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
),

last_leverage as (
        SELECT 
            xx.evt_block_time,
            xx.position_id,
            xy.leverage 
        FROM 
        (
        SELECT 
            MAX(evt_block_time) as evt_block_time,
            position_id
        FROM 
        {{ ref('tigris_v2_polygon_positions_leverage') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v2_polygon_positions_leverage') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
)

SELECT 
    lp.*, 
    lm.margin, 
    ll.leverage 
FROM 
{{ ref('tigris_v2_polygon_events_liquidate_position') }} lp 
INNER JOIN 
last_margin lm 
    ON lp.position_id = lm.position_id
INNER JOIN 
last_leverage ll 
    ON lp.position_id = ll.position_id
