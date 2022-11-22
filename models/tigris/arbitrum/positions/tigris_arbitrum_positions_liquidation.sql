{{ config(
    materialized = 'view',
    alias = 'liquidation',
    unique_key = ['evt_block_time', 'position_id']
    )
 }}

WITH 

last_margin as (
        SELECT 
            MAX(evt_block_time) as latest_block_time, 
            position_id,
            margin 
        FROM 
        {{ ref('tigris_arbitrum_positions_margin') }}
        GROUP BY 2, 3
),

last_leverage as (
        SELECT 
            MAX(evt_block_time) as latest_block_time,
            position_id, 
            leverage
        FROM 
        {{ ref('tigris_arbitrum_positions_leverage') }}
        GROUP BY 2, 3 
)

SELECT 
    lp.*, 
    lm.margin, 
    ll.leverage 
FROM 
{{ ref('tigris_arbitrum_events_liquidate_position') }} lp 
INNER JOIN 
last_margin lm 
    ON lp.position_id = lp.position_id
INNER JOIN 
last_leverage ll 
    ON lp.position_id = ll.position_id
