{{ config(
    materialized = 'view',
    alias = 'liquidation',
    unique_key = ['evt_block_time', 'position_id']
    )
 }}

WITH 

last_margin as (
        SELECT 
            *
        FROM 
        (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY position_id ORDER BY evt_block_time DESC) as rank_, 
            position_id,
            margin 
        FROM 
        (
        SELECT 
            position_id,
            margin, 
            evt_block_time
        FROM 
        {{ ref('tigris_arbitrum_positions_margin') }}
        ) xx 
        ) x 
        WHERE x.rank_ = 1 
),

last_leverage as (
        SELECT 
            *
        FROM 
        (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY position_id ORDER BY evt_block_time DESC) as rank_, 
            position_id,
            leverage 
        FROM 
        (
        SELECT 
            position_id,
            leverage, 
            evt_block_time
        FROM 
        {{ ref('tigris_arbitrum_positions_leverage') }}
        ) xx 
        ) x 
        WHERE x.rank_ = 1 
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
