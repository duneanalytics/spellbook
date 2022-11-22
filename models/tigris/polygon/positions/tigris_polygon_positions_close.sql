{{ config(
    materialized = 'view',
    alias = 'polygon_positions_close',
    unique_key = ['latest_leverage_time', 'evt_tx_hash', 'position_id', 'payout']
    )
 }}

WITH 

latest_leverage as (
    SELECT 
        ab.*, 
        l.leverage 
    FROM 
    (
    SELECT 
        MIN(l.evt_block_time) as latest_leverage_time, 
        cp.evt_block_time, 
        cp.evt_tx_hash,
        cp.position_id,
        cp.payout, 
        cp.evt_index,
        cp.version,
        cp.price, 
        cp.trader, 
        (100/cp.perc_closed) * cp.payout as previous_margin, 
        ((100/cp.perc_closed) * cp.payout) - cp.payout as new_margin
    FROM 
    {{ ref('tigris_polygon_events_close_position') }} cp 
    INNER JOIN 
    {{ ref('tigris_polygon_positions_leverage') }} l 
        ON cp.position_id = l.position_id 
        AND cp.evt_block_time > l.evt_block_time
    GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    ) ab 
    INNER JOIN 
    {{ ref('tigris_polygon_positions_leverage') }} l 
        ON ab.position_id = l.position_id
        AND ab.latest_leverage_time = l.evt_block_time
)

SELECT * FROM latest_leverage