{{ config(
    materialized = 'view',
    alias = 'close',
    unique_key = ['latest_leverage_time', 'evt_tx_hash', 'position_id', 'payout']
    )
 }}

WITH 

latest_leverage as (
    SELECT 
        MIN(l.evt_block_time) as latest_leverage_time, 
        l.leverage as leverage, 
        cp.evt_block_time, 
        cp.evt_tx_hash,
        cp.position_id,
        cp.payout, 
        (100/cp.perc_closed) * cp.payout as previous_margin, 
        ((100/cp.perc_closed) * cp.payout) - cp.payout as new_margin 
    FROM 
    {{ ref('tigris_arbitrum_events_close_position') }} cp 
    INNER JOIN 
    {{ ref('tigris_arbitrum_positions_leverage') }} l 
        ON cp.position_id = l.position_id 
        AND cp.evt_block_time > l.evt_block_time
    GROUP BY 2, 3, 4, 5, 6, 7, 8 
)

SELECT * FROM latest_leverage