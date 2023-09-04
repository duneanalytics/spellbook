{{ config(
    tags=['dunesql'],
    schema = 'tigris_arbitrum',
    alias = alias('positions_close')
    )
 }}

WITH 

get_positions_contract as (
    SELECT 
        *
    FROM 
    {{ ref('tigris_arbitrum_events_close_position') }}
), 

latest_leverage_time as (
        SELECT 
            MAX(l.evt_block_time) as latest_leverage_time, 
            cp.evt_block_time, 
            cp.evt_tx_hash, 
            cp.evt_index, 
            cp.position_id, 
            cp.positions_contract
        FROM 
        get_positions_contract cp 
        INNER JOIN 
        {{ ref('tigris_arbitrum_positions_leverage') }} l 
            ON cp.position_id = l.position_id
            AND cp.evt_block_time > l.evt_block_time
            AND cp.positions_contract = l.positions_contract
        GROUP BY 2, 3, 4, 5, 6
),

latest_leverage as (
        SELECT 
            llt.*, 
            l.leverage 
        FROM 
        latest_leverage_time llt 
        INNER JOIN 
        {{ ref('tigris_arbitrum_positions_leverage') }} l 
            ON llt.position_id = l.position_id
            AND llt.latest_leverage_time = l.evt_block_time
)

SELECT 
    gc.block_month, 
    gc.protocol_version,
    gc.day, 
    gc.evt_block_time,
    gc.project_contract_address,
    gc.evt_index,
    gc.evt_tx_hash,
    gc.position_id,
    gc.price, 
    (100/gc.perc_closed) * gc.payout as previous_margin, 
    ((100/gc.perc_closed) * gc.payout) - gc.payout as new_margin,
    ll.leverage, 
    gc.payout,
    gc.trader, 
    gc.version, 
    gc.positions_contract
FROM 
get_positions_contract gc 
INNER JOIN 
latest_leverage ll 
    ON gc.positions_contract = ll.positions_contract
    AND gc.evt_tx_hash = ll.evt_tx_hash
    AND gc.position_id = ll.position_id
    AND gc.evt_index = ll.evt_index
