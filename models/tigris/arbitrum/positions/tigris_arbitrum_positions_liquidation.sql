{{ config(
    tags=['dunesql'],
    schema = 'tigris_arbitrum',
    alias = alias('positions_liquidation')
    )
 }}

WITH 

last_margin as (
        SELECT 
            xx.evt_block_time,
            xx.position_id,
            xx.positions_contract,
            xy.margin 
        FROM 
        (
        SELECT 
            MAX(evt_block_time) as evt_block_time,
            position_id,
            positions_contract
        FROM 
        {{ ref('tigris_arbitrum_positions_margin') }}
        GROUP BY 2, 3 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_arbitrum_positions_margin') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
            AND xx.positions_contract = xy.positions_contract
),

last_leverage as (
        SELECT 
            xx.evt_block_time,
            xx.position_id,
            xx.positions_contract,
            xy.leverage 
        FROM 
        (
        SELECT 
            MAX(evt_block_time) as evt_block_time,
            position_id,
            positions_contract
        FROM 
        {{ ref('tigris_arbitrum_positions_leverage') }}
        GROUP BY 2, 3 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_arbitrum_positions_leverage') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
            AND xx.positions_contract = xy.positions_contract
)

SELECT 
    lp.day, 
    lp.block_month,
    lp.protocol_version,
    lp.evt_block_time,
    lp.position_id,
    lp.evt_tx_hash,
    lp.evt_index,
    lp.trader,
    lp.project_contract_address,
    lp.version,
    lp.price,
    lm.margin, 
    ll.leverage, 
    lp.positions_contract
FROM 
{{ ref('tigris_arbitrum_events_liquidate_position') }} lp 
INNER JOIN 
last_margin lm 
    ON lp.position_id = lm.position_id
    AND lp.positions_contract = lm.positions_contract
INNER JOIN 
last_leverage ll 
    ON lp.position_id = ll.position_id
    AND lp.positions_contract = ll.positions_contract
