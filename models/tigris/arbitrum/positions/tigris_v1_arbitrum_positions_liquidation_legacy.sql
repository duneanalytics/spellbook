{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_arbitrum',
    alias = alias('positions_liquidation', legacy_model=True)
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
        {{ ref('tigris_v1_arbitrum_positions_margin_legacy') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v1_arbitrum_positions_margin_legacy') }} xy 
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
        {{ ref('tigris_v1_arbitrum_positions_leverage_legacy') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v1_arbitrum_positions_leverage_legacy') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
)

SELECT 
    lp.evt_block_time,
    lp.position_id,
    lp.evt_tx_hash,
    lp.evt_index,
    lp.trader,
    lp.day,
    lp.project_contract_address,
    lm.margin, 
    ll.leverage 
FROM 
{{ ref('tigris_v1_arbitrum_events_liquidate_position_legacy') }} lp 
INNER JOIN 
last_margin lm 
    ON lp.position_id = lm.position_id
INNER JOIN 
last_leverage ll 
    ON lp.position_id = ll.position_id
;
