{{ config(
	tags=['legacy'],
    schema = 'tigris_v1_polygon',
    alias = alias('positions_limit_cancel', legacy_model=True)
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
        {{ ref('tigris_v1_polygon_positions_margin_legacy') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_positions_margin_legacy') }} xy 
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
        {{ ref('tigris_v1_polygon_positions_leverage_legacy') }}
        GROUP BY 2 
        ) xx 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_positions_leverage_legacy') }} xy 
            ON xx.evt_block_time = xy.evt_block_time
            AND xx.position_id = xy.position_id
), 

missing_traders as (
        SELECT 
            'v1.1' as version, 
            date_trunc('day', evt_block_time) as day,
            evt_block_time, 
            evt_index,
            evt_tx_hash,
            _id as position_id, 
            lower('0xe1c15f1f8d2a99123f7a554865cef7b25e06d698') as trader
        FROM 
        {{ source('tigristrade_polygon', 'Tradingv1_evt_LimitCancelled') }}
),

limit_orders as (
        SELECT 
            * 
        FROM 
        missing_traders

        UNION ALL 

        SELECT 
            * 
        FROM 
        {{ ref('tigris_v1_polygon_events_limit_cancel_legacy') }}
)

SELECT 
    lp.*, 
    lm.margin, 
    ll.leverage 
FROM 
limit_orders lp 
INNER JOIN 
last_margin lm 
    ON lp.position_id = lm.position_id
INNER JOIN 
last_leverage ll 
    ON lp.position_id = ll.position_id