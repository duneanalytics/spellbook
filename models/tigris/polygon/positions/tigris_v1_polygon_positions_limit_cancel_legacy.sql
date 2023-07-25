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

missing_traders_v11 as (
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

missing_traders_v13 as (
        SELECT 
            'v1.3' as version, 
            date_trunc('day', evt_block_time) as day,
            evt_block_time, 
            evt_index,
            evt_tx_hash,
            _id as position_id, 
            CASE 
                WHEN CAST(_id as double) IN (244, 241) THEN lower('0x17eec8a23f48ca90726405c77ac2abf559516317')
                WHEN CAST(_id as double) = 171 THEN lower('0xbf0dc9434b89f6271621548c01c247873ec2c207')
                WHEN CAST(_id as double) = 257 THEN lower('0x8c9f0679c3d96cb698a604ce31ed674647c18d9a')
            ELSE ''
            END as trader
        FROM 
        {{ source('tigristrade_polygon', 'TradingV3_evt_LimitCancelled') }}
), 

missing_traders_v14 as (
        SELECT 
            'v1.4' as version, 
            date_trunc('day', evt_block_time) as day,
            evt_block_time, 
            evt_index,
            evt_tx_hash,
            _id as position_id, 
            CASE 
                WHEN CAST(_id as double) IN (431, 426, 427) THEN lower('0x1ff37d66dd1c073bd6c4244c1477672153f2acd7')
                WHEN CAST(_id as double) = 940 THEN lower('0xd2b81badeff0f69ed78462fce17d4d8706d5f4db')
                WHEN CAST(_id as double) IN (404, 405, 407) THEN lower('0x8c9f0679c3d96cb698a604ce31ed674647c18d9a')
            ELSE ''
            END as trader
        FROM 
        {{ source('tigristrade_polygon', 'TradingV4_evt_LimitCancelled') }}
), 

limit_orders as (
        SELECT 
            * 
        FROM 
        missing_traders_v11

        UNION ALL 

        SELECT 
            *
        FROM 
        missing_traders_v13

        UNION ALL 

        SELECT
            *
        FROM 
        missing_traders_v14

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