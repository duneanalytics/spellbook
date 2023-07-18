{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v1_polygon',
    alias = alias('events_liquidate_position', legacy_model=True),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'evt_index']
    )
}}

WITH 

liquidate_position_v1 as (
        SELECT 
            date_trunc('day', pl.evt_block_time) as day, 
            pl.evt_tx_hash,
            pl.evt_index,
            pl.evt_block_time,
            pl._id as position_id,
            op.trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'Tradingv1_evt_PositionLiquidated') }} pl 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_events_open_position_legacy') }} op 
            ON pl._id = op.position_id
            AND op.version = 'v1'
        {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v2 as (
        SELECT 
            date_trunc('day', pl.evt_block_time) as day, 
            pl.evt_tx_hash,
            pl.evt_index,
            pl.evt_block_time,
            pl._id as position_id,
            op.trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionLiquidated') }} pl 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_events_open_position_legacy') }} op 
            ON pl._id = op.position_id
            AND op.version = 'v2'
        {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v3 as (
        SELECT 
            date_trunc('day', pl.evt_block_time) as day, 
            pl.evt_tx_hash,
            pl.evt_index,
            pl.evt_block_time,
            pl._id as position_id,
            op.trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionLiquidated') }} pl 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_events_open_position_legacy') }} op 
            ON pl._id = op.position_id
            AND op.version = 'v3'
        {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v4 as (
        SELECT 
            date_trunc('day', pl.evt_block_time) as day, 
            pl.evt_tx_hash,
            pl.evt_index,
            pl.evt_block_time,
            pl._id as position_id,
            op.trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionLiquidated') }} pl 
        INNER JOIN 
        {{ ref('tigris_v1_polygon_events_open_position_legacy') }} op 
            ON pl._id = op.position_id
            AND op.version = 'v4'
        {% if is_incremental() %}
        WHERE pl.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v5 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV5_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v6 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV6_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v7 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV7_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v8 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV8_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v1.1' as version FROM liquidate_position_v1

UNION ALL

SELECT *, 'v1.2' as version FROM liquidate_position_v2

UNION ALL

SELECT *, 'v1.3' as version FROM liquidate_position_v3

UNION ALL

SELECT *, 'v1.4' as version FROM liquidate_position_v4

UNION ALL

SELECT *, 'v1.5' as version FROM liquidate_position_v5

UNION ALL

SELECT *, 'v1.6' as version FROM liquidate_position_v6

UNION ALL

SELECT *, 'v1.7' as version FROM liquidate_position_v7

UNION ALL

SELECT *, 'v1.8' as version FROM liquidate_position_v8
;