{{ config(
    alias = 'polygon_events_liquidate_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader']
    )
}}

WITH 

liquidate_position_v1 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV1_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v2 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV2_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v3 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV3_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

liquidate_position_v4 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV4_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
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
)

SELECT *, 'v1' as version FROM liquidate_position_v1

UNION 

SELECT *, 'v2' as version FROM liquidate_position_v2

UNION 

SELECT *, 'v3' as version FROM liquidate_position_v3

UNION 

SELECT *, 'v4' as version FROM liquidate_position_v4

UNION 

SELECT *, 'v5' as version FROM liquidate_position_v5

UNION 

SELECT *, 'v6' as version FROM liquidate_position_v6

UNION 

SELECT *, 'v7' as version FROM liquidate_position_v7