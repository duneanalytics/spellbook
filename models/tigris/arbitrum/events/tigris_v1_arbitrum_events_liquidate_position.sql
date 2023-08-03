{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_liquidate_position'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader']
    )
}}

WITH 

liquidate_position_v2 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

liquidate_position_v3 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

liquidate_position_v4 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

liquidate_position_v5 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

SELECT *, 'v1.2' as version FROM liquidate_position_v2

UNION ALL

SELECT *, 'v1.3' as version FROM liquidate_position_v3

UNION ALL

SELECT *, 'v1.4' as version FROM liquidate_position_v4

UNION ALL

SELECT *, 'v1.5' as version FROM liquidate_position_v5