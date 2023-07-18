{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v2_arbitrum',
    alias = alias('events_liquidate_position', legacy_model=True),
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
            id as position_id,
            trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_PositionLiquidated') }}
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
            id as position_id,
            trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_PositionLiquidated') }}
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
            id as position_id,
            trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_evt_PositionLiquidated') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2.1' as version FROM liquidate_position_v1

UNION ALL

SELECT *, 'v2.2' as version FROM liquidate_position_v2

UNION ALL

SELECT *, 'v2.3' as version FROM liquidate_position_v3