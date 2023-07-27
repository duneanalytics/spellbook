{{ config(
	tags=['legacy'],
	
    schema = 'tigris_v2_arbitrum',
    alias = alias('events_add_margin', legacy_model=True),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin']
    )
}}

WITH 

add_margin_v1 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            addMargin/1e18 as margin_change, 
            newMargin/1e18 as margin, 
            newPrice/1e18 as price, 
            trader
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_AddToPosition') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

add_margin_v2 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            addMargin/1e18 as margin_change, 
            newMargin/1e18 as margin, 
            newPrice/1e18 as price, 
            trader
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_AddToPosition') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

add_margin_v3 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            addMargin/1e18 as margin_change, 
            newMargin/1e18 as margin, 
            newPrice/1e18 as price, 
            trader
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_evt_AddToPosition') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2.1' as version FROM add_margin_v1

UNION ALL 

SELECT *, 'v2.2' as version FROM add_margin_v2

UNION ALL 

SELECT *, 'v2.3' as version FROM add_margin_v3