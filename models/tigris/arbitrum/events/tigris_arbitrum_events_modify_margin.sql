{{ config(
    alias = 'modify_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', evt_tx_hash', 'position_id', 'trader', 'margin', 'leverage']
    )
}}

WITH 

modify_margin_v2 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _isMarginAdded as type, 
            _newMargin/1e18 as margin, 
            _newLeverage/1e18 as price, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_MarginModified') }}
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
            _id as position_id,
            _newMargin/1e18 as margin, 
            _newPrice/1e18 as price, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_AddToPosition') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

add_margin_v4 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _newMargin/1e18 as margin, 
            _newPrice/1e18 as price, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_AddToPosition') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2' as version FROM add_margin_v2

UNION 

SELECT *, 'v3' as version FROM add_margin_v3

UNION 

SELECT *, 'v4' as version FROM add_margin_v4