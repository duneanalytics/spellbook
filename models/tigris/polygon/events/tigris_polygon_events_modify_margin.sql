{{ config(
    alias = 'events_modify_margin',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'evt_index', 'trader', 'margin', 'leverage']
    )
}}

WITH 

modify_margin_v5 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm._id as position_id,
            mm._isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm._newMargin/1e18 as margin, 
            mm._newLeverage/1e18 as leverage, 
            mm._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV5_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV5_call_addMargin') }} am 
            ON mm._id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV5_call_removeMargin') }} rm 
            ON mm._id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

modify_margin_v6 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm._id as position_id,
            mm._isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm._newMargin/1e18 as margin, 
            mm._newLeverage/1e18 as leverage, 
            mm._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV6_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV6_call_addMargin') }} am 
            ON mm._id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV6_call_removeMargin') }} rm 
            ON mm._id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

modify_margin_v7 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm._id as position_id,
            mm._isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm._newMargin/1e18 as margin, 
            mm._newLeverage/1e18 as leverage, 
            mm._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV7_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV7_call_addMargin') }} am 
            ON mm._id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV7_call_removeMargin') }} rm 
            ON mm._id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

modify_margin_v8 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm._id as position_id,
            mm._isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm._newMargin/1e18 as margin, 
            mm._newLeverage/1e18 as leverage, 
            mm._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV8_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV8_call_addMargin') }} am 
            ON mm._id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_polygon', 'TradingV8_call_removeMargin') }} rm 
            ON mm._id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v5' as version FROM modify_margin_v5

UNION ALL

SELECT *, 'v6' as version FROM modify_margin_v6

UNION ALL

SELECT *, 'v7' as version FROM modify_margin_v7

UNION ALL

SELECT *, 'v8' as version FROM modify_margin_v8
;