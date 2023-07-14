{{ config(
    schema = 'tigris_v2_arbitrum',
    alias = alias('events_modify_margin'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'leverage']
    )
}}

WITH 

modify_margin_v1 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm.id as position_id,
            mm.isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm.newMargin/1e18 as margin, 
            mm.newLeverage/1e18 as leverage, 
            mm.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'Trading_call_addMargin') }} am 
            ON mm.id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            AND mm.isMarginAdded = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'Trading_call_removeMargin') }} rm 
            ON mm.id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            AND mm.isMarginAdded = false
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

modify_margin_v2 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm.id as position_id,
            mm.isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm.newMargin/1e18 as margin, 
            mm.newLeverage/1e18 as leverage, 
            mm.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_call_addMargin') }} am 
            ON mm.id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            AND mm.isMarginAdded = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_call_removeMargin') }} rm 
            ON mm.id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            AND mm.isMarginAdded = false
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

modify_margin_v3 as (
        SELECT 
            date_trunc('day', mm.evt_block_time) as day, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm.id as position_id,
            mm.isMarginAdded as modify_type, 
            COALESCE(am._addMargin/1e18, rm._removeMargin/1e18) as margin_change, 
            mm.newMargin/1e18 as margin, 
            mm.newLeverage/1e18 as leverage, 
            mm.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_evt_MarginModified') }} mm 
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_call_addMargin') }} am 
            ON mm.id = am._id 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND am.call_success = true 
            AND mm.isMarginAdded = true 
            {% if is_incremental() %}
            AND am.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        LEFT JOIN 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_call_removeMargin') }} rm 
            ON mm.id = rm._id 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND rm.call_success = true 
            AND mm.isMarginAdded = false
            {% if is_incremental() %}
            AND rm.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2.1' as version FROM modify_margin_v1

UNION ALL 

SELECT *, 'v2.2' as version FROM modify_margin_v2

UNION ALL 

SELECT *, 'v2.3' as version FROM modify_margin_v3
