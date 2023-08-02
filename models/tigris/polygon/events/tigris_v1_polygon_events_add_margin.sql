{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_polygon',
    alias = alias('events_add_margin'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'evt_index']
    )
}}

WITH 

add_margin_v5 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV5_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_polygon', 'TradingV5_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

add_margin_v6 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV6_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_polygon', 'TradingV6_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %} 
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

add_margin_v7 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV7_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_polygon', 'TradingV7_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

add_margin_v8 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV8_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_polygon', 'TradingV8_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

SELECT *, 'v1.5' as version FROM add_margin_v5

UNION ALL

SELECT *, 'v1.6' as version FROM add_margin_v6

UNION ALL

SELECT *, 'v1.7' as version FROM add_margin_v7

UNION ALL

SELECT *, 'v1.8' as version FROM add_margin_v8
