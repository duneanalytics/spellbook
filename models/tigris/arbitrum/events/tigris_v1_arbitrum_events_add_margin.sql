{{ config(
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_add_margin'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin']
    )
}}

WITH 

add_margin_v2 as (
        SELECT 
            date_trunc('day', ap.evt_block_time) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_arbitrum', 'TradingV2_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

add_margin_v3 as (
        SELECT 
            date_trunc('day', ap.evt_block_time) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_arbitrum', 'TradingV3_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

add_margin_v4 as (
        SELECT 
            date_trunc('day', ap.evt_block_time) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_arbitrum', 'TradingV4_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

add_margin_v5 as (
        SELECT 
            date_trunc('day', ap.evt_block_time) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            af._addMargin/1e18 as margin_change, 
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_AddToPosition') }} ap 
        INNER JOIN 
        {{ source('tigristrade_arbitrum', 'TradingV5_call_addToPosition') }} af 
            ON ap._id = af._id 
            AND ap.evt_tx_hash = af.call_tx_hash 
            AND af.call_success = true 
            {% if is_incremental() %}
            AND af.call_block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)


SELECT *, 'v1.2' as version FROM add_margin_v2

UNION ALL

SELECT *, 'v1.3' as version FROM add_margin_v3

UNION ALL

SELECT *, 'v1.4' as version FROM add_margin_v4

UNION ALL

SELECT *, 'v1.5' as version FROM add_margin_v5
;