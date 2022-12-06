{{ config(
    alias = 'events_limit_order',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id']
    )
}}

WITH 

pairs as (
        SELECT 
            * 
        FROM 
        {{ ref('tigris_polygon_events_asset_added') }}
), 

limit_order_v1 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'Tradingv1_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

limit_order_v2 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV2_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

limit_order_v3 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction, 
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV3_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

limit_order_v4 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV4_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

limit_order_v5 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV5_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

limit_order_v6 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV6_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

limit_order_v7 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV7_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

limit_order_v8 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._openPrice/1e18 as price, 
            t._margin/1e18 as margin, 
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd, 
            '' as margin_asset, 
            ta.pair, 
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_polygon', 'TradingV8_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v1' as version FROM limit_order_v1

UNION ALL 

SELECT *, 'v2' as version FROM limit_order_v2

UNION ALL

SELECT *, 'v3' as version FROM limit_order_v3

UNION ALL

SELECT *, 'v4' as version FROM limit_order_v4

UNION ALL

SELECT *, 'v5' as version FROM limit_order_v5

UNION ALL

SELECT *, 'v6' as version FROM limit_order_v6

UNION ALL

SELECT *, 'v7' as version FROM limit_order_v7

UNION ALL

SELECT *, 'v8' as version FROM limit_order_v8