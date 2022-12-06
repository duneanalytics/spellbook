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
        {{ ref('tigris_arbitrum_events_asset_added') }}
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
            t._direction as direction, 
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_LimitOrderExecuted') }} t 
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
            t._direction as direction, 
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_LimitOrderExecuted') }} t 
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
            t._direction as direction, 
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_LimitOrderExecuted') }} t 
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
            t._direction as direction, 
            '' as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_LimitOrderExecuted') }} t 
        INNER JOIN 
        pairs ta 
            ON t._asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2' as version FROM limit_order_v2

UNION ALL

SELECT *, 'v3' as version FROM limit_order_v3

UNION ALL

SELECT *, 'v4' as version FROM limit_order_v4

UNION ALL

SELECT *, 'v5' as version FROM limit_order_v5