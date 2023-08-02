{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_limit_order'),
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
        {{ ref('tigris_v1_arbitrum_events_asset_added') }}
),

{% set limit_order_trading_evt_tables = [
    'TradingV2_evt_LimitOrderExecuted'
    ,'TradingV3_evt_LimitOrderExecuted'
    ,'TradingV4_evt_LimitOrderExecuted'
    ,'TradingV5_evt_LimitOrderExecuted'
] %}

limit_orders AS (
    {% for limit_order_trading_evt in limit_order_trading_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t._id as position_id,
            t._openPrice/1e18 as price,
            t._margin/1e18 as margin,
            t._lev/1e18 as leverage,
            t._margin/1e18 * t._lev/1e18 as volume_usd,
            CAST(NULL as VARBINARY) as margin_asset,
            ta.pair,
            CASE WHEN t._direction = true THEN 'true' ELSE 'false' END as direction,
            CAST(NULL as VARBINARY) as referral,
            t._trader as trader
        FROM {{ source('tigristrade_arbitrum', limit_order_trading_evt) }} t
        INNER JOIN pairs ta
            ON t._asset = ta.asset_id
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day) 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT *
FROM limit_orders
