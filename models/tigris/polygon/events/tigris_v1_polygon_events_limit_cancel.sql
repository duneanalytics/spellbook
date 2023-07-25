{{ config(
	tags=['dunesql'],
    schema = 'tigris_v1_polygon',
    alias = alias('events_limit_cancel'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id']
    )
}}

WITH 

{% set limit_cancel_trading_evt_tables = [
    'TradingV2_evt_LimitCancelled'
    ,'TradingV3_evt_LimitCancelled'
    ,'TradingV4_evt_LimitCancelled'
    ,'TradingV5_evt_LimitCancelled'
    ,'TradingV6_evt_LimitCancelled'
    ,'TradingV7_evt_LimitCancelled'
    ,'TradingV8_evt_LimitCancelled'
] %}

limit_orders AS (
    {% for limit_cancel_trading_evt in limit_cancel_trading_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t._id as position_id,
            t._trader as trader
        FROM {{ source('tigristrade_polygon', limit_cancel_trading_evt) }} t
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day) 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * FROM limit_orders