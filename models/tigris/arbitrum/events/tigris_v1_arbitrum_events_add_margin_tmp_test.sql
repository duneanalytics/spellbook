{{ config(
    tags=['dunesql'],
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_add_margin_tmp_test'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin']
    )
}}

WITH 

{% set add_margin_evt_tables = [
    'TradingV2_evt_AddToPosition'
    ,'TradingV3_evt_AddToPosition'
    ,'TradingV4_evt_AddToPosition'
    ,'TradingV5_evt_AddToPosition'
] %}

{% set add_margin_call_tables = [
    'TradingV2_call_addToPosition'
    ,'TradingV3_call_addToPosition'
    ,'TradingV4_call_addToPosition'
    ,'TradingV5_call_addToPosition'
] %}

add_margin_events AS (
    {% for add_margin_trading_evt in add_margin_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            TRY_CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
            ap.evt_tx_hash,
            ap.evt_index,
            ap.evt_block_time,
            ap._id as position_id,
            ap._newMargin/1e18 as margin, 
            ap._newPrice/1e18 as price, 
            ap._trader as trader,
            ap.contract_address as project_contract_address
        FROM {{ source('tigristrade_arbitrum', add_margin_trading_evt) }} ap
        {% if is_incremental() %}
        WHERE ap.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

add_margin_calls AS (
    {% for add_margin_trading_call in add_margin_call_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            ap.call_success, 
            ap.call_tx_hash,
            ap._id as position_id,
            ap._addMargin/1e18 as margin_change
        FROM {{ source('tigristrade_arbitrum', add_margin_trading_call) }} ap
        {% if is_incremental() %}
        WHERE ap.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT 
    ae.version, 
    ae.day, 
    ae.evt_tx_hash,
    ae.evt_index,
    ae.evt_block_time,
    ae.position_id,
    ac.margin_change,
    ae.margin, 
    ae.price, 
    ae.trader,
    ae.project_contract_address
FROM 
add_margin_events ae 
INNER JOIN 
add_margin_calls ac 
    ON ae.version = ac.version 
    AND ae.evt_tx_hash = ac.call_tx_hash
    AND ae.position_id = ac.position_id
    AND ac.call_success = true 


