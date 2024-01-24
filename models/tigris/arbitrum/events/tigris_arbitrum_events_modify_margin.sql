{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'events_modify_margin',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'leverage', 'protocol_version']
    )
}}

WITH 

{% set modify_margin_v1_evt_tables = [
    'TradingV2_evt_MarginModified'
    ,'TradingV3_evt_MarginModified'
    ,'TradingV4_evt_MarginModified'
    ,'TradingV5_evt_MarginModified'
] %}

{% set remove_margin_v1_call_tables = [
    'TradingV2_call_removeMargin'
    ,'TradingV3_call_removeMargin'
    ,'TradingV4_call_removeMargin'
    ,'TradingV5_call_removeMargin'
] %}

{% set add_margin_v1_call_tables = [
    'TradingV2_call_addMargin'
    ,'TradingV3_call_addMargin'
    ,'TradingV4_call_addMargin'
    ,'TradingV5_call_addMargin'
] %}

{% set modify_margin_v2_evt_tables = [
    'Trading_evt_MarginModified'
    ,'TradingV2_evt_MarginModified'
    ,'TradingV3_evt_MarginModified'
    ,'TradingV4_evt_MarginModified'
    ,'TradingV5_evt_MarginModified'
    ,'TradingV6_evt_MarginModified'
] %}

{% set remove_margin_v2_call_tables = [
    'Trading_call_removeMargin'
    ,'TradingV2_call_removeMargin'
    ,'TradingV3_call_removeMargin'
    ,'TradingV4_call_removeMargin'
    ,'TradingV5_call_removeMargin'
    ,'TradingV6_call_removeMargin'
] %}

{% set add_margin_v2_call_tables = [
    'Trading_call_addMargin'
    ,'TradingV2_call_addMargin'
    ,'TradingV3_call_addMargin'
    ,'TradingV4_call_addMargin'
    ,'TradingV5_call_addMargin'
    ,'TradingV6_call_addMargin'
] %}

modify_margin_events_v1 AS (
    {% for modify_margin_trading_evt in modify_margin_v1_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm._id as position_id,
            mm._isMarginAdded as modify_type,
            mm._newMargin/1e18 as margin, 
            mm._newLeverage/1e18 as leverage, 
            mm._trader as trader,
            mm.contract_address as project_contract_address
        FROM {{ source('tigristrade_arbitrum', modify_margin_trading_evt) }} mm
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

add_margin_calls_v1 AS (
    {% for add_margin_trading_call in add_margin_v1_call_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            ap.call_success, 
            ap.call_tx_hash,
            ap._id as position_id,
            ap._addMargin/1e18 as margin_change
        FROM {{ source('tigristrade_arbitrum', add_margin_trading_call) }} ap
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

remove_margin_calls_v1 AS (
    {% for remove_margin_trading_call in remove_margin_v1_call_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            ap.call_success, 
            ap.call_tx_hash,
            ap._id as position_id,
            ap._removeMargin/1e18 as margin_change
        FROM {{ source('tigristrade_arbitrum', remove_margin_trading_call) }} ap
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

modify_margin_v1 as (  
        SELECT 
            mm.*, 
            COALESCE(am.margin_change, rm.margin_change) as margin_change
        FROM 
        modify_margin_events_v1 mm 
        LEFT JOIN 
        add_margin_calls_v1 am 
            ON mm.version = am.version 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND mm.position_id = am.position_id
            AND am.call_success = true 
        LEFT JOIN 
        remove_margin_calls_v1 rm 
            ON mm.version = rm.version 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND mm.position_id = rm.position_id
            AND rm.call_success = true 
),

modify_margin_events_v2 AS (
    {% for modify_margin_trading_evt in modify_margin_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            mm.evt_tx_hash,
            mm.evt_index,
            mm.evt_block_time,
            mm.id as position_id,
            mm.isMarginAdded as modify_type,
            mm.newMargin/1e18 as margin, 
            mm.newLeverage/1e18 as leverage, 
            mm.trader as trader,
            mm.contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', modify_margin_trading_evt) }} mm
        {% if is_incremental() %}
        WHERE mm.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

add_margin_calls_v2 AS (
    {% for add_margin_trading_call in add_margin_v2_call_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            ap.call_success, 
            ap.call_tx_hash,
            ap._id as position_id,
            ap._addMargin/1e18 as margin_change
        FROM {{ source('tigristrade_v2_arbitrum', add_margin_trading_call) }} ap
        {% if is_incremental() %}
        WHERE ap.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

remove_margin_calls_v2 AS (
    {% for remove_margin_trading_call in remove_margin_v2_call_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            ap.call_success, 
            ap.call_tx_hash,
            ap._id as position_id,
            ap._removeMargin/1e18 as margin_change
        FROM {{ source('tigristrade_v2_arbitrum', remove_margin_trading_call) }} ap
        {% if is_incremental() %}
        WHERE ap.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

modify_margin_v2 as (  
        SELECT 
            mm.*, 
            COALESCE(am.margin_change, rm.margin_change) as margin_change
        FROM 
        modify_margin_events_v2 mm 
        LEFT JOIN 
        add_margin_calls_v2 am 
            ON mm.version = am.version 
            AND mm.evt_tx_hash = am.call_tx_hash
            AND mm.position_id = am.position_id
            AND am.call_success = true 
        LEFT JOIN 
        remove_margin_calls_v2 rm 
            ON mm.version = rm.version 
            AND mm.evt_tx_hash = rm.call_tx_hash
            AND mm.position_id = rm.position_id
            AND rm.call_success = true 
)

SELECT 
    a.*,
    c.positions_contract 
FROM 
modify_margin_v1 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT
    a.*,
    c.positions_contract
FROM 
modify_margin_v2 a
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

    -- reload