{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'events_add_margin',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'margin', 'protocol_version']
    )
}}

WITH 

{% set add_margin_v1_evt_tables = [
    'TradingV2_evt_AddToPosition'
    ,'TradingV3_evt_AddToPosition'
    ,'TradingV4_evt_AddToPosition'
    ,'TradingV5_evt_AddToPosition'
] %}

{% set add_margin_v2_evt_tables = [
    'Trading_evt_AddToPosition'
    ,'TradingV2_evt_AddToPosition'
    ,'TradingV3_evt_AddToPosition'
    , 'TradingV4_evt_AddToPosition'
    , 'TradingV5_evt_AddToPosition'
    , 'TradingV6_evt_AddToPosition'
] %}

{% set add_margin_v1_call_tables = [
    'TradingV2_call_addToPosition'
    ,'TradingV3_call_addToPosition'
    ,'TradingV4_call_addToPosition'
    ,'TradingV5_call_addToPosition'
] %}

add_margin_events AS (
    {% for add_margin_trading_evt in add_margin_v1_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            CAST(date_trunc('DAY', ap.evt_block_time) AS date) as day, 
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
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

add_margin_calls AS (
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

add_margin_v1 as (
    SELECT 
        '1' as protocol_version,
        ae.version, 
        ae.day, 
        CAST(date_trunc('MONTH', ae.day) AS date) as block_month,
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
), 

add_margin_v2 AS (
    {% for add_margin_trading_evt in add_margin_v2_evt_tables %}
        SELECT
            '2' as protocol_version,
            '{{ 'v2.' + loop.index | string }}' as version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month,
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            addMargin/1e18 as margin_change, 
            newMargin/1e18 as margin, 
            newPrice/1e18 as price, 
            trader,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', add_margin_trading_evt) }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT 
    a.*, 
    c.positions_contract
FROM 
add_margin_v1 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT
    a.*,
    c.positions_contract
FROM 
add_margin_v2 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version
    
-- reload





