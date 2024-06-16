{{ config(
	
    schema = 'tigris_polygon',
    alias = 'events_limit_cancel',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'protocol_version']
    )
}}

WITH 

{% set limit_cancel_v1_1_trading_evt_tables = [
    'Tradingv1_evt_LimitCancelled'
    ,'TradingV2_evt_LimitCancelled'
    ,'TradingV3_evt_LimitCancelled'
    ,'TradingV4_evt_LimitCancelled'
] %}


{% set limit_cancel_v1_2_trading_evt_tables = [
    'TradingV5_evt_LimitCancelled'
    ,'TradingV6_evt_LimitCancelled'
    ,'TradingV7_evt_LimitCancelled'
    ,'TradingV8_evt_LimitCancelled'
] %}

{% set limit_cancel_v2_trading_evt_tables = [
    'Trading_evt_LimitCancelled'
    ,'TradingV2_evt_LimitCancelled'
    ,'TradingV3_evt_LimitCancelled'
    ,'TradingV4_evt_LimitCancelled'
    ,'TradingV5_evt_LimitCancelled'
    ,'TradingV6_evt_LimitCancelled'
] %}

limit_orders_v1_1 AS (
    {% for limit_cancel_trading_evt in limit_cancel_v1_1_trading_evt_tables %}
        SELECT
            '{{ 'v1.' + loop.index | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t._id as position_id,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_polygon', limit_cancel_trading_evt) }} t
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

limit_orders_v1_2 AS (
    {% for limit_cancel_trading_evt in limit_cancel_v1_2_trading_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 4) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t._id as position_id,
            contract_address as project_contract_address,
            t._trader as trader
        FROM {{ source('tigristrade_polygon', limit_cancel_trading_evt) }} t
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

limit_orders_v2 AS (
    {% for limit_cancel_trading_evt in limit_cancel_v2_trading_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t.id as position_id,
            contract_address as project_contract_address,
            t.trader as trader
        FROM {{ source('tigristrade_v2_polygon', limit_cancel_trading_evt) }} t
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT 
    a.*,
    o.trader,
    c.positions_contract
FROM 
limit_orders_v1_1 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version
INNER JOIN 
{{ ref('tigris_polygon_events_open_position') }} o 
    ON c.positions_contract = o.positions_contract
    AND a.position_id = o.position_id

UNION ALL 

SELECT 
    a.*,
    c.positions_contract
FROM 
limit_orders_v1_2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract
FROM 
limit_orders_v2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

