{{ config(
    
    schema = 'tigris_polygon',
    alias = 'events_limit_order',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'protocol_version']
    )
}}

WITH 

pairs as (
        SELECT 
            * 
        FROM 
        {{ ref('tigris_polygon_events_asset_added') }}
),

{% set limit_order_trading_v1_evt_tables = [
    'Tradingv1_evt_LimitOrderExecuted'
    ,'TradingV2_evt_LimitOrderExecuted'
    ,'TradingV3_evt_LimitOrderExecuted'
    ,'TradingV4_evt_LimitOrderExecuted'
    ,'TradingV5_evt_LimitOrderExecuted'
    ,'TradingV6_evt_LimitOrderExecuted'
    ,'TradingV7_evt_LimitOrderExecuted'
    ,'TradingV8_evt_LimitOrderExecuted'
] %}

{% set limit_order_trading_v2_evt_tables = [
    'Trading_evt_LimitOrderExecuted',
    'TradingV2_evt_LimitOrderExecuted',
    'TradingV3_evt_LimitOrderExecuted',
    'TradingV4_evt_LimitOrderExecuted',
    'TradingV5_evt_LimitOrderExecuted',
    'TradingV6_evt_LimitOrderExecuted'
] %}

limit_orders_v1 AS (
    {% for limit_order_trading_evt in limit_order_trading_v1_evt_tables %}
        SELECT
            '{{ 'v1.' + loop.index | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month, 
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
            t._trader as trader,
            t.contract_address as project_contract_address
        FROM {{ source('tigristrade_polygon', limit_order_trading_evt) }} t
        INNER JOIN pairs ta
            ON t._asset = ta.asset_id
            AND ta.protocol_version = '1'
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

limit_orders_v2 AS (
    {% for limit_order_trading_evt in limit_order_trading_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t.id as position_id,
            t.openPrice/1e18 as price,
            t.margin/1e18 as margin,
            t.lev/1e18 as leverage,
            t.margin/1e18 * t.lev/1e18 as volume_usd,
            CAST(NULL as VARBINARY) as margin_asset,
            ta.pair,
            CASE WHEN t.direction = true THEN 'true' ELSE 'false' END as direction,
            CAST(NULL as VARBINARY) as referral,
            t.trader as trader,
            t.contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_polygon', limit_order_trading_evt) }} t
        INNER JOIN pairs ta
            ON t.asset = ta.asset_id
            AND ta.protocol_version = '2'
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
   c.positions_contract 
FROM 
limit_orders_v1 a 
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
