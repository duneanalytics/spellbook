{{ config(
    
    schema = 'tigris_polygon',
    alias = 'events_liquidate_position',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.evt_block_time')],
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'protocol_version']
    )
}}

WITH 

{% set liquidate_position_v1_1_evt_tables = [
    'Tradingv1_evt_PositionLiquidated'
    ,'TradingV2_evt_PositionLiquidated'
    ,'TradingV3_evt_PositionLiquidated'
    ,'TradingV4_evt_PositionLiquidated'
] %}

{% set liquidate_position_v1_2_evt_tables = [
    'TradingV5_evt_PositionLiquidated'
    ,'TradingV6_evt_PositionLiquidated'
    ,'TradingV7_evt_PositionLiquidated'
    ,'TradingV8_evt_PositionLiquidated'
] %}

{% set liquidate_position_v2_evt_tables = [
    'Trading_evt_PositionLiquidated'
    ,'TradingV2_evt_PositionLiquidated'
    ,'TradingV3_evt_PositionLiquidated'
    ,'TradingV4_evt_PositionLiquidated'
    ,'TradingV5_evt_PositionLiquidated'
    ,'TradingV6_evt_PositionLiquidated'
] %}

liquidate_position_v1_1 AS (
    {% for liquidate_position_trading_evt in liquidate_position_v1_1_evt_tables %}
        SELECT
            '{{ 'v1.' + loop.index | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            CAST(NULL as double) as price, 
            contract_address as project_contract_address
        FROM {{ source('tigristrade_polygon', liquidate_position_trading_evt) }}
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

liquidate_position_v1_2 AS (
    {% for liquidate_position_trading_evt in liquidate_position_v1_2_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 4) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            CAST(NULL as double) as price, 
            contract_address as project_contract_address,
            _trader as trader
        FROM {{ source('tigristrade_polygon', liquidate_position_trading_evt) }}
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

liquidate_position_v2 AS (
    {% for liquidate_position_trading_evt in liquidate_position_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            liqPrice/1e18 as price, 
            contract_address as project_contract_address,
            trader as trader
        FROM {{ source('tigristrade_v2_polygon', liquidate_position_trading_evt) }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
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
liquidate_position_v1_1 a 
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
liquidate_position_v1_2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract 
FROM 
liquidate_position_v2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

