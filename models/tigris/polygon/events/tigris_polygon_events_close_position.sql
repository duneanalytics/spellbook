{{ config(
    
    schema = 'tigris_polygon',
    alias = 'events_close_position',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'evt_index', 'protocol_version']
    )
}}

WITH 

{% set close_position_v1_1_evt_tables = [
    'Tradingv1_evt_PositionClosed'
    ,'TradingV2_evt_PositionClosed'
    ,'TradingV3_evt_PositionClosed'
    ,'TradingV4_evt_PositionClosed'
] %}

{% set close_position_v1_2_evt_tables = [
    'TradingV5_evt_PositionClosed'
    ,'TradingV6_evt_PositionClosed'
    ,'TradingV7_evt_PositionClosed'
    ,'TradingV8_evt_PositionClosed'
] %}

{% set close_position_v2_evt_tables = [
    'Trading_evt_PositionClosed'
    ,'TradingV2_evt_PositionClosed'
    ,'TradingV3_evt_PositionClosed'
    ,'TradingV4_evt_PositionClosed'
    ,'TradingV5_evt_PositionClosed'
    ,'TradingV6_evt_PositionClosed'
] %}

close_position_v1_1 AS (
    {% for close_position_trading_evt in close_position_v1_1_evt_tables %}
        SELECT
            '{{ 'v1.' + loop.index | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _closePrice/1e18 as price, 
            _payout/1e18 as payout, 
            _percent/1e2 as perc_closed, 
            contract_address as project_contract_address
        FROM {{ source('tigristrade_polygon', close_position_trading_evt) }}
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

close_position_v1_2 AS (
    {% for close_position_trading_evt in close_position_v1_2_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 4) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _closePrice/1e18 as price, 
            _payout/1e18 as payout, 
            _percent/1e8 as perc_closed, 
            contract_address as project_contract_address,
            _trader as trader
        FROM {{ source('tigristrade_polygon', close_position_trading_evt) }}
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
), 

close_position_v2 AS (
    {% for close_position_trading_evt in close_position_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            id as position_id,
            closePrice/1e18 as price, 
            payout/1e18 as payout, 
            percent/1e8 as perc_closed, 
            contract_address as project_contract_address,
            trader
        FROM {{ source('tigristrade_v2_polygon', close_position_trading_evt) }}
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
    o.trader,
    c.positions_contract
FROM 
close_position_v1_1 a
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
close_position_v1_2 a
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version
WHERE evt_tx_hash NOT IN (0x561cde89720f8af596bf8958dd96339d8b3923094d6d27dd8bf14f5326c9ae25, 0x17e49a19c4feaf014bf485ee2277bfa09375bde9931da9a95222de7a1e704d70, 0x146e22e33c8218ac8c70502b292bbc6d9334983135a1e70ffe0125784bfdcc91)

UNION ALL 

SELECT 
    a.*,
    c.positions_contract 
FROM 
close_position_v2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version