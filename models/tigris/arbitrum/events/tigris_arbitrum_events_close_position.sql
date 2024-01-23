{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'events_close_position',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'evt_index', 'protocol_version']
    )
}}

WITH 

{% set close_position_v1_evt_tables = [
    'TradingV2_evt_PositionClosed'
    ,'TradingV3_evt_PositionClosed'
    ,'TradingV4_evt_PositionClosed'
    ,'TradingV5_evt_PositionClosed'
] %}

{% set close_position_v2_evt_tables = [
    'Trading_evt_PositionClosed'
    ,'TradingV2_evt_PositionClosed'
    ,'TradingV3_evt_PositionClosed'
    ,'TradingV4_evt_PositionClosed'
    ,'TradingV5_evt_PositionClosed'
    ,'TradingV6_evt_PositionClosed'
] %}

close_position_v1 AS (
    {% for close_position_trading_evt in close_position_v1_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
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
            _trader as trader,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_arbitrum', close_position_trading_evt) }}
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
            trader,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', close_position_trading_evt) }}
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
close_position_v1 a
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract 
FROM 
close_position_v2 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

-- reload