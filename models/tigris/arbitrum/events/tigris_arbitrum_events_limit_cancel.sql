{{ config(
	
    schema = 'tigris_arbitrum',
    alias = 'events_limit_cancel',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'protocol_version']
    )
}}

WITH 

{% set limit_cancel_v1_trading_evt_tables = [
    'TradingV2_evt_LimitCancelled'
    ,'TradingV3_evt_LimitCancelled'
    ,'TradingV4_evt_LimitCancelled'
    ,'TradingV5_evt_LimitCancelled'
] %}

{% set limit_cancel_v2_trading_evt_tables = [
    'Trading_evt_LimitCancelled'
    ,'TradingV2_evt_LimitCancelled'
    ,'TradingV3_evt_LimitCancelled'
    ,'TradingV4_evt_LimitCancelled'
    ,'TradingV5_evt_LimitCancelled'
    ,'TradingV6_evt_LimitCancelled'
] %}

limit_orders_v1 AS (
    {% for limit_cancel_trading_evt in limit_cancel_v1_trading_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 1) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time,
            t.evt_index,
            t.evt_tx_hash,
            t._id as position_id,
            t._trader as trader,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_arbitrum', limit_cancel_trading_evt) }} t
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
            t.trader as trader,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', limit_cancel_trading_evt) }} t
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day) 
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
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract
FROM 
limit_orders_v2 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

-- reload