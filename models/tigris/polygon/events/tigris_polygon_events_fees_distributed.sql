{{ config(
    
    schema = 'tigris_polygon',
    alias = 'events_fees_distributed',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'version', 'protocol_version']
    )
}}

WITH 

{% set fees_distributed_v1_evt_tables = [
    'TradingV7_evt_FeesDistributed'
    ,'TradingV8_evt_FeesDistributed'
] %}

{% set fees_distributed_v2_evt_tables = [
    'Trading_evt_FeesDistributed',
    'TradingV2_evt_FeesDistributed',
    'TradingV3_evt_FeesDistributed',
    'TradingV4_evt_FeesDistributed',
    'TradingV5_evt_FeesDistributed',
    'TradingV6_evt_FeesDistributed'
] %}

fees_v1 AS (
    {% for fees_evt in fees_distributed_v1_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 6) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_block_time,
            evt_index,
            evt_tx_hash,
            _botFees/1e18 + _burnFees/1e18 + _daoFees/1e18 + _refFees/1e18 as fees,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_polygon', fees_evt) }}
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),


fees_v2 AS (
    {% for fees_evt in fees_distributed_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', evt_block_time) AS date) as block_month, 
            evt_block_time,
            evt_index,
            evt_tx_hash,
            botFees/1e18 + burnFees/1e18 + daoFees/1e18 + refFees/1e18 as fees,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_polygon', fees_evt) }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day) 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT * FROM fees_v1

UNION ALL 

SELECT * FROM fees_v2 