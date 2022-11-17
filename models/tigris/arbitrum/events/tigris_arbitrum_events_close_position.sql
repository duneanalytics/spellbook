{{ config(
    alias = 'close_position',
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'trader', 'price', 'payout', 'perc_closed']
    )
}}

WITH 

close_position_v2 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _closePrice/1e18 as price, 
            _payout/1e18 as payout, 
            _percent/1e18 as perc_closed, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionClosed') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

close_position_v3 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _closePrice/1e18 as price, 
            _payout/1e18 as payout, 
            _percent/1e18 as perc_closed, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionClosed') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

close_position_v4 as (
        SELECT 
            date_trunc('day', evt_block_time) as day, 
            evt_tx_hash,
            evt_index,
            evt_block_time,
            _id as position_id,
            _closePrice/1e18 as price, 
            _payout/1e18 as payout, 
            _percent/1e18 as perc_closed, 
            _trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionClosed') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v2' as version FROM close_position_v2

UNION 

SELECT *, 'v3' as version FROM close_position_v3

UNION 

SELECT *, 'v4' as version FROM close_position_v4
