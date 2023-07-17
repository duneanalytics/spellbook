{{ config(
    schema = 'tigris_v1_arbitrum',
    alias = alias('events_open_position'),
    partition_by = ['day'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id']
    )
}}

WITH 

pairs as (
        SELECT 
            * 
        FROM 
        {{ ref('tigris_v1_arbitrum_events_asset_added') }}
), 

open_positions_v2 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            t._tradeInfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV2_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON t._tradeInfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

open_positions_v3 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            t._tradeInfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV3_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON t._tradeInfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

open_positions_v4 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            t._tradeInfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV4_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON t._tradeInfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

open_positions_v5 as (
        SELECT 
            date_trunc('day', t.evt_block_time) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            t._tradeInfo:margin/1e18 as margin, 
            t._tradeInfo:leverage/1e18 as leverage,
            t._tradeInfo:margin/1e18 * _tradeInfo:leverage/1e18 as volume_usd, 
            t._tradeInfo:marginAsset as margin_asset, 
            ta.pair, 
            t._tradeInfo:direction as direction, 
            t._tradeInfo:referral as referral, 
            t._trader as trader 
        FROM 
        {{ source('tigristrade_arbitrum', 'TradingV5_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON t._tradeInfo:asset = ta.asset_id 
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT *, 'v1.2' as version FROM open_positions_v2

UNION ALL

SELECT *, 'v1.3' as version FROM open_positions_v3

UNION ALL

SELECT *, 'v1.4' as version FROM open_positions_v4

UNION ALL

SELECT *, 'v1.5' as version FROM open_positions_v5
;