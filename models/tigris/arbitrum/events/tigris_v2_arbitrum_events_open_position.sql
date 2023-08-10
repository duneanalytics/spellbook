{{ config(
    tags=['dunesql'],
    schema = 'tigris_v2_arbitrum',
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
        {{ ref('tigris_v2_arbitrum_events_asset_added') }}
), 

open_positions_v1 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t.id as position_id, 
            t.price/1e18 as price, 
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 as margin, 
            CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as leverage,
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 * CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as volume_usd, 
            from_hex(json_extract_scalar(tradeInfo, '$.marginAsset')) as margin_asset, 
            ta.pair, 
            CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) as direction, 
            from_hex(json_extract_scalar(tradeInfo, '$.referral')) as referral, 
            t.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'Trading_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON CAST(json_extract_scalar(tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

open_positions_v2 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t.id as position_id, 
            t.price/1e18 as price, 
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 as margin, 
            CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as leverage,
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 * CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as volume_usd, 
            from_hex(json_extract_scalar(tradeInfo, '$.marginAsset')) as margin_asset, 
            ta.pair, 
            CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) as direction, 
            from_hex(json_extract_scalar(tradeInfo, '$.referral')) as referral, 
            t.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV2_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON CAST(json_extract_scalar(tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),

open_positions_v3 as (
        SELECT 
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t.id as position_id, 
            t.price/1e18 as price, 
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 as margin, 
            CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as leverage,
            CAST(json_extract_scalar(tradeInfo, '$.margin') as double)/1e18 * CAST(json_extract_scalar(tradeInfo, '$.leverage') as double)/1e18 as volume_usd, 
            from_hex(json_extract_scalar(tradeInfo, '$.marginAsset')) as margin_asset, 
            ta.pair, 
            CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) as direction, 
            from_hex(json_extract_scalar(tradeInfo, '$.referral')) as referral, 
            t.trader as trader 
        FROM 
        {{ source('tigristrade_v2_arbitrum', 'TradingV3_evt_PositionOpened') }} t 
        INNER JOIN 
        pairs ta 
            ON CAST(json_extract_scalar(tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)

SELECT *, 'v2.1' as version FROM open_positions_v1

UNION ALL 

SELECT *, 'v2.2' as version FROM open_positions_v2

UNION ALL 

SELECT *, 'v2.3' as version FROM open_positions_v3