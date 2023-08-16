{{ config(
    tags=['dunesql'],
    schema = 'tigris_polygon',
    alias = alias('open_position'),
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
        {{ ref('tigris_v1_polygon_events_asset_added') }}
), 

{% set open_position_tables = [
    'options_evt_TradeOpened',

] %}

open_position AS (
    {% for open_position in open_position_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            TRY_CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            evt_block_time,
            evt_tx_hash,
            evt_index, 
            id as position_id, 
            price/1e18 as open_price, 
            CAST(NULL as double) as close_price, 
            CAST(NULL as double) as profitnLoss, 
            CAST(json_extract_scalar(tradeInfo, '$.collateral') as double)/1e18 as collateral,
            from_hex(json_extract_scalar(tradeInfo, '$.collateralAsset')) as collateral_asset, 
            CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) as direction, 
            p.pair, 
            split_part( human_readable_seconds( CAST(json_extract_scalar(tradeInfo, '$.duration') as double) ),',',1) as options_period,
            from_hex(json_extract_scalar(tradeInfo, '$.referrer')) as referral,
            trader,
            CAST(orderType as VARCHAR) as order_type 
        FROM {{ source('tigristrade_v2_polygon', open_position) }} t
        INNER JOIN pairs ta
            ON t.asset = ta.asset_id
        {% if is_incremental() %}
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT *
FROM limit_orders