{{ config(
    
    schema = 'tigris_polygon',
    alias = 'events_open_position',
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

{% set open_position_v1_1_evt_tables = [
    'Tradingv1_evt_PositionOpened'
    ,'TradingV2_evt_PositionOpened'
    ,'TradingV3_evt_PositionOpened'
    ,'TradingV4_evt_PositionOpened'
] %}

{% set open_position_v1_2_evt_tables = [
    'TradingV5_evt_PositionOpened'
    ,'TradingV6_evt_PositionOpened'
    ,'TradingV7_evt_PositionOpened'
    ,'TradingV8_evt_PositionOpened'
] %}

{% set open_position_v2_evt_tables = [
    'Trading_evt_PositionOpened',
    'TradingV2_evt_PositionOpened',
    'TradingV3_evt_PositionOpened',
    'TradingV4_evt_PositionOpened',
    'TradingV5_evt_PositionOpened',
    'TradingV6_evt_PositionOpened'
] %} 

open_position_v1_1 AS (
    {% for open_position_trading_evt in open_position_v1_1_evt_tables %}
        SELECT
            '{{ 'v1.' + loop.index | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            CAST(json_extract_scalar(_tradeInfo, '$.margin') as double)/1e18 as margin, 
            CAST(json_extract_scalar(_tradeInfo, '$.leverage') as double)/1e18 as leverage,
            CAST(json_extract_scalar(_tradeInfo, '$.margin') as double)/1e18 * CAST(json_extract_scalar(_tradeInfo, '$.leverage') as double)/1e18 as volume_usd, 
            from_hex(json_extract_scalar(_tradeInfo, '$.marginAsset')) as margin_asset, 
            ta.pair, 
            CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) as direction, 
            from_hex(json_extract_scalar(_tradeInfo, '$.referral')) as referral, 
            t._trader as trader,
            t.contract_address as project_contract_address,
            'open_position' as open_type 
        FROM {{ source('tigristrade_polygon', open_position_trading_evt) }} t
        INNER JOIN pairs ta
            ON CAST(json_extract_scalar(_tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
            AND ta.protocol_version = '1'
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

open_position_v1_2 AS (
    {% for open_position_trading_evt in open_position_v1_2_evt_tables %}
        SELECT
            '{{ 'v1.' + (loop.index + 4) | string }}' as version,
            '1' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
            t.evt_block_time, 
            t.evt_index, 
            t.evt_tx_hash, 
            t._id as position_id, 
            t._price/1e18 as price, 
            CAST(json_extract_scalar(_tradeInfo, '$.margin') as double)/1e18 as margin, 
            CAST(json_extract_scalar(_tradeInfo, '$.leverage') as double)/1e18 as leverage,
            CAST(json_extract_scalar(_tradeInfo, '$.margin') as double)/1e18 * CAST(json_extract_scalar(_tradeInfo, '$.leverage') as double)/1e18 as volume_usd, 
            from_hex(json_extract_scalar(_tradeInfo, '$.marginAsset')) as margin_asset, 
            ta.pair, 
            CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) as direction, 
            from_hex(json_extract_scalar(_tradeInfo, '$.referral')) as referral, 
            t._trader as trader,
            t.contract_address as project_contract_address,
            CASE 
                WHEN CAST(_orderType as VARCHAR) = '1' AND CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) = 'true' THEN 'limit_buy'
                WHEN CAST(_orderType as VARCHAR) = '1' AND CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) = 'false' THEN 'limit_sell'
                WHEN CAST(_orderType as VARCHAR) = '2' AND CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) = 'true' THEN 'buy_stop'
                WHEN CAST(_orderType as VARCHAR) = '2' AND CAST(json_extract_scalar(_tradeInfo, '$.direction') as VARCHAR) = 'false' THEN 'sell_stop'
                ELSE 'open_position'
            END as open_type 
        FROM {{ source('tigristrade_polygon', open_position_trading_evt) }} t
        INNER JOIN pairs ta
            ON CAST(json_extract_scalar(_tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
            AND ta.protocol_version = '1'
        {% if is_incremental() %}
        WHERE 1 = 0 
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

open_position_v2 AS (
    {% for open_position_trading_evt in open_position_v2_evt_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month,
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
            t.trader as trader,
            t.contract_address as project_contract_address,
            CASE 
                WHEN CAST(orderType as VARCHAR) = '1' AND CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) = 'true' THEN 'limit_buy'
                WHEN CAST(orderType as VARCHAR) = '1' AND CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) = 'false' THEN 'limit_sell'
                WHEN CAST(orderType as VARCHAR) = '2' AND CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) = 'true' THEN 'buy_stop'
                WHEN CAST(orderType as VARCHAR) = '2' AND CAST(json_extract_scalar(tradeInfo, '$.direction') as VARCHAR) = 'false' THEN 'sell_stop'
                ELSE 'open_position'
            END as open_type 
        FROM {{ source('tigristrade_v2_polygon', open_position_trading_evt) }} t
        INNER JOIN pairs ta
            ON CAST(json_extract_scalar(tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
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
open_position_v1_1 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract 
FROM 
open_position_v1_2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version

UNION ALL 

SELECT 
    a.*,
    c.positions_contract 
FROM 
open_position_v2 a 
INNER JOIN 
{{ ref('tigris_polygon_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version
