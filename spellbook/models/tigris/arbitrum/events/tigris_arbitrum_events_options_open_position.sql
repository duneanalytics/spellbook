{{ config(
    
    schema = 'tigris_arbitrum',
    alias = 'options_open_position',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['evt_block_time', 'evt_tx_hash', 'position_id', 'positions_contract']
    )
}}

WITH 

pairs as (
        SELECT 
            * 
        FROM 
        {{ ref('tigris_arbitrum_events_asset_added') }}
), 

{% set open_position_tables = [
    'options_evt_TradeOpened',
    'Options_V2_evt_TradeOpened',
    'Options_V3_evt_TradeOpened'
] %}

open_position_v2 AS (
    {% for open_position in open_position_tables %}
        SELECT
            '{{ 'v2.' + loop.index | string }}' as version,
            '2' as protocol_version,
            CAST(date_trunc('DAY', t.evt_block_time) AS date) as day, 
            CAST(date_trunc('MONTH', t.evt_block_time) AS date) as block_month, 
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
            ta.pair, 
            split_part( human_readable_seconds( CAST(json_extract_scalar(tradeInfo, '$.duration') as double) ),',',1) as options_period,
            from_hex(json_extract_scalar(tradeInfo, '$.referrer')) as referral,
            trader,
            CAST(orderType as VARCHAR) as order_type,
            contract_address as project_contract_address
        FROM {{ source('tigristrade_v2_arbitrum', open_position) }} t
        INNER JOIN pairs ta
            ON CAST(json_extract_scalar(tradeInfo, '$.asset') as double) = CAST(ta.asset_id as double)
            AND ta.protocol_version = '2'
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
open_position_v2 a 
INNER JOIN 
{{ ref('tigris_arbitrum_events_contracts_positions') }} c 
    ON a.project_contract_address = c.trading_contract
    AND a.version = c.trading_contract_version