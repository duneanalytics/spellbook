{{ config(
    
    schema = 'mux_protocol_optimism',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2023-01-05' %}

WITH 

open_position as (
SELECT
    evt_block_time,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    trader,
    'open' as trade_type,
    CAST(json_extract_scalar(args, '$.collateralId') as double) as underlying_id,
    assetId as virtual_id,
    CAST(json_extract_scalar(args, '$.isLong') as varchar) as direction,
    CAST(json_extract_scalar(args, '$.feeUsd') as double)/1e18 as fee_usd,
    CAST(json_extract_scalar(args, '$.amount') as double)/1e18 as volume_usd,
    CAST(json_extract_scalar(args, '$.amount') as double) as volume_raw,
    CAST(NULL as varbinary) as margin_asset
FROM
    {{ source('mux_optimism', 'LiquidityPoolHop1_evt_OpenPosition') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

closed_position as (
SELECT
    evt_block_time,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    trader,
    'close' as trade_type,
    CAST(json_extract_scalar(args, '$.collateralId') as double) as underlying_id,
    CAST(json_extract_scalar(args, '$.profitAssetId') as double) as virtual_id,
    CAST(json_extract_scalar(args, '$.isLong') as varchar) as direction,
    CAST(json_extract_scalar(args, '$.feeUsd') as double)/1e18 as fee_usd,
    CAST(json_extract_scalar(args, '$.amount') as double)/1e18 as volume_usd,
    CAST(json_extract_scalar(args, '$.amount') as double) as volume_raw,
    CAST(NULL as varbinary) as margin_asset
FROM
    {{ source('mux_optimism', 'LiquidityPoolHop1_evt_ClosePosition') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

liquidate_position as (
SELECT
    evt_block_time,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    trader,
    'liquidate' as trade_type,
    CAST(json_extract_scalar(args, '$.collateralId') as double) as underlying_id,
    CAST(json_extract_scalar(args, '$.profitAssetId') as double) as virtual_id,
    CAST(json_extract_scalar(args, '$.isLong') as varchar) as direction,
    CAST(json_extract_scalar(args, '$.feeUsd') as double)/1e18 as fee_usd,
    CAST(json_extract_scalar(args, '$.amount') as double)/1e18 as volume_usd,
    CAST(json_extract_scalar(args, '$.amount') as double) as volume_raw,
    CAST(NULL as varbinary) as margin_asset
FROM
    {{ source('mux_optimism', 'LiquidityPoolHop1_evt_Liquidate') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

combined_table as (
SELECT * FROM open_position
UNION ALL
SELECT * FROM closed_position
UNION ALL
SELECT * FROM liquidate_position
),

full_tables as (
SELECT
    c.*,
    CASE WHEN direction = 'true' THEN 'long' ELSE 'short' END AS direction_tmp,
    from_utf8(a.symbol) as underlying_asset,
    from_utf8(b.symbol) as virtual_asset
FROM
combined_table c
LEFT JOIN {{ ref('mux_protocol_optimism_asset_added') }} a
ON c.underlying_id = a.id
LEFT JOIN  {{ ref('mux_protocol_optimism_asset_added') }} b
ON c.virtual_id = b.id
)

SELECT
    'optimism' as blockchain, 
    'mux_protocol' as project, 
    '1' as version,
    'mux_protocol' as frontend,
    CAST(date_trunc('day', a.evt_block_time) as date) as block_date, 
    CAST(date_trunc('month', a.evt_block_time) as date) as block_month,
    a.evt_block_time as block_time, 
    a.virtual_asset,
    a.underlying_asset,
    concat(virtual_asset, '-', underlying_asset) as market,
    a.contract_address as market_address,
    a.volume_usd,
    a.fee_usd,
    CAST(NULL as double) as margin_usd,
    concat(trade_type, '-' , direction_tmp) as trade,
    a.trader, 
    a.volume_raw,
    a.tx_hash,
    b."from" as tx_from,
    b."to" as tx_to,
    evt_index
FROM
    full_tables a
INNER JOIN {{ source('optimism', 'transactions') }} b
ON a.tx_hash = b.hash
{% if is_incremental() %}
    AND {{incremental_predicate('b.block_time')}}
    {% endif %}