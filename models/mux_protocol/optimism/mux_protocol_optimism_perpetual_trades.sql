{{ config(
    
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', evt_index],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "mux_protocol",
                                \'["sofiat"]\') }}'
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
    CASE WHEN direction = 'true' THEN 'long' ELSE 'short' END AS direction,
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
     {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
),

closed_position as (
SELECT
    evt_block_time,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    trader,
    CASE WHEN direction = 'true' THEN 'long' ELSE 'short' END AS direction,
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
     {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
),

liquidate_position as (
SELECT
    evt_block_time,
    contract_address,
    evt_tx_hash as tx_hash,
    evt_index,
    trader,
    CASE WHEN direction = 'true' THEN 'long' ELSE 'short' END AS direction,
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
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
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
    concat(trade_type, '-' , direction) as trade,
    a.trader, 
    a.volume_raw,
    a.tx_hash,
    b."from" as tx_from,
    b."to" as tx_to,
    evt_index
FROM
    full_tables a
LEFT JOIN {{ source('optimism', 'transactions') }} b
ON a.tx_hash = b.hash
{% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}

    


























{% set project_start_date = '2021-08-31' %}

WITH 

perp_events as (
    -- decrease position
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'decrease_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        sizeDelta/1E30 as volume_usd, 
        fee/1E30 as fee_usd, 
        collateralDelta/1E30 as margin_usd,
        CAST(sizeDelta as UINT256) as volume_raw,
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type,
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_arbitrum', 'Vault_evt_DecreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}

    UNION ALL 

    -- increase position 
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'increase_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        sizeDelta/1E30 as volume_usd, 
        fee/1E30 as fee_usd, 
        collateralDelta/1E30 as margin_usd,
        CAST(sizeDelta as UINT256) as volume_raw,
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_arbitrum', 'Vault_evt_IncreasePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}

    UNION ALL 

    -- liquidate position 
    SELECT
        evt_block_time as block_time, 
        evt_block_number as block_number, 
        'liquidate_position' as trade_data, 
        indexToken as virtual_asset,
        collateralToken as underlying_asset,
        size/1E30 as volume_usd, 
        0 as fee_usd, 
        collateral/1E30 as margin_usd,
        CAST(size as UINT256) as volume_raw, 
        CASE WHEN isLong = false THEN 'short' ELSE 'long' END as trade_type, 
        account as trader, 
        contract_address as market_address, 
        evt_index,
        evt_tx_hash as tx_hash 
    FROM 
    {{ source('gmx_arbitrum', 'Vault_evt_LiquidatePosition') }}
    {% if not is_incremental() %}
    WHERE evt_block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
)

SELECT 
    'arbitrum' as blockchain, 
    'gmx' as project, 
    '1' as version,
    'gmx' as frontend,
    CAST(date_trunc('day', pe.block_time) as date) as block_date, 
    CAST(date_trunc('month', pe.block_time) as date) as block_month,
    pe.block_time, 
    COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR)) as virtual_asset, 
    COALESCE(erc20b.symbol, CAST(pe.underlying_asset as VARCHAR)) as underlying_asset, 
    CASE 
        WHEN pe.virtual_asset = pe.underlying_asset THEN COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR))
        ELSE COALESCE(erc20a.symbol, CAST(pe.virtual_asset as VARCHAR)) || '-' || COALESCE(erc20b.symbol, CAST(pe.underlying_asset as VARCHAR))
    END as market, 
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_data = 'increase_position' THEN 'open' || '-' || pe.trade_type
        WHEN pe.trade_data = 'decrease_position' THEN 'close' || '-' || pe.trade_type
        WHEN pe.trade_data = 'liquidate_position' THEN 'liquidate' || '-' || pe.trade_type
    END as trade, 
    pe.trader, 
    pe.volume_raw,
    pe.tx_hash,
    txns."to" as tx_to,
    txns."from" as tx_from,
    pe.evt_index
FROM 
perp_events pe 
INNER JOIN {{ source('arbitrum', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash
    AND pe.block_number = txns.block_number
    {% if not is_incremental() %}
    AND txns.block_time >= DATE '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND txns.block_time >= date_trunc('day', now() - interval '7' DAY)
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = pe.virtual_asset
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = pe.underlying_asset
    AND erc20b.blockchain = 'arbitrum'