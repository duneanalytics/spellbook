{{ config(
    alias = 'perpetual_trades',
    schema = 'dip_exchange_v1_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
)}}

{% set project_start_date = '2023-01-01' %}

WITH perp_events AS (
    -- Increase Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'increase' AS trade_data,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(feeValue AS DOUBLE)/1e30 AS fee_usd,
        CAST(sizeChanged AS DOUBLE)/1e30 AS volume_usd,
        CAST(collateralValue AS DOUBLE)/1e30 AS margin_usd,
        CAST(indexPrice AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        side,
        CAST(NULL AS DOUBLE) AS pnl
    FROM {{ source('pool_base', 'Pool_evt_IncreasePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'

    UNION ALL

    -- Decrease Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'decrease' AS trade_data,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(feeValue AS DOUBLE)/1e30 AS fee_usd,
        CAST(sizeChanged AS DOUBLE)/1e30 AS volume_usd,
        CAST(collateralChanged AS DOUBLE)/1e30 AS margin_usd,
        CAST(indexPrice AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        side,
        TRY_CAST(JSON_EXTRACT_SCALAR(pnl, '$.abs') AS DOUBLE)/1e30 AS pnl
    FROM {{ source('pool_base', 'Pool_evt_DecreasePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'

    UNION ALL

    -- Liquidate Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'liquidate' AS trade_data,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(feeValue AS DOUBLE)/1e30 AS fee_usd,
        CAST(size AS DOUBLE)/1e30 AS volume_usd,
        CAST(collateralValue AS DOUBLE)/1e30 AS margin_usd,
        CAST(indexPrice AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        side,
        TRY_CAST(JSON_EXTRACT_SCALAR(pnl, '$.abs') AS DOUBLE)/1e30 AS pnl
    FROM {{ source('pool_base', 'Pool_evt_LiquidatePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
)

SELECT 
    'base' AS blockchain,
    'dip_exchange' AS project,
    '1' AS version,
    'dip_exchange' AS frontend,
    CAST(DATE_TRUNC('day', pe.block_time) AS DATE) AS block_date,
    CAST(DATE_TRUNC('month', pe.block_time) AS DATE) AS block_month,
    pe.block_time,
    pe.indexToken AS virtual_asset,
    pe.collateralToken AS underlying_asset,
    pe.position_id AS market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_data = 'increase' AND pe.side = 0 THEN 'open_long'
        WHEN pe.trade_data = 'increase' AND pe.side = 1 THEN 'open_short'
        WHEN pe.trade_data = 'decrease' AND pe.side = 0 THEN 'close_long'
        WHEN pe.trade_data = 'decrease' AND pe.side = 1 THEN 'close_short'
        WHEN pe.trade_data = 'liquidate' THEN 'liquidation'
    END AS trade,
    pe.trader,
    pe.side AS trade_type,
    pe.price,
    CAST(NULL AS UINT256) AS volume_raw,
    pe.tx_hash,
    pe.tx_to,
    pe.tx_from,
    pe.evt_index,
    pe.pnl
FROM perp_events pe
