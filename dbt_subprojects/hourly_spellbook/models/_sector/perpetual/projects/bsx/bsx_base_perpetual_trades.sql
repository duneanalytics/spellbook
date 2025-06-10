{{ config(
    alias = 'perpetual_trades',
    schema = 'bsx_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
) }}

WITH perp_events AS (
    -- Open Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'open_position' AS trade_data,
        productId AS product_id,
        evt_tx_from AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        CAST(fee AS DOUBLE) AS fee_usd,
       CAST(NULL AS DOUBLE) AS volume_usd,
    CAST(NULL AS DOUBLE) AS margin_usd
    FROM {{ source('bsx_base', 'bsx1000x_evt_openposition') }}
    WHERE evt_block_time >= DATE '2023-01-01'

    UNION ALL

    -- Close Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'close_position' AS trade_data,
        productId AS product_id,
        evt_tx_from AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        CAST(fee AS DOUBLE) AS fee_usd,
        CAST(NULL AS DOUBLE) AS volume_usd,
        CAST(NULL AS DOUBLE) AS margin_usd
    FROM {{ source('bsx_base', 'bsx1000x_evt_closeposition') }}
    WHERE evt_block_time >= DATE '2023-01-01'
)

SELECT 
    'base' AS blockchain,
    'bsx' AS project,
    '1' AS version,
    'bsx' AS frontend,
    CAST(date_trunc('day', pe.block_time) AS DATE) AS block_date,
    CAST(date_trunc('month', pe.block_time) AS DATE) AS block_month,
    pe.block_time,
    CAST(NULL AS VARCHAR) AS virtual_asset,
    CAST(NULL AS VARCHAR) AS underlying_asset,
    CAST(NULL AS VARCHAR) AS market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_data = 'open_position' THEN 'open'
        WHEN pe.trade_data = 'close_position' THEN 'close'
    END AS trade,
    pe.trader,
    CAST(NULL AS UINT256) AS volume_raw,
    pe.tx_hash,
    txns."to" AS tx_to,
    txns."from" AS tx_from,
    pe.evt_index,
    CAST(NULL AS DOUBLE) AS pnl
FROM perp_events pe
INNER JOIN {{ source('base', 'transactions') }} txns 
    ON pe.tx_hash = txns.hash 
    AND pe.block_number = txns.block_number
    AND txns.block_time >= DATE '2023-01-01'