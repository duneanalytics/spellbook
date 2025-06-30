{{ config(
    alias = 'perpetual_trades',
    schema = 'print3r_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
) }}

WITH perp_events AS (
    -- Increase Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'increase_position' AS trade_data,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        TRY_CAST(fee AS DOUBLE) AS fee_usd,
        TRY_CAST(sizeDelta AS DOUBLE) AS volume_usd,
        TRY_CAST(collateralDelta AS DOUBLE) AS margin_usd
    FROM {{ source('print_3r_base', 'vault_evt_increaseposition') }}
    WHERE evt_block_time >= DATE '2023-01-01'
      {% if is_incremental() %}
        AND evt_block_time > (SELECT MAX(block_time) FROM {{ this }})
      {% endif %}

    UNION ALL

    -- Decrease Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'decrease_position' AS trade_data,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        TRY_CAST(fee AS DOUBLE) AS fee_usd,
        TRY_CAST(sizeDelta AS DOUBLE) AS volume_usd,
        TRY_CAST(collateralDelta AS DOUBLE) AS margin_usd
    FROM {{ source('print_3r_base', 'vault_evt_decreaseposition') }}
    WHERE evt_block_time >= DATE '2023-01-01'
      {% if is_incremental() %}
        AND evt_block_time > (SELECT MAX(block_time) FROM {{ this }})
      {% endif %}

    UNION ALL

    -- Liquidate Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'liquidate_position' AS trade_data,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        TRY_CAST(NULL AS DOUBLE) AS fee_usd,
        TRY_CAST(NULL AS DOUBLE) AS volume_usd,
        TRY_CAST(NULL AS DOUBLE) AS margin_usd
    FROM {{ source('print_3r_base', 'vault_evt_liquidateposition') }}
    WHERE evt_block_time >= DATE '2023-01-01'
      {% if is_incremental() %}
        AND evt_block_time > (SELECT MAX(block_time) FROM {{ this }})
      {% endif %}
)

SELECT 
    'base' AS blockchain,
    'print_3r' AS project,
    '1' AS version,
    'print_3r' AS frontend,
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
        WHEN pe.trade_data = 'increase_position' THEN 'open'
        WHEN pe.trade_data = 'decrease_position' THEN 'close'
        WHEN pe.trade_data = 'liquidate_position' THEN 'liquidation'
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
    {% if is_incremental() %}
      AND txns.block_time > (SELECT MAX(block_time) FROM {{ this }})
    {% endif %}
