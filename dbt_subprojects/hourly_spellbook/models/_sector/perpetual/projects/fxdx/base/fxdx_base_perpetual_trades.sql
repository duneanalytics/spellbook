{{ config(
    alias = 'perpetual_trades',
    schema = 'fxdx_v1_base',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index']
) }}

{% set project_start_date = '2023-01-01' %}

WITH perp_events AS (

    -- Increase Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'increase' AS trade_action,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(fee AS DOUBLE)/1e30 AS fee_usd,
        CAST(sizeDelta AS DOUBLE)/1e30 AS volume_usd,
        CAST(collateralDelta AS DOUBLE)/1e30 AS margin_usd,
        CAST(price AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        isLong,
        CAST(NULL AS DOUBLE) AS pnl_usd,
        CAST(NULL AS DOUBLE) AS collateral_value,
        CAST(NULL AS DOUBLE) AS reserve_amount
    FROM {{ source('fxdx_base', 'Vault_evt_IncreasePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
      {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
      {% endif %}

    UNION ALL

    -- Decrease Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'decrease' AS trade_action,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(fee AS DOUBLE)/1e30 AS fee_usd,
        CAST(sizeDelta AS DOUBLE)/1e30 AS volume_usd,
        CAST(collateralDelta AS DOUBLE)/1e30 AS margin_usd,
        CAST(price AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        isLong,
        CAST(NULL AS DOUBLE) AS pnl_usd,
        CAST(NULL AS DOUBLE) AS collateral_value,
        CAST(NULL AS DOUBLE) AS reserve_amount
    FROM {{ source('fxdx_base', 'Vault_evt_DecreasePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
      {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
      {% endif %}

    UNION ALL

    -- Liquidate Position events
    SELECT 
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        'liquidate' AS trade_action,
        key AS position_id,
        account AS trader,
        contract_address AS market_address,
        evt_index,
        evt_tx_hash AS tx_hash,
        evt_tx_from AS tx_from,
        evt_tx_to AS tx_to,
        CAST(0 AS DOUBLE) AS fee_usd,
        CAST(size AS DOUBLE)/1e30 AS volume_usd,
        CAST(0 AS DOUBLE) AS margin_usd,
        CAST(markPrice AS DOUBLE)/1e30 AS price,
        collateralToken,
        indexToken,
        isLong,
        CAST(realisedPnl AS DOUBLE)/1e30 AS pnl_usd,
        CAST(collateral AS DOUBLE)/1e30 AS collateral_value,
        CAST(reserveAmount AS DOUBLE)/1e30 AS reserve_amount
    FROM {{ source('fxdx_base', 'Vault_evt_LiquidatePosition') }}
    WHERE evt_block_time >= DATE '{{ project_start_date }}'
      {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
      {% endif %}
)

SELECT 
    'base' AS blockchain,
    'fxdx' AS project,
    '1' AS version,
    'fxdx' AS frontend,
    CAST(DATE_TRUNC('day', pe.block_time) AS DATE) AS block_date,
    CAST(DATE_TRUNC('month', pe.block_time) AS DATE) AS block_month,
    pe.block_time,
    CASE 
        WHEN LOWER(CAST(pe.indexToken AS VARCHAR)) = '0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22' THEN 'cbETH'
        WHEN LOWER(CAST(pe.indexToken AS VARCHAR)) = '0xd6c5469a7cc587e1e89a841fb7c102ff1370c05f' THEN 'WETH'
        WHEN LOWER(CAST(pe.indexToken AS VARCHAR)) = '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca' THEN 'USDbc'
        ELSE CAST(pe.indexToken AS VARCHAR)
    END AS virtual_asset,
    CASE 
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22' THEN 'cbETH'
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0xd6c5469a7cc587e1e89a841fb7c102ff1370c05f' THEN 'WETH'
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca' THEN 'USDbc'
        ELSE CAST(pe.collateralToken AS VARCHAR)
    END AS underlying_asset,
    CASE 
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22' THEN 'cbETH'
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0xd6c5469a7cc587e1e89a841fb7c102ff1370c05f' THEN 'WETH'
        WHEN LOWER(CAST(pe.collateralToken AS VARCHAR)) = '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca' THEN 'USDbc'
        ELSE CAST(pe.collateralToken AS VARCHAR)
    END AS market,
    pe.market_address,
    pe.volume_usd,
    pe.fee_usd,
    pe.margin_usd,
    CASE 
        WHEN pe.trade_action = 'increase' AND pe.isLong = true THEN 'open_long'
        WHEN pe.trade_action = 'increase' AND pe.isLong = false THEN 'open_short'
        WHEN pe.trade_action = 'decrease' AND pe.isLong = true THEN 'close_long'
        WHEN pe.trade_action = 'decrease' AND pe.isLong = false THEN 'close_short'
        WHEN pe.trade_action = 'liquidate' THEN 'liquidation'
    END AS trade,
    pe.trader,
    CASE 
        WHEN pe.isLong = true THEN 'long'
        WHEN pe.isLong = false THEN 'short'
    END AS trade_type,
    pe.price,
    CAST(NULL AS UINT256) AS volume_raw,
    pe.tx_hash,
    pe.tx_to,
    pe.tx_from,
    pe.evt_index,
    pe.pnl_usd AS pnl,
    pe.collateral_value,
    pe.reserve_amount
FROM perp_events pe
