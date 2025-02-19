{{ config(
    schema = 'fxdx_base',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2023-05-18' %}

WITH all_executed_positions AS (
    SELECT 
        *,
        'Open' AS trade_type
    FROM delta_prod.fxdx_base.vault_evt_increaseposition
    WHERE evt_block_time >= DATE '2023-05-18'

    UNION ALL

    SELECT 
        *, 
        'Close' AS trade_type
    FROM delta_prod.fxdx_base.vault_evt_decreaseposition
    WHERE evt_block_time >= DATE '2023-05-18'
),

margin_fees_info AS (
    SELECT 
        evt_tx_hash,
        evt_index,
        feeUsd,
        -- Add row number to ensure one fee per trade
        ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) as rn,
        LEAD(evt_index, 1, 1000000) OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS next_evt_index
    FROM delta_prod.fxdx_base.vault_evt_collectpositiontradefees
    WHERE evt_block_time >= DATE '2023-05-18'
),

complete_perp_tx AS (
    SELECT 
        *, 
        index_token || '/USD' AS market
    FROM 
    (
        SELECT 
            event.account,
            event.collateralDelta,
            event.collateralToken,
            event.contract_address,
            event.fee,
            event.indexToken,
            event.key,
            event.sizeDelta,
            event.evt_block_time,
            event.evt_index,
            event.evt_tx_hash,
            event.isLong,
            event.evt_tx_from,
            event.evt_tx_to,
            event.trade_type,
            (
                CASE
                    WHEN collateralToken = 0xd6c5469a7cc587e1e89a841fb7c102ff1370c05f THEN 'WETH'
                    WHEN collateralToken = 0x50c5725949a6f0c72e6c4a641f24049a917db0cb THEN 'DAI'
                    WHEN collateralToken = 0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca THEN 'USDCbC'
                    WHEN collateralToken = 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 THEN 'USDC'
                    WHEN collateralToken = 0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22 THEN 'cbETH'
                    ELSE tokens1.symbol
                END
            ) AS underlying_asset,
            (
                CASE
                    WHEN indexToken = 0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22 THEN 'cbETH'
                    WHEN indexToken = 0xd6c5469a7cc587e1e89a841fb7c102ff1370c05f THEN 'WETH'
                    ELSE tokens.symbol
                END
            ) AS index_token,
            fee.feeUsd AS margin_fee
        FROM all_executed_positions event
        INNER JOIN margin_fees_info fee
            ON event.evt_tx_hash = fee.evt_tx_hash
            AND event.evt_index > fee.evt_index
            AND event.evt_index < fee.next_evt_index
            AND fee.rn = 1  -- Ensure we only get one fee per trade
        INNER JOIN delta_prod.tokens.erc20 tokens
            ON event.indexToken = tokens.contract_address
            AND tokens.blockchain = 'base'
        INNER JOIN delta_prod.tokens.erc20 tokens1
            ON event.collateralToken = tokens1.contract_address
            AND tokens1.blockchain = 'base'
    )
)

SELECT 
    'base' AS blockchain,
    CAST(date_trunc('DAY', evt_block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', evt_block_time) AS date) AS block_month,
    evt_block_time AS block_time,
    CAST(NULL AS VARCHAR) AS virtual_asset,
    underlying_asset,
    market,
    contract_address AS market_address,
    CAST(sizeDelta/1e30 AS DOUBLE) AS volume_usd,
    CAST(margin_fee/1e30 AS DOUBLE) AS fee_usd,
    CAST(collateralDelta/1e30 AS DOUBLE) AS margin_usd,
    (CASE
        WHEN isLong = false AND trade_type = 'Open' THEN 'Open Short'
        WHEN isLong = true AND trade_type = 'Open' THEN 'Open Long'
        WHEN isLong = false AND trade_type = 'Close' THEN 'Close Short'
        WHEN isLong = true AND trade_type = 'Close' THEN 'Close Long'
    END) AS trade,
    'FXDX' AS project,
    'v2' AS version,
    'FXDX' AS frontend,
    account AS trader,
    sizeDelta AS volume_raw,
    evt_tx_hash AS tx_hash,
    evt_tx_from AS tx_from,
    evt_tx_to AS tx_to,
    evt_index
FROM complete_perp_tx