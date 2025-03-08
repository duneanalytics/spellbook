{{ config(
    schema = 'yfx_v4_base',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["base"]\',
                                spell_type = "project",
                                spell_name = "yfx_v4",
                                contributors = \'["princi"]\') }}'
)
}}

WITH all_events AS (
    SELECT
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        evt_tx_from as "from",
        evt_tx_to as to,
        evt_index,
        contract_address,
        taker as account,
        market,
        id as productId,  -- Using id as productId for asset mapping
        CAST(shortValue as double) / 1e18 as shortValue,
        CAST(longValue as double) / 1e18 as longValue,
        (CAST(feeToExchange as double) + CAST(feeToMaker as double) + CAST(feeToInviter as double)) as fee,
        'open' as trade_type
    FROM {{ source('yfx_v4_base', 'Pool_evt_OpenUpdate') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL

    SELECT
        evt_block_time,
        evt_block_number,
        evt_tx_hash,
        evt_tx_from as "from",
        evt_tx_to as to,
        evt_index,
        contract_address,
        taker as account,
        market,
        id as productId,  -- Using id as productId for asset mapping
        CAST(shortValue as double) / 1e18 as shortValue,
        CAST(longValue as double) / 1e18 as longValue,
        (CAST(feeToExchange as double) + CAST(feeToMaker as double) + CAST(feeToInviter as double)) as fee,
        'close' as trade_type
    FROM {{ source('yfx_v4_base', 'Pool_evt_CloseUpdate') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT 
    'base' AS blockchain,
    CAST(date_trunc('DAY', evt_block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', evt_block_time) AS date) AS block_month,
    evt_block_time AS block_time,
    CASE
        WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
        WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
        WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
        WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
        WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
        WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
        WHEN productId = UINT256 '7' OR productId = UINT256 '22' THEN 'MATIC'
        WHEN productId = UINT256 '8' THEN 'LUNA'
        WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
        WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
        WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
        WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
        ELSE CONCAT('product_id_', CAST(productId as VARCHAR))
    END AS virtual_asset,

    CASE
        WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
        WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
        WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
        WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
        WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
        WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
        WHEN productId = UINT256 '7' OR productId = UINT256 '22' THEN 'MATIC'
        WHEN productId = UINT256 '8' THEN 'LUNA'
        WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
        WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
        WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
        WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
        ELSE CONCAT('product_id_', CAST(productId as VARCHAR))
    END AS underlying_asset,  

    CASE
        WHEN productId = UINT256 '1' OR productId = UINT256 '16' THEN 'ETH'
        WHEN productId = UINT256 '2' OR productId = UINT256 '17' THEN 'BTC'
        WHEN productId = UINT256 '3' OR productId = UINT256 '18' THEN 'LINK'
        WHEN productId = UINT256 '4' OR productId = UINT256 '19' THEN 'SNX'
        WHEN productId = UINT256 '5' OR productId = UINT256 '20' THEN 'SOL'
        WHEN productId = UINT256 '6' OR productId = UINT256 '21' THEN 'AVAX'
        WHEN productId = UINT256 '7' OR productId = UINT256 '22' THEN 'MATIC'
        WHEN productId = UINT256 '8' THEN 'LUNA'
        WHEN productId = UINT256 '9' OR productId = UINT256 '23' THEN 'AAVE'
        WHEN productId = UINT256 '10' OR productId = UINT256 '24' THEN 'APE'
        WHEN productId = UINT256 '11' OR productId = UINT256 '25' THEN 'AXS'
        WHEN productId = UINT256 '12' OR productId = UINT256 '26' THEN 'UNI'
        ELSE CONCAT('product_id_', CAST(productId as VARCHAR))
    END AS market,
    contract_address AS market_address,
    GREATEST(shortValue, longValue) AS volume_usd,
    CAST(fee / 1e18 AS DOUBLE) AS fee_usd,
    GREATEST(shortValue, longValue) AS margin_usd,
    CASE
        WHEN longValue > shortValue AND trade_type = 'open' THEN 'Open Long'
        WHEN shortValue > longValue AND trade_type = 'open' THEN 'Open Short'
        WHEN longValue > shortValue AND trade_type = 'close' THEN 'Close Long'
        WHEN shortValue > longValue AND trade_type = 'close' THEN 'Close Short'
    END AS trade,
    'yfx' AS project,
    '4' AS version,
    'yfx' AS frontend,
    account AS trader,
    CAST(GREATEST(shortValue, longValue) * 1e18 AS uint256) AS volume_raw,
    evt_tx_hash AS tx_hash,
    "from" AS tx_from,
    to AS tx_to,
    evt_index
FROM all_events
WHERE evt_block_time >= DATE '2024-01-01'