{{ config(
    alias = 'perpetual_trades',
    schema = 'katanaperps_katana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2026-03-23' %}

WITH trade_events AS (
    SELECT
        evt_block_time AS block_time,
        evt_block_number AS block_number,
        evt_index,
        contract_address AS market_address,
        evt_tx_hash AS tx_hash,
        buyWallet AS buy_wallet,
        sellWallet AS sell_wallet,
        baseAssetSymbol AS base_asset_symbol,
        quoteAssetSymbol AS quote_asset_symbol,
        CAST(baseQuantity AS UINT256) AS base_quantity_raw,
        CAST(quoteQuantity AS DOUBLE) / 1e8 AS quote_quantity_usd,
        CAST(makerFeeQuantity AS DOUBLE) / 1e8 AS maker_fee_usd,
        CAST(takerFeeQuantity AS DOUBLE) / 1e8 AS taker_fee_usd,
        makerSide AS maker_side
    FROM {{ source('katanaperps_katana', 'exchange_v1_evt_tradeexecuted') }}
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% else -%}
    WHERE evt_block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
),

trade_sides AS (
    SELECT
        block_time,
        block_number,
        market_address,
        tx_hash,
        CAST(CAST(evt_index AS BIGINT) * 10 + 1 AS BIGINT) AS evt_index,
        buy_wallet AS trader,
        'long' AS trade,
        CASE
            WHEN maker_side = 0 THEN maker_fee_usd
            ELSE taker_fee_usd
        END AS fee_usd,
        base_asset_symbol,
        quote_asset_symbol,
        base_quantity_raw,
        quote_quantity_usd
    FROM trade_events

    UNION ALL

    SELECT
        block_time,
        block_number,
        market_address,
        tx_hash,
        CAST(CAST(evt_index AS BIGINT) * 10 + 2 AS BIGINT) AS evt_index,
        sell_wallet AS trader,
        'short' AS trade,
        CASE
            WHEN maker_side = 1 THEN maker_fee_usd
            ELSE taker_fee_usd
        END AS fee_usd,
        base_asset_symbol,
        quote_asset_symbol,
        base_quantity_raw,
        quote_quantity_usd
    FROM trade_events
),

transactions_filtered AS (
    SELECT
        hash,
        block_number,
        "from",
        "to",
        block_time,
        block_date
    FROM {{ source('katana', 'transactions') }}
    WHERE {% if is_incremental() -%}
        {{ incremental_predicate('block_time') }}
    {% else -%}
        block_time >= TIMESTAMP '{{ project_start_date }}'
    {% endif -%}
)

SELECT
    'katana' AS blockchain,
    CAST(date_trunc('day', t.block_time) AS date) AS block_date,
    CAST(date_trunc('month', t.block_time) AS date) AS block_month,
    t.block_time,
    t.base_asset_symbol AS virtual_asset,
    t.base_asset_symbol AS underlying_asset,
    CONCAT(t.base_asset_symbol, '/', t.quote_asset_symbol) AS market,
    t.market_address,
    t.quote_quantity_usd AS volume_usd,
    t.fee_usd,
    CAST(NULL AS DOUBLE) AS margin_usd,
    t.trade,
    'katanaperps' AS project,
    '1' AS version,
    'katanaperps' AS frontend,
    t.trader,
    t.base_quantity_raw AS volume_raw,
    t.tx_hash,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    t.evt_index
FROM trade_sides t
INNER JOIN transactions_filtered tx
    ON t.tx_hash = tx.hash
    AND t.block_number = tx.block_number
    AND CAST(date_trunc('day', t.block_time) AS date) = tx.block_date
