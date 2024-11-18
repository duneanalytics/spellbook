{{  config(
        schema = 'zeroex_arbitrum',
        alias = 'native_fills',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

WITH prices AS (
    SELECT
        minute,
        contract_address,
        symbol,
        price
    FROM {{ source('prices', 'usd') }}
    WHERE blockchain = 'arbitrum'
        {% if is_incremental() %}
        AND {{ incremental_predicate('minute') }}
        {% endif %}
),

tokens AS (
    SELECT
        contract_address,
        symbol,
        decimals
    FROM {{ source('tokens', 'erc20') }}
    WHERE blockchain = 'arbitrum'
),

v4_limit_fills AS (
    SELECT
        fills.evt_block_time AS block_time,
        fills.evt_block_number as block_number,
        'v4' AS protocol_version,
        'limit' as native_order_type,
        fills.evt_tx_hash AS transaction_hash,
        fills.evt_index,
        fills.maker AS maker_address,
        fills.taker AS taker_address,
        fills.makerToken AS maker_token,
        fills.takerTokenFilledAmount as taker_token_filled_amount_raw,
        fills.makerTokenFilledAmount as maker_token_filled_amount_raw,
        fills.contract_address,
        mt.symbol AS maker_symbol,
        CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair,
        fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount,
        fills.takerToken AS taker_token,
        tt.symbol AS taker_symbol,
        fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount,
        (fills.feeRecipient in
            (0x9b858be6e3047d88820f439b240deac2418a2551,0x86003b044f70dac0abc80ac8957305b6370893ed,0x5bc2419a087666148bfbe1361ae6c06d240c6131))
            AS matcha_limit_order_flag,
        CASE
                WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6)
                WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6)
                WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18)
                WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18)
                WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
            END AS volume_usd,
        fills.protocolFeePaid / 1e18 AS protocol_fee_paid_eth
    FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
    LEFT JOIN prices tp ON
        date_trunc('minute', evt_block_time) = tp.minute
        AND CASE
                WHEN fills.takerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.takerToken
            END = tp.contract_address
    LEFT JOIN prices mp ON
        DATE_TRUNC('minute', evt_block_time) = mp.minute
        AND CASE
                WHEN fills.makerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.makerToken
            END = mp.contract_address
    LEFT OUTER JOIN tokens mt ON mt.contract_address = fills.makerToken
    LEFT OUTER JOIN tokens tt ON tt.contract_address = fills.takerToken
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
        {% endif %}
),

v4_rfq_fills AS (
    SELECT
        fills.evt_block_time AS block_time,
        fills.evt_block_number as block_number,
        'v4' AS protocol_version,
        'rfq' as native_order_type,
        fills.evt_tx_hash AS transaction_hash,
        fills.evt_index,
        fills.maker AS maker_address,
        fills.taker AS taker_address,
        fills.makerToken AS maker_token,
        fills.takerTokenFilledAmount as taker_token_filled_amount_raw,
        fills.makerTokenFilledAmount as maker_token_filled_amount_raw,
        fills.contract_address,
        mt.symbol AS maker_symbol,
        CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair,
        fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount,
        fills.takerToken AS taker_token,
        tt.symbol AS taker_symbol,
        fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount,
        false AS matcha_limit_order_flag,
        CASE
                WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6)
                WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6)
                WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18)
                WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18)
                WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
            END AS volume_usd,
        cast(NULL as double) AS protocol_fee_paid_eth
    FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
    LEFT JOIN prices tp ON
        date_trunc('minute', evt_block_time) = tp.minute
        AND CASE
                WHEN fills.takerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.takerToken
            END = tp.contract_address
    LEFT JOIN prices mp ON
        DATE_TRUNC('minute', evt_block_time) = mp.minute
        AND CASE
                WHEN fills.makerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.makerToken
            END = mp.contract_address
    LEFT OUTER JOIN tokens mt ON mt.contract_address = fills.makerToken
    LEFT OUTER JOIN tokens tt ON tt.contract_address = fills.takerToken
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
        {% endif %}
),

otc_fills as (
    SELECT
        fills.evt_block_time AS block_time,
        fills.evt_block_number as block_number,
        'v4' AS protocol_version,
        'otc' as native_order_type,
        fills.evt_tx_hash AS transaction_hash,
        fills.evt_index,
        fills.maker AS maker_address,
        fills.taker AS taker_address,
        fills.makerToken AS maker_token,
        fills.takerTokenFilledAmount as taker_token_filled_amount_raw,
        fills.makerTokenFilledAmount as maker_token_filled_amount_raw,
        fills.contract_address,
        mt.symbol AS maker_symbol,
        CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair,
        fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount,
        fills.takerToken AS taker_token,
        tt.symbol AS taker_symbol,
        fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount,
        FALSE  AS matcha_limit_order_flag,
        CASE
                WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6)
                WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6)
                WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18)
                WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18)
                WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
            END AS volume_usd,
        cast(NULL as double) AS protocol_fee_paid_eth
    FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
    LEFT JOIN prices tp ON
        date_trunc('minute', evt_block_time) = tp.minute
        AND CASE
                WHEN fills.takerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.takerToken
            END = tp.contract_address
    LEFT JOIN prices mp ON
        DATE_TRUNC('minute', evt_block_time) = mp.minute
        AND CASE
                WHEN fills.makerToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 0x82af49447d8a07e3bd95bd0d56f35241523fbab1
                ELSE fills.makerToken
            END = mp.contract_address
    LEFT OUTER JOIN tokens mt ON mt.contract_address = fills.makerToken
    LEFT OUTER JOIN tokens tt ON tt.contract_address = fills.takerToken
    WHERE 1=1
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
        {% if not is_incremental() %}
        AND evt_block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
        {% endif %}
),

all_fills as (
    SELECT * FROM v4_limit_fills
    UNION ALL
    SELECT * FROM v4_rfq_fills
    UNION ALL
    SELECT * FROM otc_fills
)

SELECT DISTINCT
    all_fills.block_time,
    all_fills.block_number,
    all_fills.protocol_version AS version,
    DATE_TRUNC('day', all_fills.block_time) AS block_date,
    CAST(DATE_TRUNC('month', all_fills.block_time) AS date) AS block_month,
    all_fills.transaction_hash AS tx_hash,
    all_fills.evt_index,
    all_fills.maker_address AS maker,
    CASE
        WHEN all_fills.taker_address IN (0xdef1c0ded9bec7f1a1670819833240f027b25eff, 0xdb6f1920a889355780af7570773609bd8cb1f498) THEN tx."from"
        ELSE all_fills.taker_address
    END AS taker,
    all_fills.maker_token,
    all_fills.maker_token_filled_amount_raw AS maker_token_amount_raw,
    all_fills.taker_token,
    all_fills.taker_token_filled_amount_raw AS taker_token_amount_raw,
    all_fills.maker_symbol,
    all_fills.taker_symbol,
    all_fills.maker_asset_filled_amount AS maker_token_amount,
    all_fills.taker_asset_filled_amount AS taker_token_amount,
    all_fills.token_pair,
    all_fills.matcha_limit_order_flag,
    all_fills.native_order_type,
    CAST(all_fills.volume_usd AS double) AS volume_usd,
    all_fills.protocol_fee_paid_eth,
    'arbitrum' AS blockchain,
    all_fills.contract_address,
    tx."from" AS tx_from,
    tx.to AS tx_to
FROM all_fills
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON all_fills.transaction_hash = tx.hash
    AND all_fills.block_number = tx.block_number
    AND all_fills.block_time = tx.block_time
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{zeroex_v3_start_date}}'
    {% endif %}