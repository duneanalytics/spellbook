{{ config(
    alias = 'perpetual_trades',
    schema = 'perpl_monad',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Perpl is an orderbook perp DEX on Monad. All markets are settled
-- against AUSD (6 decimals) by the single exchange contract
-- 0x34B6552d57a35a1D042CcAe1951BD1C370112a6F. Each fill emits:
--   - MakerOrderFilled (one per matched maker leg) — fields per slot:
--       0: perpId / market_id
--       1: maker accountId
--       2: orderId
--       3: pricePNS   (raw price; scaled by ContractAdded.priceDecimals)
--       4: lotLNS     (raw fill size; scaled by ContractAdded.lotDecimals)
--       5: feeCNS     (AUSD raw, 6 decimals)
--       6: lockedBalanceCNS
--       7: amountCNS  (signed int256; sign = maker direction)
--       8: balanceCNS
--   - TakerOrderFilled (once per taker order in the same tx) — slot 1
--     carries the taker's accountId.
--   - AccountDeposit — binds an accountId to a wallet via tx.from on the
--     deposit tx (used as the account_id -> wallet map).
--   - ContractAdded — emitted once per market; slot 5 = priceDecimals,
--     slot 6 = lotDecimals.
--
-- Trade attribution: each row corresponds to one MakerOrderFilled event,
-- with `trader` set to the taker's wallet (the EOA that initiated the
-- tx) and `trade` describing the taker's direction. amountCNS < 0 means
-- the maker bought, so the taker sold (short).

{% set project_start_date = '2026-02-11' %}
-- Wide lower bound for the historical-lookup CTEs (deposits, market_config).
-- Those scan FULL history (not time-series) per the Spellbook convention:
-- "do NOT use incremental_predicate when checking full history (e.g., pool
-- creation events)" (.cursor/rules/dbt-core-conventions.mdc:20). We still
-- want a partition-prune hint, so anchor to a date safely before any Perpl
-- activity — earliest market listing on Monad was 2026-02-11.
{% set lookup_lower_bound = '2026-01-01' %}
{% set perpl_contract = '0x34b6552d57a35a1d042ccae1951bd1c370112a6f' %}
{% set maker_topic   = '0xf5f5aef063f495816b6982c44c70c97c8fbec2fa24b73487e6542ce021431214' %}
{% set taker_topic   = '0x02d2bf39d2355aaa5486487e934403fd3ba3f88c73ab71938cee11931fddeb7b' %}
{% set deposit_topic = '0x84b5adc99a7d46a960b87da6e4882021bab65d63c6d02c9bcfc52f02e710b1e3' %}
{% set listing_topic = '0xd78b2274f0f8c5fb36f3464d4fc273e80ba45dde960a535eeacf3c1ae42c4e3c' %}

WITH

deposits AS (
    -- account_id -> wallet binding via the original deposit tx. Listings
    -- and deposits happen well before fills, so this CTE is NOT
    -- incremental: it always scans the full history.
    SELECT
        bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) AS account_id,
        t."from" AS wallet,
        l.block_time AS ts
    FROM {{ source('monad', 'logs') }} l
    INNER JOIN {{ source('monad', 'transactions') }} t
        ON t.hash = l.tx_hash
        AND t.block_date = l.block_date
        AND t.block_time = l.block_time
    WHERE l.contract_address = {{ perpl_contract }}
      AND l.topic0 = {{ deposit_topic }}
      AND l.block_date >= DATE '{{ lookup_lower_bound }}'
      AND t.block_date >= DATE '{{ lookup_lower_bound }}'
),

account_map AS (
    SELECT
        account_id,
        MIN_BY(wallet, ts) AS wallet
    FROM deposits
    GROUP BY account_id
),

market_config AS (
    -- Per-market priceDecimals / lotDecimals from the one-time listing
    -- event. Listings are not time-series — always scan full history.
    SELECT
        bytearray_to_uint256(bytearray_substring(l.data,   1, 32)) AS market_id,
        bytearray_to_uint256(bytearray_substring(l.data, 161, 32)) AS price_precision,
        bytearray_to_uint256(bytearray_substring(l.data, 193, 32)) AS size_precision
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ perpl_contract }}
      AND l.topic0 = {{ listing_topic }}
      AND l.block_date >= DATE '{{ lookup_lower_bound }}'
),

maker_fills AS (
    SELECT
        l.block_time,
        l.block_number,
        l.block_date,
        l.tx_hash,
        l.index AS evt_index,
        bytearray_to_uint256(bytearray_substring(l.data,   1, 32)) AS market_id,
        bytearray_to_uint256(bytearray_substring(l.data,  33, 32)) AS maker_account_id,
        bytearray_to_uint256(bytearray_substring(l.data,  97, 32)) AS price_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 129, 32)) AS size_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 161, 32)) AS fee_raw,
        -- High bit of slot 7 (signed int256 amountCNS): >= 128 means
        -- negative => maker bought.
        bytearray_to_bigint(bytearray_substring(l.data, 225, 1))   AS amount_high_byte
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ perpl_contract }}
      AND l.topic0 = {{ maker_topic }}
      {% if is_incremental() %}
      AND {{ incremental_predicate('l.block_time') }}
      {% else %}
      AND l.block_date >= DATE '{{ project_start_date }}'
      {% endif %}
),

taker_fills AS (
    -- One row per tx with the taker's account_id (slot 1 of
    -- TakerOrderFilled). For typical single-taker txs all maker legs
    -- share the same taker; collapse with MIN to one row per tx.
    SELECT
        l.tx_hash,
        MIN(bytearray_to_uint256(bytearray_substring(l.data, 33, 32))) AS taker_account_id
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ perpl_contract }}
      AND l.topic0 = {{ taker_topic }}
      {% if is_incremental() %}
      AND {{ incremental_predicate('l.block_time') }}
      {% else %}
      AND l.block_date >= DATE '{{ project_start_date }}'
      {% endif %}
    GROUP BY l.tx_hash
),

transactions_filtered AS (
    SELECT
        hash,
        block_time,
        block_date,
        "from" AS tx_from,
        "to"   AS tx_to
    FROM {{ source('monad', 'transactions') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% else %}
    WHERE block_date >= DATE '{{ project_start_date }}'
    {% endif %}
)

SELECT
    'monad' AS blockchain,
    CAST(date_trunc('day', mf.block_time) AS date)   AS block_date,
    CAST(date_trunc('month', mf.block_time) AS date) AS block_month,
    mf.block_time,
    -- Base / quote tickers. Perpl's on-chain ContractAdded event does
    -- not expose the symbol string in a documented slot (Exchange.json
    -- ABI in PerplFoundation/dex-sdk only declares addContract() as a
    -- function), so the mapping is maintained inline. Update when a
    -- new market lists.
    CASE mf.market_id
        WHEN UINT256 '1'  THEN 'BTC'
        WHEN UINT256 '10' THEN 'MON'
        WHEN UINT256 '20' THEN 'ETH'
        WHEN UINT256 '30' THEN 'SOL'
    END AS virtual_asset,
    'AUSD' AS underlying_asset,
    COALESCE(
        CASE mf.market_id
            WHEN UINT256 '1'  THEN 'BTC-AUSD'
            WHEN UINT256 '10' THEN 'MON-AUSD'
            WHEN UINT256 '20' THEN 'ETH-AUSD'
            WHEN UINT256 '30' THEN 'SOL-AUSD'
        END,
        'market_' || CAST(mf.market_id AS VARCHAR)
    ) AS market,
    {{ perpl_contract }} AS market_address,
    -- volume = price * size / 10^(priceDecimals + lotDecimals)
    CAST(mf.price_raw AS DOUBLE)
        * CAST(mf.size_raw AS DOUBLE)
        / power(10, CAST(mc.price_precision AS DOUBLE) + CAST(mc.size_precision AS DOUBLE))
        AS volume_usd,
    -- fee: feeCNS is in AUSD raw units (6 decimals)
    CAST(mf.fee_raw AS DOUBLE) / 1e6 AS fee_usd,
    CAST(NULL AS DOUBLE) AS margin_usd,
    -- Taker direction = opposite of maker. amount_high_byte >= 128
    -- means amountCNS < 0 => maker bought => taker sold => 'short'.
    CASE WHEN mf.amount_high_byte >= 128 THEN 'short' ELSE 'long' END AS trade,
    'perpl' AS project,
    '1' AS version,
    'perpl' AS frontend,
    am.wallet AS trader,
    -- Normalize fill size to 18-decimal units of the base asset:
    --   volume_raw = lotLNS * 10^(18 - lotDecimals)
    -- e.g. BTC market (lotDecimals=5) with raw lot 17 (=0.00017 BTC)
    -- yields volume_raw = 17 * 10^13 = 170000000000000.
    mf.size_raw * CAST(power(10, 18 - CAST(mc.size_precision AS INTEGER)) AS UINT256) AS volume_raw,
    mf.tx_hash,
    tx.tx_from,
    tx.tx_to,
    mf.evt_index
FROM maker_fills mf
LEFT JOIN taker_fills  tf ON tf.tx_hash = mf.tx_hash
LEFT JOIN account_map  am ON am.account_id = COALESCE(tf.taker_account_id, mf.maker_account_id)
LEFT JOIN market_config mc ON mc.market_id  = mf.market_id
INNER JOIN transactions_filtered tx
    ON tx.hash = mf.tx_hash
    AND tx.block_date = mf.block_date
