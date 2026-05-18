{{ config(
    alias = 'perpetual_trades',
    schema = 'leverup_monad',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- LeverUp is a gTrade-fork perp DEX on Monad, settled in USD via a single
-- Diamond contract 0xea1b8E4aB7f14F7dCA68c5B214303B13078FC5ec. Three fill
-- events drive perpetual_trades:
--   * OpenMarketTrade        — the actual fill for any open (both market
--     orders and limit-order fills route through it). The `ot` JSON column
--     carries the full open-trade struct: pairBase, entryPrice, qty, isLong.
--   * CloseTradeSuccessful   — user-initiated close. `closeInfo` JSON only
--     carries close-side fields (closePrice, fees, pnl); we look up
--     pair_base / size_raw / is_long from the matching open via tradeHash.
--   * ExecuteCloseSuccessful — keeper-triggered close (TP / SL / liquidation).
--     Same closeInfo shape as the manual close; executionType encodes the
--     trigger type.
--
-- Symbol mapping (pairBase address -> "BTC/USD" etc.) still comes from raw
-- monad.logs PairAdded events: the leverup_monad.leverup_evt_pairadded
-- decoded table exists but is empty as of 2026-05-18, so Dune has not yet
-- ingested the listings; the raw-log decode below is the only source.
--
-- Protocol-wide precision constants (gTrade convention):
--   PRICE_PRECISION = 18 (1e18 fixed-point)
--   SIZE_PRECISION  = 10 (1e10 fixed-point, regardless of market)

{% set leverup_contract     = '0xea1b8e4ab7f14f7dca68c5b214303b13078fc5ec' %}
{% set pair_added_topic     = '0x07406f7848ebae7b4bdbf957a2bc69ba5ffcebccd2b386244e6ccd3f47634fbf' %}
-- First PairAdded was 2025-10-28; first trade fill came a few days later.
{% set project_start_date   = '2025-10-01' %}
-- Wide lower-bound hint for the PairAdded lookup CTE: it scans full
-- history (not time-series). Anchor before any LeverUp activity.
{% set lookup_lower_bound   = '2025-10-01' %}

WITH

market_config AS (
    -- Resolve pair_base -> pair_name from on-chain PairAdded events.
    -- A pairBase may be listed multiple times (rename / reconfig); pick
    -- the latest. Listings are one-shot historical metadata, so we scan
    -- full history and do NOT use the incremental predicate here.
    SELECT
        pair_base,
        pair_name,
        regexp_extract(pair_name, '^([^/]+)') AS virtual_asset
    FROM (
        SELECT
            bytearray_substring(l.topic1, 13, 20) AS pair_base,
            from_utf8(
                bytearray_substring(
                    l.data,
                    5 * 32 + 1,
                    CAST(bytearray_to_uint256(bytearray_substring(l.data, 4 * 32 + 1, 32)) AS INTEGER)
                )
            ) AS pair_name,
            row_number() OVER (
                PARTITION BY bytearray_substring(l.topic1, 13, 20)
                ORDER BY l.block_time DESC
            ) AS rn
        FROM {{ source('monad', 'logs') }} l
        WHERE l.contract_address = {{ leverup_contract }}
          AND l.topic0 = {{ pair_added_topic }}
          AND l.block_date >= DATE '{{ lookup_lower_bound }}'
    )
    WHERE rn = 1
),

-- Full-history projection of opens. Used twice: once as the source for
-- the opens leg (filtered by incremental predicate downstream), and once
-- as a tradeHash -> open-snapshot lookup for the closes (NOT filtered, so
-- closes that fire in this window against trades opened earlier still join).
opens_decoded AS (
    SELECT
        o.evt_block_time                                                  AS block_time,
        o.evt_block_number                                                AS block_number,
        o.evt_block_date                                                  AS block_date,
        o.evt_tx_hash                                                     AS tx_hash,
        o.evt_index                                                       AS evt_index,
        o."user"                                                          AS trader,
        o.tradeHash                                                       AS trade_hash,
        from_hex(substring(json_extract_scalar(o.ot, '$.pairBase'), 3))   AS pair_base,
        CAST(json_extract_scalar(o.ot, '$.entryPrice') AS UINT256)        AS price_raw,
        CAST(json_extract_scalar(o.ot, '$.qty')        AS UINT256)        AS size_raw,
        json_extract_scalar(o.ot, '$.isLong') = 'true'                    AS is_long
    FROM {{ source('leverup_monad', 'leverup_evt_openmarkettrade') }} o
    WHERE o.evt_block_date >= DATE '{{ project_start_date }}'
),

opens AS (
    SELECT *, 'open' AS leg
    FROM opens_decoded
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
),

closes_manual AS (
    SELECT
        c.evt_block_time                                                  AS block_time,
        c.evt_block_number                                                AS block_number,
        c.evt_block_date                                                  AS block_date,
        c.evt_tx_hash                                                     AS tx_hash,
        c.evt_index                                                       AS evt_index,
        c."user"                                                          AS trader,
        c.tradeHash                                                       AS trade_hash,
        m.pair_base,
        CAST(json_extract_scalar(c.closeInfo, '$.closePrice') AS UINT256) AS price_raw,
        m.size_raw,
        m.is_long,
        'close_manual' AS leg
    FROM {{ source('leverup_monad', 'leverup_evt_closetradesuccessful') }} c
    LEFT JOIN opens_decoded m
      ON m.trade_hash = c.tradeHash
    WHERE c.evt_block_date >= DATE '{{ project_start_date }}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('c.evt_block_time') }}
    {% endif %}
),

closes_trigger AS (
    SELECT
        c.evt_block_time                                                  AS block_time,
        c.evt_block_number                                                AS block_number,
        c.evt_block_date                                                  AS block_date,
        c.evt_tx_hash                                                     AS tx_hash,
        c.evt_index                                                       AS evt_index,
        c."user"                                                          AS trader,
        c.tradeHash                                                       AS trade_hash,
        m.pair_base,
        CAST(json_extract_scalar(c.closeInfo, '$.closePrice') AS UINT256) AS price_raw,
        m.size_raw,
        m.is_long,
        'close_trigger' AS leg
    FROM {{ source('leverup_monad', 'leverup_evt_executeclosesuccessful') }} c
    LEFT JOIN opens_decoded m
      ON m.trade_hash = c.tradeHash
    WHERE c.evt_block_date >= DATE '{{ project_start_date }}'
    {% if is_incremental() %}
      AND {{ incremental_predicate('c.evt_block_time') }}
    {% endif %}
),

all_legs AS (
    SELECT * FROM opens
    UNION ALL SELECT * FROM closes_manual
    UNION ALL SELECT * FROM closes_trigger
),

transactions_filtered AS (
    SELECT hash, block_time, block_date, "from" AS tx_from, "to" AS tx_to
    FROM {{ source('monad', 'transactions') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% else %}
    WHERE block_date >= DATE '{{ project_start_date }}'
    {% endif %}
)

SELECT
    'monad' AS blockchain,
    CAST(date_trunc('day',  a.block_time) AS date) AS block_date,
    CAST(date_trunc('month', a.block_time) AS date) AS block_month,
    a.block_time,
    mc.virtual_asset,
    'USD' AS underlying_asset,
    mc.pair_name AS market,
    {{ leverup_contract }} AS market_address,
    -- volume_usd = price * size / 10^(price_precision + size_precision)
    --            = price_raw * size_raw / 10^(18 + 10) = / 1e28
    CAST(a.price_raw AS DOUBLE) * CAST(a.size_raw AS DOUBLE) / 1e28 AS volume_usd,
    CAST(NULL AS DOUBLE) AS fee_usd,
    CAST(NULL AS DOUBLE) AS margin_usd,
    -- Open legs report the position's direction directly. Close legs
    -- report the direction of the position being closed (prefix 'close-').
    CASE
        WHEN a.leg = 'open' AND a.is_long THEN 'long'
        WHEN a.leg = 'open'                THEN 'short'
        WHEN a.is_long                     THEN 'close-long'
        ELSE                                    'close-short'
    END AS trade,
    'leverup' AS project,
    '1' AS version,
    'leverup' AS frontend,
    a.trader,
    -- Normalize fill size to 18-decimal units of the base asset:
    --   size_raw is in 1e10 fixed-point (gTrade convention, protocol-wide).
    --   volume_raw = size_raw * 10^(18 - 10) = size_raw * 1e8.
    -- Divide volume_raw by 10^18 to get human-readable base-asset units.
    a.size_raw * UINT256 '100000000' AS volume_raw,
    a.tx_hash,
    tx.tx_from,
    tx.tx_to,
    a.evt_index
FROM all_legs a
LEFT JOIN market_config mc ON mc.pair_base = a.pair_base
INNER JOIN transactions_filtered tx
    ON tx.hash = a.tx_hash
    AND tx.block_date = a.block_date
