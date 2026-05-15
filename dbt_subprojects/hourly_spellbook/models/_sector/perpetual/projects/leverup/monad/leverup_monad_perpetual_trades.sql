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

-- LeverUp is a gTrade-fork perp DEX on Monad. Single Diamond contract at
-- 0xea1b8E4aB7f14F7dCA68c5B214303B13078FC5ec routes every market. Trades
-- are settled by a keeper, which emits one of:
--   * LimitOrderFilled        — open. Data layout (verified empirically
--       against 2026-05-12 events):
--       slot 0: trader (also in topic1), slot 1: broker, slot 2: pairIndex,
--       slot 3: price (1e18), slot 4: qty (1e10 fixed-point),
--       slot 5: pairBase (address — the market identifier),
--       slot 12: isLong.
--   * TradeClosedManual       — user-initiated close. Layout:
--       slot 0: close fill price (1e18), slots 1-4: PnL/fee/margin.
--       Slot 5+ embeds the full open snapshot (pairBase=slot 10, qty=slot 9,
--       isLong=slot 17).
--   * TradeClosedTrigger      — TP/SL trigger close. Same as manual but
--       shifted by one slot (trigger metadata at slot 0): price=slot 1,
--       embedded snapshot at slot 6 (pairBase=slot 11, qty=slot 10,
--       isLong=slot 18).
--
-- pairBase (the market_id) is a 20-byte address. For real Monad ERC20s
-- (e.g. WMON) it's the deployed contract; for synthetic markets (BTC, ETH,
-- 5x boosted, equities, commodities, forex) it's a placeholder used as
-- the canonical identifier inside LeverUp.
--
-- Symbol mapping comes from a one-shot PairAdded event (topic0
-- 0x07406f78...) emitted by the Diamond once per listing. topic1 carries
-- pairBase; the event data has the pair name as an ABI-encoded UTF-8
-- string at slots 4 (length) and 5+ (bytes). We decode every listing
-- in the market_config CTE and take the latest per pairBase (handles
-- re-listings like kPEPE -> KPEPE on 0x721D…).
--
-- Protocol-wide precision constants:
--   PRICE_PRECISION = 18 (1e18 fixed-point)
--   SIZE_PRECISION  = 10 (gTrade 1e10 fixed-point, regardless of market)

{% set leverup_contract     = '0xea1b8e4ab7f14f7dca68c5b214303b13078fc5ec' %}
{% set limit_filled_topic   = '0x49de9d6b914a309d9c2f9abede8d0e78b6c9beb3cb4467b0db09d73721cfbc80' %}
{% set close_manual_topic   = '0x3467a9d36e6bff6850880c52bbc83482131a4c2e679e0536c5ae996a6cf7df96' %}
{% set close_trigger_topic  = '0x0e28b4b0ac3704543b9d224078f41763ceaf9cc1564c56b6a79d4908adc89854' %}
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

opens AS (
    SELECT
        l.block_time, l.block_number, l.block_date, l.tx_hash, l.index AS evt_index,
        bytearray_substring(l.topic1, 13, 20)                        AS trader,
        bytearray_substring(l.data, 5 * 32 + 13, 20)                 AS pair_base,
        bytearray_to_uint256(bytearray_substring(l.data, 3 * 32 + 1, 32)) AS price_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 4 * 32 + 1, 32)) AS size_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 12 * 32 + 1, 32)) AS is_long,
        'open' AS leg
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ leverup_contract }}
      AND l.topic0 = {{ limit_filled_topic }}
      {% if is_incremental() %}
      AND {{ incremental_predicate('l.block_time') }}
      {% else %}
      AND l.block_date >= DATE '{{ project_start_date }}'
      {% endif %}
),

closes_manual AS (
    SELECT
        l.block_time, l.block_number, l.block_date, l.tx_hash, l.index AS evt_index,
        bytearray_substring(l.topic1, 13, 20)                         AS trader,
        bytearray_substring(l.data, 10 * 32 + 13, 20)                 AS pair_base,
        bytearray_to_uint256(bytearray_substring(l.data, 0 * 32 + 1, 32))  AS price_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 9 * 32 + 1, 32))  AS size_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 17 * 32 + 1, 32)) AS is_long,
        'close_manual' AS leg
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ leverup_contract }}
      AND l.topic0 = {{ close_manual_topic }}
      {% if is_incremental() %}
      AND {{ incremental_predicate('l.block_time') }}
      {% else %}
      AND l.block_date >= DATE '{{ project_start_date }}'
      {% endif %}
),

closes_trigger AS (
    SELECT
        l.block_time, l.block_number, l.block_date, l.tx_hash, l.index AS evt_index,
        bytearray_substring(l.topic1, 13, 20)                         AS trader,
        bytearray_substring(l.data, 11 * 32 + 13, 20)                 AS pair_base,
        bytearray_to_uint256(bytearray_substring(l.data, 1 * 32 + 1, 32))  AS price_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 10 * 32 + 1, 32)) AS size_raw,
        bytearray_to_uint256(bytearray_substring(l.data, 18 * 32 + 1, 32)) AS is_long,
        'close_trigger' AS leg
    FROM {{ source('monad', 'logs') }} l
    WHERE l.contract_address = {{ leverup_contract }}
      AND l.topic0 = {{ close_trigger_topic }}
      {% if is_incremental() %}
      AND {{ incremental_predicate('l.block_time') }}
      {% else %}
      AND l.block_date >= DATE '{{ project_start_date }}'
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
        WHEN a.leg = 'open' AND a.is_long = UINT256 '1' THEN 'long'
        WHEN a.leg = 'open'                             THEN 'short'
        WHEN a.is_long = UINT256 '1'                    THEN 'close-long'
        ELSE                                                 'close-short'
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
