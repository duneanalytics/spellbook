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

-- Perpl is an orderbook perp DEX on Monad, settled in AUSD via a single
-- exchange contract. Each fill emits a MakerOrderFilled event for the
-- matched maker leg plus a TakerOrderFilled summary event for the taker.
-- Per-market config (priceDecimals, lotDecimals, asset symbol/name) is
-- captured from the addContract function call at listing time. Account
-- → wallet mapping comes from the AccountCreated event.
--
-- This model reads exclusively from Dune's decoded `perpl_monad.v1_*`
-- tables (no slot-level decoding of raw monad.logs). See sources YAML.

{% set project_start_date = '2026-02-11' %}        -- first addContract call (BTC + MON listed)
{% set lookup_lower_bound = '2026-01-01' %}        -- wide hint for the full-history lookup CTEs

WITH

market_config AS (
    -- Per-market priceDecimals, lotDecimals, and ticker. Take the latest
    -- successful addContract() call per perpId (handles re-listings).
    -- Listings are one-shot historical metadata, so we scan full history
    -- (no incremental predicate).
    SELECT perpId AS market_id,
           priceDecimals AS price_precision,
           lotDecimals   AS size_precision,
           symbol,
           name
    FROM (
        SELECT perpId, priceDecimals, lotDecimals, symbol, name, call_block_time,
               row_number() OVER (PARTITION BY perpId ORDER BY call_block_time DESC) AS rn
        FROM {{ source('perpl_monad', 'v1_call_addcontract') }}
        WHERE call_success = true
          AND call_block_date >= DATE '{{ lookup_lower_bound }}'
    )
    WHERE rn = 1
),

account_map AS (
    -- account_id -> wallet binding via AccountCreated. One row per
    -- account, full history scan.
    SELECT id AS account_id,
           account AS wallet
    FROM {{ source('perpl_monad', 'v1_evt_accountcreated') }}
    WHERE evt_block_date >= DATE '{{ lookup_lower_bound }}'
),

maker_fills AS (
    SELECT
        evt_block_time,
        evt_block_number,
        evt_block_date,
        evt_tx_hash AS tx_hash,
        evt_tx_from,
        evt_tx_to,
        evt_index,
        accountId AS maker_account_id,
        perpId    AS market_id,
        pricePNS  AS price_raw,
        lotLNS    AS size_raw,
        feeCNS    AS fee_raw,
        -- amountCNS < 0 => maker bought => taker sold => 'short'.
        CASE WHEN amountCNS < INT256 '0' THEN 'short' ELSE 'long' END AS trade
    FROM {{ source('perpl_monad', 'v1_evt_makerorderfilled') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% else %}
    WHERE evt_block_date >= DATE '{{ project_start_date }}'
    {% endif %}
),

position_events AS (
    -- Every position-state event carries (accountId, perpId). In a fill
    -- tx, the taker's position changes alongside one or more makers'
    -- positions — so for a given (tx_hash, perpId) we see the maker's
    -- accountId (already in maker_fills) plus the taker's accountId.
    -- We use this to recover the taker's account: settlement txs are
    -- submitted by a keeper bot, so `evt_tx_from` would point at the
    -- keeper, not the user.
    SELECT evt_block_time, evt_tx_hash AS tx_hash, accountId, perpId AS market_id
    FROM {{ source('perpl_monad', 'v1_evt_positionopened') }}
    {% if is_incremental() %}WHERE {{ incremental_predicate('evt_block_time') }}{% else %}WHERE evt_block_date >= DATE '{{ project_start_date }}'{% endif %}
    UNION ALL
    SELECT evt_block_time, evt_tx_hash, accountId, perpId
    FROM {{ source('perpl_monad', 'v1_evt_positionincreased') }}
    {% if is_incremental() %}WHERE {{ incremental_predicate('evt_block_time') }}{% else %}WHERE evt_block_date >= DATE '{{ project_start_date }}'{% endif %}
    UNION ALL
    SELECT evt_block_time, evt_tx_hash, accountId, perpId
    FROM {{ source('perpl_monad', 'v1_evt_positiondecreased') }}
    {% if is_incremental() %}WHERE {{ incremental_predicate('evt_block_time') }}{% else %}WHERE evt_block_date >= DATE '{{ project_start_date }}'{% endif %}
    UNION ALL
    SELECT evt_block_time, evt_tx_hash, accountId, perpId
    FROM {{ source('perpl_monad', 'v1_evt_positionclosed') }}
    {% if is_incremental() %}WHERE {{ incremental_predicate('evt_block_time') }}{% else %}WHERE evt_block_date >= DATE '{{ project_start_date }}'{% endif %}
    UNION ALL
    SELECT evt_block_time, evt_tx_hash, accountId, perpId
    FROM {{ source('perpl_monad', 'v1_evt_positioninverted') }}
    {% if is_incremental() %}WHERE {{ incremental_predicate('evt_block_time') }}{% else %}WHERE evt_block_date >= DATE '{{ project_start_date }}'{% endif %}
),

maker_accounts AS (
    -- All accountIds that appear as the MAKER side in any fill in this tx
    -- for this perpId. Subtract from position_events to find the taker.
    SELECT DISTINCT tx_hash, market_id, maker_account_id AS account_id
    FROM maker_fills
),

taker_accounts AS (
    -- The taker is the accountId that has a position change in the same
    -- (tx_hash, perpId) but is NOT a maker. Pick MIN if multiple.
    SELECT pe.tx_hash, pe.market_id, MIN(pe.accountId) AS taker_account_id
    FROM position_events pe
    LEFT JOIN maker_accounts ma
      ON ma.tx_hash = pe.tx_hash
     AND ma.market_id = pe.market_id
     AND ma.account_id = pe.accountId
    WHERE ma.account_id IS NULL
    GROUP BY pe.tx_hash, pe.market_id
)

SELECT
    'monad'                                           AS blockchain,
    mf.evt_block_date                                 AS block_date,
    CAST(date_trunc('month', mf.evt_block_time) AS date) AS block_month,
    mf.evt_block_time                                 AS block_time,
    mc.symbol                                         AS virtual_asset,
    'AUSD'                                            AS underlying_asset,
    COALESCE(mc.symbol || '-AUSD',
             'market_' || CAST(mf.market_id AS VARCHAR)) AS market,
    0x34b6552d57a35a1d042ccae1951bd1c370112a6f        AS market_address,
    -- volume = price * size / 10^(priceDecimals + lotDecimals)
    CAST(mf.price_raw AS DOUBLE) * CAST(mf.size_raw AS DOUBLE)
        / power(10, CAST(mc.price_precision AS DOUBLE) + CAST(mc.size_precision AS DOUBLE))
        AS volume_usd,
    -- fee: feeCNS is AUSD raw (6 decimals).
    CAST(mf.fee_raw AS DOUBLE) / 1e6                  AS fee_usd,
    CAST(NULL AS DOUBLE)                              AS margin_usd,
    mf.trade,
    'perpl'                                           AS project,
    '1'                                               AS version,
    'perpl'                                           AS frontend,
    -- trader: the taker's wallet. We recover the taker's accountId from
    -- the sibling position event in the same (tx_hash, perpId), excluding
    -- the maker's accountId. Falls back to the maker's wallet if no
    -- such position event exists (rare — e.g., for fills that don't
    -- emit a position event).
    COALESCE(am_taker.wallet, am.wallet)              AS trader,
    -- Normalize size to 18 decimals of the base asset:
    --   volume_raw = lotLNS * 10^(18 - lotDecimals)
    mf.size_raw * CAST(power(10, 18 - CAST(mc.size_precision AS INTEGER)) AS UINT256) AS volume_raw,
    mf.tx_hash,
    mf.evt_tx_from                                    AS tx_from,
    mf.evt_tx_to                                      AS tx_to,
    mf.evt_index
FROM maker_fills mf
LEFT JOIN taker_accounts ta ON ta.tx_hash = mf.tx_hash AND ta.market_id = mf.market_id
LEFT JOIN account_map am       ON am.account_id       = mf.maker_account_id
LEFT JOIN account_map am_taker ON am_taker.account_id = ta.taker_account_id
LEFT JOIN market_config mc ON mc.market_id = mf.market_id
