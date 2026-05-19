{{
    config(
        schema = 'nad_fun_v3_monad',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- nad.fun is a memecoin launchpad on Monad. After a token graduates from
-- the bonding curve, every on-platform trade routes through one of two
-- decoded router contracts that wrap a Uniswap V3-style pool (one pool
-- per token, always paired against WMON). The routers are the canonical
-- nad.fun trade surface; the underlying pool also accepts traffic from
-- aggregators / MEV bots / direct callers, which is NOT nad.fun activity
-- and is intentionally excluded.
--
-- Router contracts (both emit DexRouterBuy / DexRouterSell with the same
-- shape: sender, token, amountIn, amountOut):
--   * swap router 0x003989CD92C31A51D8B20bDBd0c51E444f88d081 — the main
--     user-facing path; large median trade.
--   * dex  router 0x0B79d71AE99528D1dB24A4148b5f4F865cc2b137 — high-volume
--     small-ticket path (bots / micro-trades).
--
-- Convention: on a BUY, amountIn is WMON spent (gross of router fee) and
-- amountOut is token received. On a SELL, amountIn is token spent and
-- amountOut is WMON received (net of router fee). We surface those raw
-- amounts as-is — i.e., `token_sold_amount_raw` on a buy IS the
-- gross WMON paid by the user, including the router fee.

{% set wmon = '0x3bd359c1119da7da1d913d1c4d2b7c461115433a' %}
{% set project_start_date = '2025-11-01' %}

WITH

router_buys AS (
    SELECT
        evt_block_time, evt_block_number, evt_tx_hash, evt_index,
        contract_address, sender, token, amountIn, amountOut
    FROM {{ source('nad_fun_monad', 'swap_evt_dexrouterbuy') }}
    WHERE
        {% if is_incremental() %}{{ incremental_predicate('evt_block_time') }}
        {% else %}evt_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
    UNION ALL
    SELECT
        evt_block_time, evt_block_number, evt_tx_hash, evt_index,
        contract_address, sender, token, amountIn, amountOut
    FROM {{ source('nad_fun_monad', 'dexrouter_evt_dexrouterbuy') }}
    WHERE
        {% if is_incremental() %}{{ incremental_predicate('evt_block_time') }}
        {% else %}evt_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
),

router_sells AS (
    SELECT
        evt_block_time, evt_block_number, evt_tx_hash, evt_index,
        contract_address, sender, token, amountIn, amountOut
    FROM {{ source('nad_fun_monad', 'swap_evt_dexroutersell') }}
    WHERE
        {% if is_incremental() %}{{ incremental_predicate('evt_block_time') }}
        {% else %}evt_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
    UNION ALL
    SELECT
        evt_block_time, evt_block_number, evt_tx_hash, evt_index,
        contract_address, sender, token, amountIn, amountOut
    FROM {{ source('nad_fun_monad', 'dexrouter_evt_dexroutersell') }}
    WHERE
        {% if is_incremental() %}{{ incremental_predicate('evt_block_time') }}
        {% else %}evt_block_date >= DATE '{{ project_start_date }}'
        {% endif %}
)

SELECT
    'monad' AS blockchain,
    'nad_fun' AS project,
    '1' AS version,
    CAST(date_trunc('month', evt_block_time) AS date) AS block_month,
    CAST(date_trunc('day',   evt_block_time) AS date) AS block_date,
    evt_block_time AS block_time,
    evt_block_number AS block_number,
    amountOut AS token_bought_amount_raw,
    amountIn  AS token_sold_amount_raw,
    token AS token_bought_address,
    {{ wmon }} AS token_sold_address,
    sender AS taker,
    CAST(NULL AS varbinary) AS maker,
    contract_address AS project_contract_address,
    evt_tx_hash AS tx_hash,
    evt_index
FROM router_buys

UNION ALL

SELECT
    'monad' AS blockchain,
    'nad_fun' AS project,
    '1' AS version,
    CAST(date_trunc('month', evt_block_time) AS date) AS block_month,
    CAST(date_trunc('day',   evt_block_time) AS date) AS block_date,
    evt_block_time AS block_time,
    evt_block_number AS block_number,
    amountOut AS token_bought_amount_raw,
    amountIn  AS token_sold_amount_raw,
    {{ wmon }} AS token_bought_address,
    token AS token_sold_address,
    sender AS taker,
    CAST(NULL AS varbinary) AS maker,
    contract_address AS project_contract_address,
    evt_tx_hash AS tx_hash,
    evt_index
FROM router_sells
