{% macro zeroex_settler_agg(blockchain, start_date='2024-07-15') %}
{%- if target.name == 'ci' -%}
    {%- set start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=14)).strftime('%Y-%m-%d') -%}
{%- endif -%}
{%- set weth = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' -%}
{%- set native_tokens = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' -%}
{%- set erc20_transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -%}
{#- 2^128. minAmountOut at/above this is a "no minimum" sentinel (callers scatter across the top of the
    uint256 range, incl. UINT256_MAX), not a real token amount — even a $1B trade is well under 2^128.
    The floor fallback must ignore these or it emits absurd volume (e.g. ~1e71 USD). -#}
{%- set max_plausible_amount = '340282366920938463463374607431768211456' -%}

-- Deterministic 0x Settler aggregator decode (replaces the heuristic leg-matching + the PR #9795 band-aid).
-- Per settler call, emits ONE 0x-API aggregator row = the user's net swap:
--   token_bought = AllowedSlippage.buyToken (from calldata, deterministic).
--   token_bought amount = the unique Transfer(buyToken, to=receiver) when resolvable, else the minAmountOut
--     floor (verified tight, ~1% under actual). receiver = the tx-level sender (execute) / msgSender
--     (executeMetaTxn) — verified to be the true buyToken recipient, NOT the calldata `recipient` (which is
--     often an intermediate routing hop). Anchoring on a single calldata-named token to the verified user
--     means it cannot mis-bind to internal routing hops the way the heuristic did.
--   token_sold = best-effort: the unique Transfer(from=receiver, token != buyToken).
--   volume_usd = priced via either leg (add_amount_usd), so an unpriced buyToken still values off the sell leg.
-- NOTE: native-ETH buyToken is mapped to mainnet WETH for pricing, so it only resolves a price on ethereum;
-- per-chain wrapped-native pricing for native buys is a tracked follow-up (off-mainnet native buys -> NULL volume).

WITH settler AS (
    SELECT
        tx_hash, block_time, block_number, method_id, settler_address, zid, tag, rn,
        buy_token, min_amount_out, settler_msgsender
    FROM {{ ref('zeroex_v2_' ~ blockchain ~ '_settler_txs') }}
    -- exclude the sentinel zid (non-trade fills), matching the zeroex_v2 pipeline this replaces
    WHERE zid != 0xa00000000000000000000000
      -- Drop 0x Settler fills that execute *inside* a CoW Protocol settlement (cow_rn is set in the
      -- settler-txs staging when the GPv2Settlement address appears in the Settler call's input). There
      -- the user's trade is the CoW order — already captured by the cow_protocol model — and the Settler
      -- call is only the solver's internal routing leg. Emitting it double-counts the same swap and
      -- collides with the cow_protocol row on the dex_aggregator key (blockchain, tx_hash, evt_index,
      -- trace_address). (Underlying-venue volume from such fills is still captured in dex.trades.)
      AND cow_rn IS NULL
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_time >= DATE '{{ start_date }}'
    {% endif %}
),

-- Distinct partition key of the settler txs, so the transactions/logs scans prune by partition
-- (block_time + block_number + tx_hash) rather than a tx_hash-only post-scan filter, per zeroex_v2.sql's norm.
settler_tx_keys AS (
    SELECT DISTINCT block_time, block_number, tx_hash FROM settler
),

-- Tx-level sender/recipient (the settler trace's own `from` is an intermediary for nested/Relay-routed calls).
txs AS (
    SELECT t.hash AS tx_hash, t."from" AS tx_from, t."to" AS tx_to
    FROM {{ source(blockchain, 'transactions') }} t
    JOIN settler_tx_keys k
        ON  k.block_time = t.block_time
        AND k.block_number = t.block_number
        AND k.tx_hash = t.hash
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.block_time') }}
    {% else %}
    WHERE t.block_time >= DATE '{{ start_date }}'
    {% endif %}
),

calls AS (
    SELECT
        s.tx_hash, s.block_time, s.block_number, s.settler_address, s.zid, s.tag, s.rn,
        -- guard the floor against "no minimum" sentinels (minAmountOut >= 2^128): null them so the
        -- fallback yields null volume rather than an absurd amount. real amounts are far below 2^128.
        CASE WHEN s.min_amount_out < UINT256 '{{ max_plausible_amount }}' THEN s.min_amount_out END AS min_amount_out,
        t.tx_from, t.tx_to,
        -- receiver (the user): execute -> tx-level sender; executeMetaTxn -> msgSender (relayer pays gas).
        CASE WHEN s.method_id = 0xfd3ad6d4 THEN s.settler_msgsender ELSE t.tx_from END AS receiver,
        -- native sentinels represented/priced as WETH (see header note)
        CASE WHEN s.buy_token IN {{ native_tokens }} THEN {{ weth }} ELSE s.buy_token END AS buy_token
    FROM settler s
    LEFT JOIN txs t ON t.tx_hash = s.tx_hash
),

transfers AS (
    SELECT
        logs.block_number,
        logs.tx_hash,
        logs.contract_address AS token,
        varbinary_substring(logs.topic1, 13, 20) AS transfer_from,
        varbinary_substring(logs.topic2, 13, 20) AS transfer_to,
        -- CASE-guard the conversion (Trino only evaluates the THEN branch when the WHEN holds): a non-standard
        -- Transfer-topic log with >32-byte data would otherwise overflow bytearray_to_uint256. The WHERE filter
        -- below is not sufficient on its own — Trino may evaluate this projection before applying it.
        CASE WHEN varbinary_length(logs.data) = 32 THEN bytearray_to_uint256(logs.data) END AS amount
    FROM {{ source(blockchain, 'logs') }} AS logs
    JOIN settler_tx_keys k
        ON  k.block_time = logs.block_time
        AND k.block_number = logs.block_number
        AND k.tx_hash = logs.tx_hash
    WHERE logs.topic0 = {{ erc20_transfer_topic }}
      -- standard ERC20 value transfers only (uint256 amount is exactly 32 bytes): drops NFT/ERC721 (0-byte
      -- data) and non-standard >32-byte Transfer-topic logs. Row-reducer; the overflow guard is the CASE above.
      AND varbinary_length(logs.data) = 32
    {% if is_incremental() %}
      AND {{ incremental_predicate('logs.block_time') }}
    {% else %}
      AND logs.block_time >= DATE '{{ start_date }}'
    {% endif %}
),

-- Single pass over the transfers: buy leg = unique Transfer(buyToken, to=receiver); sell leg (best-effort) =
-- unique Transfer out of the user of a token other than buyToken.
legs AS (
    SELECT c.tx_hash, c.rn,
        count(*)            FILTER (WHERE t.token = c.buy_token AND t.transfer_to = c.receiver)   AS buy_n,
        arbitrary(t.amount) FILTER (WHERE t.token = c.buy_token AND t.transfer_to = c.receiver)   AS buy_amount,
        count(*)            FILTER (WHERE t.transfer_from = c.receiver AND t.token <> c.buy_token) AS sell_n,
        arbitrary(t.token)  FILTER (WHERE t.transfer_from = c.receiver AND t.token <> c.buy_token) AS sell_token,
        arbitrary(t.amount) FILTER (WHERE t.transfer_from = c.receiver AND t.token <> c.buy_token) AS sell_amount
    FROM calls c
    JOIN transfers t
        ON  t.tx_hash = c.tx_hash
        AND t.block_number = c.block_number
        AND t.amount > UINT256 '0'
        AND (
              (t.token = c.buy_token AND t.transfer_to = c.receiver)
           OR (t.transfer_from = c.receiver AND t.token <> c.buy_token)
        )
    GROUP BY c.tx_hash, c.rn
),

token_metadata AS (
    SELECT contract_address, symbol, decimals
    FROM {{ source('tokens', 'erc20') }}
    WHERE blockchain = '{{ blockchain }}'
),

trades AS (
    SELECT
        c.block_time, c.block_number, c.tx_hash, c.tx_from, c.tx_to, c.zid, c.tag,
        c.rn AS evt_index, c.settler_address AS contract_address, c.receiver AS taker,
        c.buy_token AS token_bought_address,
        -- bought leg = the unique Transfer(buyToken, to=receiver); fall back to the deterministic
        -- minAmountOut floor when the receiver-pivot match isn't unique. (CoW-nested settler fills are
        -- excluded upstream in the `settler` CTE, so the receiver pivot is reliable for the rows kept here.)
        CASE WHEN l.buy_n = 1 THEN l.buy_amount
             ELSE c.min_amount_out END AS token_bought_amount_raw,
        CASE WHEN l.sell_n = 1 THEN l.sell_token  END AS token_sold_address,
        CASE WHEN l.sell_n = 1 THEN l.sell_amount END AS token_sold_amount_raw
    FROM calls c
    LEFT JOIN legs l ON l.tx_hash = c.tx_hash AND l.rn = c.rn
),

results AS (
    SELECT
        '{{ blockchain }}' AS blockchain,
        trades.block_time, trades.block_number, trades.tx_hash, trades.tx_from, trades.tx_to,
        trades.zid, trades.tag, trades.evt_index, trades.contract_address, trades.taker,
        CAST(NULL AS varbinary) AS maker,
        trades.token_bought_address,
        bt.symbol AS bought_symbol,
        trades.token_bought_amount_raw,
        trades.token_bought_amount_raw / POW(10, bt.decimals) AS token_bought_amount,
        trades.token_sold_address,
        st.symbol AS sold_symbol,
        trades.token_sold_amount_raw,
        trades.token_sold_amount_raw / POW(10, st.decimals) AS token_sold_amount
    FROM trades
    LEFT JOIN token_metadata bt ON bt.contract_address = trades.token_bought_address
    LEFT JOIN token_metadata st ON st.contract_address = trades.token_sold_address
),

results_usd AS (
    {{ add_amount_usd(trades_cte = 'results') }}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '0x-API' AS project,
    'settler' AS version,
    cast(DATE_TRUNC('day', block_time) as date) AS block_date,
    cast(DATE_TRUNC('month', block_time) as date) AS block_month,
    block_time,
    block_number,
    sold_symbol AS taker_symbol,
    bought_symbol AS maker_symbol,
    CASE WHEN LOWER(sold_symbol) > LOWER(bought_symbol) THEN CONCAT(bought_symbol, '-', sold_symbol) ELSE CONCAT(sold_symbol, '-', bought_symbol) END AS token_pair,
    token_sold_amount AS taker_token_amount,
    token_bought_amount AS maker_token_amount,
    token_sold_amount_raw AS taker_token_amount_raw,
    token_bought_amount_raw AS maker_token_amount_raw,
    amount_usd AS volume_usd,
    token_sold_address AS taker_token,
    token_bought_address AS maker_token,
    taker,
    maker,
    tag,
    zid,
    tx_hash,
    tx_from,
    tx_to,
    evt_index,
    (ARRAY[-1]) AS trace_address,
    'settler' AS type,
    TRUE AS swap_flag,
    contract_address
FROM results_usd
{% endmacro %}
