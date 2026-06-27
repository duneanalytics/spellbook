{% macro zeroex_settler_agg(blockchain, start_date='2024-07-15') %}
{%- if target.name == 'ci' -%}
    {%- set start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=14)).strftime('%Y-%m-%d') -%}
{%- endif -%}
{#- Native legs (CUR2-2903) are represented with the chain's native token address from
    source('dune','blockchains') — see the native_token CTE below (not a hardcoded wrapped-token map). -#}
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
--   token_sold = deterministic (CUR2-2903), precedence per row: the unique Transfer(from=receiver) when
--     resolvable (sell_n=1, unchanged); else the calldata permit-class decode (sell_token_decoded from the
--     first take/VIP action); else native (wrapped-native token + NATIVE_CHECK.msgValue, or tx.value). CoW
--     fills (cow_rn) keep a NULL sell leg (out of scope). The permit amount is the signed sell-order size.
--   volume_usd = priced via either leg (add_amount_usd), so an unpriced buyToken still values off the sell leg.
-- NOTE: native legs (buy and sell) are represented with the chain's native token address from
-- source('dune','blockchains') (the canonical Dune native representation), which prices in
-- prices.usd_with_native on all settler chains. Native BUYS can't transfer-pivot (no ERC20 outflow of that
-- address) so they take the minAmountOut floor; native SELLS use NATIVE_CHECK.msgValue / tx.value.

WITH settler AS (
    SELECT
        tx_hash, block_time, block_number, method_id, settler_address, zid, tag, rn, cow_rn,
        buy_token, min_amount_out, settler_msgsender, trace_address,
        sell_token_decoded, sell_amount_decoded, native_value_decoded
    FROM {{ ref('zeroex_v2_' ~ blockchain ~ '_settler_txs') }}
    -- exclude the sentinel zid (non-trade fills), matching the zeroex_v2 pipeline this replaces
    WHERE zid != 0xa00000000000000000000000
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_time >= DATE '{{ start_date }}'
    {% endif %}
),

-- Per-chain native token (address/symbol/decimals) from dune.blockchains — the canonical representation
-- for native legs (per review). One row per chain; LEFT JOIN ON true / UNION'd into token_metadata below.
native_token AS (
    SELECT token_address, token_symbol, token_decimals
    FROM {{ source('dune', 'blockchains') }}
    WHERE name = '{{ blockchain }}'
),

-- Distinct partition key of the settler txs, so the transactions/logs scans prune by partition
-- (block_time + block_number + tx_hash) rather than a tx_hash-only post-scan filter, per zeroex_v2.sql's norm.
settler_tx_keys AS (
    SELECT DISTINCT block_time, block_number, tx_hash FROM settler
),

-- Tx-level sender/recipient (the settler trace's own `from` is an intermediary for nested/Relay-routed calls).
txs AS (
    SELECT t.hash AS tx_hash, t."from" AS tx_from, t."to" AS tx_to, t.value AS tx_value
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
        s.tx_hash, s.block_time, s.block_number, s.settler_address, s.zid, s.tag, s.rn, s.cow_rn,
        -- guard the floor against "no minimum" sentinels (minAmountOut >= 2^128): null them so the
        -- fallback yields null volume rather than an absurd amount. real amounts are far below 2^128.
        CASE WHEN s.min_amount_out < UINT256 '{{ max_plausible_amount }}' THEN s.min_amount_out END AS min_amount_out,
        t.tx_from, t.tx_to,
        -- receiver (the user): execute -> tx-level sender; executeMetaTxn -> msgSender (relayer pays gas).
        CASE WHEN s.method_id = 0xfd3ad6d4 THEN s.settler_msgsender ELSE t.tx_from END AS receiver,
        -- native sentinels -> the chain's native token address from dune.blockchains (canonical repr).
        -- NOTE: a native buyToken no longer transfer-pivots (the native address has no ERC20 Transfer), so
        -- native buys take the minAmountOut floor — accepted tradeoff for the canonical representation.
        CASE WHEN s.buy_token IN {{ native_tokens }} THEN nt.token_address ELSE s.buy_token END AS buy_token,
        nt.token_address AS native_addr,
        s.trace_address,
        -- guard the permit-decoded sell leg against "unlimited permit" sentinels (permitted amount
        -- >= 2^128, e.g. uint256.max): that word is then a Permit2 allowance cap, not the real fill
        -- size, so null BOTH the token and the amount and let the sell leg fall through to native/null
        -- (volume still prices off the buy side). Mirrors the min_amount_out floor guard above; without
        -- it these emit token_sold_amount_raw = uint256.max and absurd volume (~1e71 USD). (CUR2-2903)
        CASE WHEN s.sell_amount_decoded < UINT256 '{{ max_plausible_amount }}' THEN s.sell_token_decoded END AS sell_token_decoded,
        CASE WHEN s.sell_amount_decoded < UINT256 '{{ max_plausible_amount }}' THEN s.sell_amount_decoded END AS sell_amount_decoded,
        -- native sell amount: NATIVE_CHECK.msgValue (calldata; robust to nested/AllowanceHolder/4337 calls
        -- where the top-level tx.value is 0), else the top-level tx.value. NULLIF so a decoded 0 (e.g. a
        -- zero-msgValue NATIVE_CHECK) falls through to tx.value instead of sticking at 0.
        COALESCE(NULLIF(s.native_value_decoded, UINT256 '0'), t.tx_value) AS native_amt
    FROM settler s
    LEFT JOIN txs t ON t.tx_hash = s.tx_hash
    LEFT JOIN native_token nt ON true
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
    UNION ALL
    -- ensure the native token address (dune.blockchains) always resolves symbol/decimals, even where it's
    -- absent from tokens.erc20 (e.g. mode). NOT IN keeps token_metadata unique per address (no join fan-out).
    SELECT token_address, token_symbol, token_decimals
    FROM native_token
    WHERE token_address NOT IN (SELECT contract_address FROM {{ source('tokens', 'erc20') }} WHERE blockchain = '{{ blockchain }}')
),

trades AS (
    SELECT
        c.block_time, c.block_number, c.tx_hash, c.tx_from, c.tx_to, c.zid, c.tag,
        -- Trace-derived row: no log/event index exists, so emit the -1 sentinel and let the real
        -- settler-call trace_address (below) be the discriminator on the dex_aggregator merge key
        -- (blockchain, tx_hash, evt_index, trace_address) — same convention as paraswap_v6. Keying on
        -- rn (a per-tx counter ordered by zid) was both meaningless and unstable: zid ties tie-break
        -- non-deterministically, so a re-run could renumber a trade and the upsert-only merge would
        -- spawn a duplicate (cf. CUR2-1530). rn is still used below only for the calls<->legs join.
        CAST(-1 AS integer) AS evt_index, c.settler_address AS contract_address, c.receiver AS taker,
        c.buy_token AS token_bought_address,
        -- CoW-batched settler fills (cow_rn set): tx-level receiver is the CoW solver/settlement, not the user,
        -- so the receiver-pivot transfer match is unreliable amid the whole batch's transfers. Fall back to the
        -- deterministic minAmountOut floor for the bought leg and leave the sell leg null rather than risk
        -- mis-assigning another order's transfer. buyToken itself is still the deterministic calldata value.
        CASE WHEN c.cow_rn IS NOT NULL THEN c.min_amount_out
             WHEN l.buy_n = 1 THEN l.buy_amount
             ELSE c.min_amount_out END AS token_bought_amount_raw,
        -- Sell leg, deterministic (CUR2-2903), precedence per row (CoW out of scope -> NULL):
        --   1) transfer pivot (sell_n=1) — unchanged, keeps the exact on-chain amount, zero regression;
        --   2) calldata permit-class decode (recovers pivot-miss + ambiguous sell_n>=2);
        --   3) native: dune.blockchains native token address + native_amt (NATIVE_CHECK.msgValue / tx.value).
        CASE WHEN c.cow_rn IS NOT NULL THEN NULL
             WHEN l.sell_n = 1 THEN l.sell_token
             WHEN c.sell_token_decoded IS NOT NULL THEN c.sell_token_decoded
             WHEN c.native_amt > UINT256 '0' THEN c.native_addr
        END AS token_sold_address,
        CASE WHEN c.cow_rn IS NOT NULL THEN NULL
             WHEN l.sell_n = 1 THEN l.sell_amount
             WHEN c.sell_token_decoded IS NOT NULL THEN c.sell_amount_decoded
             WHEN c.native_amt > UINT256 '0' THEN c.native_amt
        END AS token_sold_amount_raw,
        c.trace_address
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
        trades.token_sold_amount_raw / POW(10, st.decimals) AS token_sold_amount,
        trades.trace_address
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
    -- Emit the REAL settler-call trace_address (not the [-1] sentinel). On the dex_aggregator.trades
    -- datashare merge key (blockchain, tx_hash, evt_index, trace_address) — which omits project/version —
    -- the old [-1] + evt_index=rn collided with cow_protocol / lifi / bitget_dex_aggregator rows (all
    -- keyed on [-1]). A real on-chain trace path is never [-1], so this keeps each project's row distinct
    -- on the datashare key while preserving both rows. Cast to array<bigint> to match the union (per dodo).
    CAST(trace_address AS array<bigint>) AS trace_address,
    'settler' AS type,
    TRUE AS swap_flag,
    contract_address
FROM results_usd
{% endmacro %}
