{% macro zeroex_settler_rfq(blockchain, project='0x API', version='settler', start_date='2024-07-15') %}
{#- In CI, floor the full-refresh window to the last 14 days, matching the settler-txs staging's own CI floor
    (and zeroex_settler_agg) so the logs/transactions joins stay cheap. The staging only carries 14 days in CI,
    so this is the binding window regardless; the seed test's fixed-date rows must fall inside it (keep fresh). -#}
{%- if target.name == 'ci' -%}
    {%- set start_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=14)).strftime('%Y-%m-%d') -%}
{%- endif -%}

-- Robust 0x Settler plain-RFQ decoder (action selector 0xd92aadfb), ported from Dune's echo indexer
-- (crates/dex-trades-indexer/src/modules/zero_ex_settler.rs).
-- Token identities come from the signed RFQ action's calldata static head; amounts come from the real
-- ERC20 Transfer logs pivoted on the maker (the counterparty guaranteed to move both legs). A leg without
-- exactly one non-zero matching transfer is dropped (ambiguous / non-RFQ false positive), as is native-ETH.
-- Only the plain RFQ action is a 0x-native maker fill with no underlying venue, so only it belongs in
-- dex.trades; AMM/VIP-routed settler actions are represented by their underlying pool venues.

{#- RFQ action static-head layout: byte offset (after the 0xd92aadfb selector at position P, 1-indexed) of
    each 32-byte ABI word. The address is the word's last 20 bytes; its leading 12 bytes must be zero. -#}
{%- set off_maker_asset = 36 -%}
{%- set off_maker = 164 -%}
{%- set off_taker_token = 228 -%}
{%- set off_metatxn_taker = 177 -%}  {#- executeMetaTxn msgSender address, offset into the top-level input -#}
{%- set zero_word = '0x000000000000000000000000' -%}
{%- set zero_addr = '0x0000000000000000000000000000000000000000' -%}
{%- set native_tokens = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' -%}
{%- set erc20_transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' %}

WITH settler_rfq_txs AS (
    -- RFQ-bearing settler calls only: the staging persists rfq_input exactly when the calldata carries 0xd92aadfb.
    SELECT
        tx_hash,
        block_time,
        block_number,
        method_id,
        settler_address,
        rfq_input AS input
    FROM {{ ref('zeroex_v2_' ~ blockchain ~ '_settler_txs') }}
    WHERE rfq_input IS NOT NULL
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% else %}
        AND block_time >= DATE '{{ start_date }}'
    {% endif %}
),

-- Distinct partition key, reused by the transactions + logs joins below so each prunes by partition
-- (block_time + block_number + tx_hash), per zeroex_v2.sql's norm.
rfq_tx_keys AS (
    SELECT DISTINCT block_time, block_number, tx_hash FROM settler_rfq_txs
),

-- Root transaction sender. The settler trace's own caller is an intermediary — for plain `execute` it is
-- almost always the AllowanceHolder permit-forwarder (~96% of RFQ execute fills on ethereum), not the user
-- — so the taker must come from the tx-level `from`. Matches zeroex_settler_agg's receiver derivation.
txs AS (
    SELECT t.hash AS tx_hash, t."from" AS tx_from
    FROM {{ source(blockchain, 'transactions') }} t
    JOIN rfq_tx_keys k
        ON  k.block_time = t.block_time
        AND k.block_number = t.block_number
        AND k.tx_hash = t.hash
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('t.block_time') }}
    {% else %}
        WHERE t.block_time >= DATE '{{ start_date }}'
    {% endif %}
),

-- Every byte offset of the plain-RFQ selector in each call (a call can bundle multiple RFQ actions).
-- The position scan is chunked into <=10k-entry windows: Trino caps sequence() at 10,000 entries and
-- settler calldata can exceed 10kB (max observed ~24kB). Chunking is lossless (no position cap) and scans
-- the same total positions as a single sequence — the second UNNEST is sized to each chunk's actual span.
rfq_positions AS (
    SELECT t.*, pos.p
    FROM settler_rfq_txs t
    CROSS JOIN UNNEST(sequence(0, (varbinary_length(t.input) - 4) / 10000)) AS chunk(c)
    CROSS JOIN UNNEST(sequence(chunk.c * 10000 + 1, least((chunk.c + 1) * 10000, varbinary_length(t.input) - 3))) AS pos(p)
    WHERE varbinary_substring(t.input, pos.p, 4) = 0xd92aadfb
),

-- Slice the three static-head ABI words once (offset arithmetic lives here only).
rfq_words AS (
    SELECT
        rp.tx_hash, rp.block_time, rp.block_number, rp.settler_address, rp.p,
        CASE
            WHEN rp.method_id = 0xfd3ad6d4 THEN varbinary_substring(rp.input, {{ off_metatxn_taker }}, 20)  -- executeMetaTxn: msgSender (signer), relayer pays gas
            ELSE x.tx_from                                                                                  -- execute: tx-level sender, not the AllowanceHolder caller
        END AS taker,
        varbinary_substring(rp.input, rp.p + {{ off_maker_asset }}, 32) AS maker_asset_word,
        varbinary_substring(rp.input, rp.p + {{ off_maker }}, 32)       AS maker_word,
        varbinary_substring(rp.input, rp.p + {{ off_taker_token }}, 32) AS taker_token_word
    FROM rfq_positions rp
    JOIN txs x ON x.tx_hash = rp.tx_hash
),

-- Address = the word's last 20 bytes. Reject words whose leading 12 bytes are non-zero (selector
-- collisions inside other actions' payloads) and native-ETH / zero token legs.
rfq_actions AS (
    SELECT
        tx_hash, block_time, block_number, settler_address, p, taker,
        varbinary_substring(maker_asset_word, 13, 20) AS maker_asset,
        varbinary_substring(maker_word, 13, 20)       AS maker,
        varbinary_substring(taker_token_word, 13, 20) AS taker_token
    FROM rfq_words
    WHERE varbinary_substring(maker_asset_word, 1, 12) = {{ zero_word }}
      AND varbinary_substring(maker_word, 1, 12)       = {{ zero_word }}
      AND varbinary_substring(taker_token_word, 1, 12) = {{ zero_word }}
      AND varbinary_substring(maker_word, 13, 20) <> {{ zero_addr }}
      AND varbinary_substring(maker_asset_word, 13, 20) NOT IN {{ native_tokens }}
      AND varbinary_substring(taker_token_word, 13, 20) NOT IN {{ native_tokens }}
),

-- ERC20 Transfer logs for the RFQ txs, partition-aligned to the settler tx set
-- (block_time + block_number + tx_hash) so the logs scan prunes by partition, per zeroex_v2.sql's norm.
transfers AS (
    SELECT
        logs.block_number,
        logs.tx_hash,
        logs.index AS evt_index,
        logs.contract_address AS token,
        varbinary_substring(logs.topic1, 13, 20) AS transfer_from,
        varbinary_substring(logs.topic2, 13, 20) AS transfer_to,
        -- CASE-guard the conversion (Trino only evaluates the THEN branch when the WHEN holds): a non-standard
        -- Transfer-topic log with >32-byte data would otherwise overflow bytearray_to_uint256. The WHERE filter
        -- below is not sufficient on its own — Trino may evaluate this projection before applying it.
        CASE WHEN varbinary_length(logs.data) = 32 THEN bytearray_to_uint256(logs.data) END AS amount
    FROM {{ source(blockchain, 'logs') }} AS logs
    JOIN rfq_tx_keys k
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

-- Maker-pivot match in a single pass over the transfers, keyed on the full RFQ action identity
-- (maker, maker_asset, taker_token) so distinct fills sharing a byte offset across multiple settler
-- traces in one tx are not collapsed. Validity gate: EXACTLY ONE non-zero transfer per leg
-- (makerAsset out of the maker; takerToken in to the maker). evt_index = the maker-leg log index.
legs AS (
    SELECT
        a.tx_hash, a.p,
        a.block_time, a.block_number, a.settler_address, a.taker, a.maker_asset, a.taker_token,
        count(*)               FILTER (WHERE t.token = a.maker_asset AND t.transfer_from = a.maker) AS maker_n,
        count(*)               FILTER (WHERE t.token = a.taker_token AND t.transfer_to   = a.maker) AS taker_n,
        arbitrary(t.amount)    FILTER (WHERE t.token = a.maker_asset AND t.transfer_from = a.maker) AS maker_amount,
        arbitrary(t.evt_index) FILTER (WHERE t.token = a.maker_asset AND t.transfer_from = a.maker) AS maker_evt_index,
        arbitrary(t.amount)    FILTER (WHERE t.token = a.taker_token AND t.transfer_to   = a.maker) AS taker_amount
    FROM rfq_actions a
    JOIN transfers t
        ON  t.tx_hash = a.tx_hash
        AND t.block_number = a.block_number
        AND t.amount > UINT256 '0'
        AND (
              (t.token = a.maker_asset AND t.transfer_from = a.maker)
           OR (t.token = a.taker_token AND t.transfer_to   = a.maker)
        )
    GROUP BY a.tx_hash, a.p, a.block_time, a.block_number, a.settler_address, a.taker, a.maker_asset, a.taker_token, a.maker
),

-- A false-positive 0xd92aadfb recurrence inside an RFQ action's ABI body can decode the SAME action twice
-- (identical maker/asset/token) at two byte offsets, both matching the one maker-leg transfer -> same evt_index.
-- Keep one row per (tx_hash, evt_index): genuine distinct fills always have distinct maker-leg logs, so this
-- only drops the spurious re-decode (verified: the lone ethereum collision was an identical-tuple recurrence).
-- (row_number() in a subquery rather than QUALIFY, which has no precedent in this codebase.)
deduped AS (
    SELECT
        '{{ blockchain }}' AS blockchain,
        '{{ project }}' AS project,
        '{{ version }}' AS version,
        CAST(date_trunc('month', block_time) AS date) AS block_month,
        CAST(date_trunc('day', block_time) AS date)   AS block_date,
        block_time,
        block_number,
        maker_amount AS token_bought_amount_raw,   -- taker receives the maker's asset
        taker_amount AS token_sold_amount_raw,     -- taker gives the taker token
        maker_asset  AS token_bought_address,
        taker_token  AS token_sold_address,
        taker,
        CAST(NULL AS varbinary) AS maker,          -- PMM venue: settler-side maker not emitted (matches native/clipper)
        settler_address AS project_contract_address,
        tx_hash,
        maker_evt_index AS evt_index,
        row_number() OVER (PARTITION BY tx_hash, maker_evt_index ORDER BY p) AS rn
    FROM legs
    WHERE maker_n = 1
      AND taker_n = 1
      AND maker_asset <> taker_token
)
SELECT
    blockchain, project, version, block_month, block_date, block_time, block_number,
    token_bought_amount_raw, token_sold_amount_raw, token_bought_address, token_sold_address,
    taker, maker, project_contract_address, tx_hash, evt_index
FROM deduped
WHERE rn = 1

{% endmacro %}
