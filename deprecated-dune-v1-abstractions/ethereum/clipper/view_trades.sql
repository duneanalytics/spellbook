CREATE OR REPLACE VIEW clipper.view_trades (
    block_time,
    project,
    version,
    trader_a,
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    trace_address,
    evt_index
) AS SELECT
    evt_block_time AS block_time,
    'Clipper' AS project,
    NULL::text AS version,
    recipient AS trader_a,
    NULL::bytea AS trader_b,
    "inAmount" AS token_a_amount_raw,
    "outAmount" AS token_b_amount_raw,
    "inAsset" as token_a_address,
    "outAsset" as token_b_address,
    contract_address AS exchange_contract_address,
    evt_tx_hash AS tx_hash,
    NULL::integer[] AS trace_address,
    evt_index
FROM clipper."ClipperExchangeInterface_evt_Swapped"