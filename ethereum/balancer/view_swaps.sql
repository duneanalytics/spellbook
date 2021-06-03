CREATE OR REPLACE VIEW balancer.view_swaps AS
SELECT
    block_time,
    token_a_symbol,
    token_b_symbol,
    token_a_amount,
    token_b_amount,
    trader_a,
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    usd_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address AS contract_address,
    tx_hash,
    tx_from,
    trace_address,
    evt_index
FROM dex.trades2
WHERE project = 'Balancer'
