CREATE TABLE IF NOT EXISTS gnosis_protocol_v2.batches
(
    block_time      timestamptz NOT NULL,
    num_trades      int8        NOT NULL,
    dex_swaps       int8        NOT NULL,
    batch_value     numeric,
    gas_per_trade   numeric,
    solver_address  bytea       NOT NULL,
    solver_name     text,
    tx_hash         bytea       NOT NULL,
    gas_price_gwei  float8,
    gas_used        numeric,
    tx_cost_usd     numeric,
    fee_value       numeric,
    call_data_size  numeric,
    unwraps         int8,
    token_approvals int8
);

CREATE UNIQUE INDEX IF NOT EXISTS batches_id ON gnosis_protocol_v2.batches (tx_hash);
CREATE INDEX batches_idx_1 ON gnosis_protocol_v2.batches (block_time);
CREATE INDEX batches_idx_2 ON gnosis_protocol_v2.batches (solver_address);
CREATE INDEX batches_idx_3 ON gnosis_protocol_v2.batches (num_trades);