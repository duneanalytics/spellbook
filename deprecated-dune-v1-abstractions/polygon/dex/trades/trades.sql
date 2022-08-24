CREATE SCHEMA IF NOT EXISTS dex;

CREATE TABLE dex.trades (
    block_time timestamptz NOT NULL,
    token_a_symbol text,
    token_b_symbol text,
    token_a_amount numeric,
    token_b_amount numeric,
    project text NOT NULL,
    version text,
    category text,
    trader_a bytea,
    trader_b bytea,
    token_a_amount_raw numeric,
    token_b_amount_raw numeric,
    usd_amount numeric,
    token_a_address bytea,
    token_b_address bytea,
    exchange_contract_address bytea NOT NULL,
    tx_hash bytea NOT NULL,
    tx_from bytea NOT NULL,
    tx_to bytea,
    trace_address integer[],
    evt_index integer,
    trade_id integer
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dex_trades_proj_tr_addr_uniq_idx ON dex.trades (project, tx_hash, trace_address, trade_id);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dex_trades_proj_evt_index_uniq_idx ON dex.trades (project, tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS dex_trades_tx_from_idx ON dex.trades (tx_from);
CREATE INDEX IF NOT EXISTS dex_trades_tx_to_idx ON dex.trades (tx_to);
CREATE INDEX IF NOT EXISTS dex_trades_project_idx ON dex.trades (project);
CREATE INDEX IF NOT EXISTS dex_trades_block_time_idx ON dex.trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS dex_trades_token_a_idx ON dex.trades (token_a_address);
CREATE INDEX IF NOT EXISTS dex_trades_token_b_idx ON dex.trades (token_b_address);
CREATE INDEX IF NOT EXISTS dex_trades_block_time_project_idx ON dex.trades (block_time, project);
CREATE INDEX CONCURRENTLY IF NOT EXISTS dex_trades_usd_amount_idx ON dex.trades (usd_amount);
