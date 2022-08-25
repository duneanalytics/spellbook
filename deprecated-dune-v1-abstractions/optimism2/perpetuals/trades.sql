CREATE SCHEMA IF NOT EXISTS perpetuals;

CREATE TABLE IF NOT EXISTS perpetuals.trades (
    block_time timestamptz NOT NULL,
    virtual_asset text,
    underlying_asset text,
    market text,
    market_address bytea,
    volume_usd numeric,
    fee_usd numeric,
    margin_usd numeric,
    trade text,
    project text NOT NULL,
    version text,
    trader bytea,
    volume_raw numeric,
    tx_hash bytea NOT NULL,
    tx_from bytea NOT NULL,
    tx_to bytea,
    evt_index integer NOT NULL,
    trade_id integer NOT NULL,
    UNIQUE (project, tx_hash, evt_index, trade_id)
);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS perpetuals_trades_proj_evt_index_uniq_idx ON perpetuals.trades (project, tx_hash, evt_index, trade_id);
CREATE INDEX IF NOT EXISTS perpetuals_trades_tx_from_idx ON perpetuals.trades (tx_from);
CREATE INDEX IF NOT EXISTS perpetuals_trades_tx_to_idx ON perpetuals.trades (tx_to);
CREATE INDEX IF NOT EXISTS perpetuals_trades_project_idx ON perpetuals.trades (project);
CREATE INDEX IF NOT EXISTS perpetuals_trades_block_time_idx ON perpetuals.trades USING BRIN (block_time);
CREATE INDEX IF NOT EXISTS perpetuals_trades_market_address_idx ON perpetuals.trades (market_address);
CREATE INDEX IF NOT EXISTS perpetuals_trades_block_time_project_idx ON perpetuals.trades (block_time, project);
CREATE INDEX CONCURRENTLY IF NOT EXISTS perpetuals_trades_volume_usd_idx ON perpetuals.trades (volume_usd);
