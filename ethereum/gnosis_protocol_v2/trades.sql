CREATE TABLE gnosis_protocol_v2.trades
(
    app_data           text,
    atoms_bought       numeric     NOT NULL,
    atoms_sold         numeric     NOT NULL,
    block_time         timestamptz NOT NULL,
    buy_price          numeric,
    buy_token_address  bytea       NOT NULL,
    buy_token          text,
    buy_value_usd      numeric,
    fee                numeric,
    fee_atoms          numeric     NOT NULL,
    fee_usd            numeric,
    order_uid          bytea       NOT NULL,
    receiver           bytea,
    sell_price         numeric,
    sell_token_address bytea       NOT NULL,
    sell_token         text,
    sell_value_usd     numeric,
    trader             bytea       NOT NULL,
    trade_value_usd    numeric,
    tx_hash            bytea       NOT NULL,
    units_bought       numeric,
    units_sold         numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS trades_id ON gnosis_protocol_v2.trades (order_uid, tx_hash);
CREATE INDEX trades_idx_1 ON gnosis_protocol_v2.trades (block_time);
CREATE INDEX trades_idx_2 ON gnosis_protocol_v2.trades (sell_token_address);
CREATE INDEX trades_idx_3 ON gnosis_protocol_v2.trades (buy_token_address);
CREATE INDEX trades_idx_4 ON gnosis_protocol_v2.trades (trader);
CREATE INDEX trades_idx_5 ON gnosis_protocol_v2.trades (app_data);
CREATE INDEX trades_idx_6 ON gnosis_protocol_v2.trades (tx_hash);
