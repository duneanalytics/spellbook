CREATE TABLE gnosis_protocol_v2.archived_trades AS
(
    SELECT * FROM gnosis_protocol_v2.view_trades
        WHERE block_time < '2022-01-01'
);

CREATE UNIQUE INDEX IF NOT EXISTS view_trades_id ON gnosis_protocol_v2.archived_trades (order_uid, tx_hash);
CREATE INDEX view_trades_idx_1 ON gnosis_protocol_v2.archived_trades (block_time);
CREATE INDEX view_trades_idx_2 ON gnosis_protocol_v2.archived_trades (sell_token_address);
CREATE INDEX view_trades_idx_3 ON gnosis_protocol_v2.archived_trades (buy_token_address);
CREATE INDEX view_trades_idx_4 ON gnosis_protocol_v2.archived_trades (trader);
CREATE INDEX view_trades_idx_5 ON gnosis_protocol_v2.archived_trades (app_data);
CREATE INDEX view_trades_idx_6 ON gnosis_protocol_v2.archived_trades (tx_hash);