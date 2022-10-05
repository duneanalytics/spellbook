CREATE TABLE IF NOT EXISTS erc20.weth_hourly_balance_changes (
    hour timestamptz NOT NULL,
    wallet_address bytea NOT NULL,
    token_address bytea NOT NULL,
    amount_raw numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS erc20_weth_hourly_balance_ch_hour_wallet_token_uniq_idx ON erc20.weth_hourly_balance_changes (hour, wallet_address, token_address);
CREATE INDEX IF NOT EXISTS erc20_weth_hourly_balance_changes_hour_idx ON erc20.weth_hourly_balance_changes USING BRIN (hour);
CREATE INDEX IF NOT EXISTS erc20_weth_hourly_balance_changes_token_address_idx ON erc20.weth_hourly_balance_changes (token_address);
CREATE INDEX IF NOT EXISTS erc20_weth_hourly_balance_changes_wallet_address_idx ON erc20.weth_hourly_balance_changes (wallet_address);
