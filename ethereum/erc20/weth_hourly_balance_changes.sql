CREATE TABLE IF NOT EXISTS dex.hourly_balance_changes (
    hour timestamptz NOT NULL,
    wallet_address bytea NOT NULL,
    token_address bytea NOT NULL,
    amount_raw numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_hourly_balance_changes_hour_wallet_address_token_address_uniq_idx ON dex.hourly_balance_changes (hour, wallet_address, token_address);
CREATE INDEX IF NOT EXISTS dex_hourly_balance_changes_hour_idx ON dex.hourly_balance_changes USING BRIN (hour);
CREATE INDEX IF NOT EXISTS dex_hourly_balance_changes_token_address_idx ON dex.hourly_balance_changes (token_address);
CREATE INDEX IF NOT EXISTS dex_hourly_balance_changes_wallet_address_idx ON dex.hourly_balance_changes (wallet_address);
