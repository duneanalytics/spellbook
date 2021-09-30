CREATE TABLE IF NOT EXISTS dex.daily_balance_changes (
    day timestamptz NOT NULL,
    pool_address bytea NOT NULL,
    token_address bytea NOT NULL,
    change_amount_raw numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_daily_balance_changes_day_pool_address_token_address_uniq_idx ON dex.daily_balance_changes (day, pool_address, token_address);
CREATE INDEX IF NOT EXISTS dex_daily_balance_changes_day_idx ON dex.daily_balance_changes USING BRIN (day);
CREATE INDEX IF NOT EXISTS dex_daily_balance_changes_token_address_idx ON dex.daily_balance_changes (token_address);
CREATE INDEX IF NOT EXISTS dex_daily_balance_changes_pool_address_idx ON dex.daily_balance_changes (pool_address);
