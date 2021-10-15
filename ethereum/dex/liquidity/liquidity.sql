CREATE TABLE IF NOT EXISTS dex.liquidity (
    day timestamptz NOT NULL,
    token_symbol text,
    token_amount numeric,
    pool_name text,
    project text NOT NULL,
    version text,
    category text,
    token_amount_raw numeric,
    token_usd_amount numeric,
    token_address bytea NOT NULL,
    pool_address bytea NOT NULL,
    token_index text,
    token_pool_percentage numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_liquidity_day_pool_address_token_address_uniq_idx ON dex.liquidity (day, pool_address, token_address);
CREATE INDEX IF NOT EXISTS dex_liquidity_day_idx ON dex.liquidity USING BRIN (day);
CREATE INDEX IF NOT EXISTS dex_liquidity_token_address_idx ON dex.liquidity (token_address);
CREATE INDEX IF NOT EXISTS dex_liquidity_token_symbol_idx ON dex.liquidity (token_symbol);
CREATE INDEX IF NOT EXISTS dex_liquidity_pool_address_idx ON dex.liquidity (pool_address);
CREATE INDEX IF NOT EXISTS dex_liquidity_pool_name_idx ON dex.liquidity (pool_name);
CREATE INDEX IF NOT EXISTS dex_liquidity_day_project_version_idx ON dex.liquidity (day, project, version);
CREATE INDEX IF NOT EXISTS dex_liquidity_day_pool_address_project_version_pool_name_idx ON dex.liquidity (day, pool_address, project, version, pool_name);
