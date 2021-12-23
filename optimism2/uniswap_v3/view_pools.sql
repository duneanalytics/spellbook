CREATE TABLE IF NOT EXISTS uniswap_v3.view_pools(
token0 bytea NOT NULL,
token1 bytea NOT NULL,
fee integer,
pool bytea PRIMARY KEY
);

CREATE UNIQUE INDEX IF NOT EXISTS uniswapv3_view_pools_uniq_idx ON uniswap_v3.view_pools (token0, token1, fee);
CREATE INDEX IF NOT EXISTS uniswapv3_view_pools_token0_token1_idx ON uniswap_v3.view_pools (token0, token1);
CREATE INDEX IF NOT EXISTS uniswapv3_view_pools_pool_idx ON uniswap_v3.view_pools (pool);
