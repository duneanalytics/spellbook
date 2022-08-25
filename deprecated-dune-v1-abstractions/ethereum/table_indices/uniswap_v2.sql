-- uniswap_v2."Factory_evt_PairCreated"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v2_Factory_evt_PairCreated_token0_idx" ON uniswap_v2."Factory_evt_PairCreated" ("token0");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v2_Factory_evt_PairCreated_token1_idx" ON uniswap_v2."Factory_evt_PairCreated" ("token1");
