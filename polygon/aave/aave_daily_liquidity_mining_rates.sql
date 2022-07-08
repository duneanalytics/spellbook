CREATE SCHEMA IF NOT EXISTS aave;

CREATE TABLE IF NOT EXISTS aave.aave_daily_liquidity_mining_rates ( 

day timestamptz,
token_address bytea,
lm_reward_apr_yr numeric,
lm_reward_apr_daily numeric,
lm_token_yr_raw numeric,
lm_token_daily_raw numeric,
aave_decimals numeric,
	PRIMARY KEY (day, token_address)
);

CREATE INDEX IF NOT EXISTS llama_aave_daily_liquidity_mining_rates_token_day_idx ON aave.aave_daily_liquidity_mining_rates (token_address, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_liquidity_mining_rates_day_idx ON aave.aave_daily_liquidity_mining_rates (day);
