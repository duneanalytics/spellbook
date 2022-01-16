CREATE SCHEMA IF NOT EXISTS aave;

CREATE TABLE IF NOT EXISTS aave.aave_daily_interest_rates (   
	underlying_token bytea,
	token bytea,
	day timestamptz,
	interest_rate_raw numeric,
	interest_rate_ray numeric,
	interest_rate_apr numeric,
		PRIMARY KEY (underlying_token, token, day)
);

CREATE INDEX IF NOT EXISTS llama_aave_daily_interest_rates_underlying_token_day_idx ON aave.aave_daily_interest_rates (underlying_token, token, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_interest_rates_underlying_day_idx ON aave.aave_daily_interest_rates (underlying_token, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_interest_rates_token_day_idx ON aave.aave_daily_interest_rates (token, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_interest_rates_day_idx ON aave.aave_daily_interest_rates (day);
