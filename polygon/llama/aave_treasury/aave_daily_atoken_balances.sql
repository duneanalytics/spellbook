CREATE SCHEMA IF NOT EXISTS llama;

CREATE TABLE IF NOT EXISTS llama.aave_daily_atoken_balances (   
	day timestamptz,
	token_address bytea,
	daily_change numeric,
	starting_balance numeric,
	interest_rate_apr numeric,
	int_earned numeric,
	total_bal numeric,
		PRIMARY KEY (day, token_address)
);

CREATE INDEX IF NOT EXISTS llama_aave_daily_atoken_balances_token_day_idx ON llama.aave_daily_atoken_balances (token_address, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_atoken_balances_day_idx ON llama.aave_daily_atoken_balances (day);
