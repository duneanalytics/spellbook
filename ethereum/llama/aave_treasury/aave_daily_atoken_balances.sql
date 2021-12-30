CREATE SCHEMA IF NOT EXISTS llama;

CREATE TABLE IF NOT EXISTS llama.aave_daily_atoken_balances (   
	day,
	token_address,
	daily_change,
	starting_balance,
	interest_rate_apr,
	int_earned,
	total_bal
		PRIMARY KEY (day, token_address)
);

CREATE INDEX IF NOT EXISTS llama_aave_daily_atoken_balances_token_day_idx ON llama.aave_daily_atoken_balances (token_address, day);
CREATE INDEX IF NOT EXISTS llama_aave_daily_atoken_balances_day_idx ON llama.aave_daily_atoken_balances (day);
