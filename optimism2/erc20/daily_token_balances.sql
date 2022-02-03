CREATE SCHEMA IF NOT EXISTS erc20;

CREATE TABLE IF NOT EXISTS erc20.daily_token_balances (
   
    day timestamp,
    user_address bytea,
    token_address bytea, 
    symbol text,
    raw_value numeric,
    token_value numeric,
    median_price numeric,
    usd_value numeric,
    	UNIQUE(day,user_address,token_address)
);

CREATE INDEX IF NOT EXISTS ovm2_daily_token_balances_day_idx ON erc20.daily_token_balances (day);
CREATE INDEX IF NOT EXISTS ovm2_daily_token_balances_day_user_address_idx ON erc20.daily_token_balances (day,user_address);
CREATE INDEX IF NOT EXISTS ovm2_daily_token_balances_day_user_address_token_address_idx ON erc20.daily_token_balances (day,user_address,token_address);
