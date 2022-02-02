CREATE SCHEMA IF NOT EXISTS erc20;

CREATE TABLE IF NOT EXISTS erc20.hourly_token_balances (
   
    hour timestamp,
    user_address AS bytea,
    token_address AS bytea, 
    symbol AS text,
    raw_value AS numeric,
    token_value AS numeric,
    median_price AS numeric,
    usd_value AS numeric,
    	UNIQUE(hour,user_address,token_address)
);

CREATE INDEX IF NOT EXISTS ovm2_hourly_token_balances_hour_idx ON ovm2.l1_gas_price_oracle_updates (hour);
CREATE INDEX IF NOT EXISTS ovm2_hourly_token_balances_hour_user_address_idx ON ovm2.l1_gas_price_oracle_updates (hour,user_address);
CREATE INDEX IF NOT EXISTS ovm2_hourly_token_balances_hour,user_address_token_address_idx ON ovm2.l1_gas_price_oracle_updates (hour,user_address,token_address);
