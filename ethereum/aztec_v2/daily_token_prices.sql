DROP TABLE aztec_v2.daily_token_prices cascade
-- DROP TABLE dune_user_generated.aztec_v2_daily_token_prices cascade
;

CREATE TABLE IF NOT EXISTS aztec_v2.daily_token_prices ( 
-- CREATE TABLE IF NOT EXISTS dune_user_generated.aztec_v2_daily_token_prices ( 
  token_address bytea
  , symbol varchar
  , date date
  , data_source varchar
  , avg_price_usd numeric
  , eth_price numeric
  , avg_price_eth numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_uniq_idx on aztec_v2.daily_token_prices (date, token_address);
CREATE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_address_idx on aztec_v2.daily_token_prices (token_address);
CREATE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_symbol_idx on aztec_v2.daily_token_prices (symbol);
CREATE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_date_idx on aztec_v2.daily_token_prices (date);

/*
CREATE UNIQUE INDEX IF NOT EXISTS dug_aztec_v2_daily_token_prices_uniq_idx on dune_user_generated.aztec_v2_daily_token_prices (date, token_address);
CREATE INDEX IF NOT EXISTS dug_aztec_v2_daily_token_prices_address_idx on dune_user_generated.aztec_v2_daily_token_prices (token_address);
CREATE INDEX IF NOT EXISTS dug_aztec_v2_daily_token_prices_symbol_idx on dune_user_generated.aztec_v2_daily_token_prices (symbol);
CREATE INDEX IF NOT EXISTS dug_aztec_v2_daily_token_prices_date_idx on dune_user_generated.aztec_v2_daily_token_prices (date);
*/