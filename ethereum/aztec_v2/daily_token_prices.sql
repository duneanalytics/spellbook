CREATE TABLE IF NOT EXISTS aztec_v2.daily_token_prices ( 
  token_address bytea
  , symbol varchar
  , date date
  , data_source varchar
  , avg_price_usd numeric
  , eth_price numeric
  , avg_price_eth numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_uniq_idx on aztec_v2.daily_token_prices (date, token_address);
CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_address_idx on aztec_v2.daily_token_prices (token_address);
CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_symbol_idx on aztec_v2.daily_token_prices (symbol);
CREATE UNIQUE INDEX IF NOT EXISTS aztec_v2_daily_token_prices_date_idx on aztec_v2.daily_token_prices (date);