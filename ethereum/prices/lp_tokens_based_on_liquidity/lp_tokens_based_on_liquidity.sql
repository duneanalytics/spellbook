CREATE TABLE IF NOT EXISTS prices.lp_tokens_based_on_liquidity (
  contract_address bytea NOT NULL,
  hour timestamptz NOT NULL,
  price numeric,
  symbol text,
  decimals int4,
  project text NOT NULL,
  version text
);

CREATE UNIQUE INDEX IF NOT EXISTS prices_lp_tokens_based_on_liquidity_contract_addr_hour_uniq_idx ON prices.lp_tokens_based_on_liquidity (contract_address, hour);
CREATE INDEX IF NOT EXISTS prices_lp_tokens_based_on_liquidity_hour_idx ON prices.lp_tokens_based_on_liquidity USING BRIN (hour);
CREATE INDEX IF NOT EXISTS prices_lp_tokens_based_on_liquidity_contract_address_idx ON prices.lp_tokens_based_on_liquidity (contract_address);
CREATE INDEX IF NOT EXISTS prices_lp_tokens_based_on_liquidity_symbol_idx ON prices.lp_tokens_based_on_liquidity (symbol);
CREATE INDEX IF NOT EXISTS prices_lp_tokens_based_on_liquidity_price_idx ON prices.lp_tokens_based_on_liquidity (price);