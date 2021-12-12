CREATE TABLE IF NOT EXISTS prices.hourly_bridge_token_price_ratios(
    hour timestamptz NOT NULL,
    lp_contract bytea NOT NULL,
    erc20_token bytea NOT NULL,
    bridge_token bytea NOT NULL,
    bridge_symbol text, 
    bridge_decimals numeric,
    price_ratio numeric, 
    num_samples numeric,
    	PRIMARY KEY (dt, LP_contract, erc20_token, bridge_token)
);

CREATE UNIQUE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_dt_idx ON prices.hourly_bridge_token_price_ratios USING BRIN (dt);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_dt_erc20_token_bridge_token_idx ON prices.hourly_bridge_token_price_ratios (dt, erc20_token, bridge_token);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_dt_bridge_token_idx ON prices.hourly_bridge_token_price_ratios (dt, bridge_token);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_dt_erc20_token_idx ON prices.hourly_bridge_token_price_ratios (dt, erc20_token);
