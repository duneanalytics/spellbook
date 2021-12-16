DROP TABLE prices.hourly_bridge_token_price_ratios;

CREATE TABLE IF NOT EXISTS prices.hourly_bridge_token_price_ratios(
    hour timestamptz NOT NULL,
    lp_contract bytea,
    erc20_token bytea NOT NULL,
    bridge_token bytea NOT NULL,
    bridge_symbol text, 
    bridge_decimals numeric,
    price_ratio numeric, 
    sample_size numeric,
    PRIMARY KEY (hour, erc20_token, bridge_token),
    UNIQUE(bridge_token,hour)
);

CREATE UNIQUE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_hour_bridge_token__hour_uniq_idx ON prices.hourly_bridge_token_price_ratios (bridge_token, hour);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_hour_idx ON prices.hourly_bridge_token_price_ratios USING BRIN (hour);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_hour_erc20_token_bridge_token_idx ON prices.hourly_bridge_token_price_ratios (hour, erc20_token, bridge_token);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_hour_bridge_token_idx ON prices.hourly_bridge_token_price_ratios (hour, bridge_token);
CREATE INDEX IF NOT EXISTS prices_hourly_bridge_token_price_ratios_hour_erc20_token_idx ON prices.hourly_bridge_token_price_ratios (hour, erc20_token);
