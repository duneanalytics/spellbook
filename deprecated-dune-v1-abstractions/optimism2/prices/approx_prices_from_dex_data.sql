CREATE SCHEMA IF NOT EXISTS prices;

CREATE TABLE IF NOT EXISTS prices.approx_prices_from_dex_data(
    contract_address bytea NOT NULL,
    hour timestamptz NOT NULL,
    median_price numeric,
    sample_size integer,
    symbol text,
    decimals int4,
        PRIMARY KEY (contract_address,hour)
);

CREATE UNIQUE INDEX IF NOT EXISTS prices_approx_prices_from_dex_data_contract_addr_hour_uniq_idx ON prices.approx_prices_from_dex_data (contract_address, hour);
CREATE INDEX IF NOT EXISTS prices_approx_prices_from_dex_data_hour_idx ON prices.approx_prices_from_dex_data USING BRIN (hour);
CREATE INDEX IF NOT EXISTS prices_approx_prices_from_dex_data_contract_address_idx ON prices.approx_prices_from_dex_data (contract_address);
CREATE INDEX IF NOT EXISTS prices_approx_prices_from_dex_data_symbol_idx ON prices.approx_prices_from_dex_data (symbol);
CREATE INDEX IF NOT EXISTS prices_approx_prices_from_dex_data_median_price_idx ON prices.approx_prices_from_dex_data (median_price);
