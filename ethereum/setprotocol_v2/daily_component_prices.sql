
CREATE TABLE IF NOT EXISTS setprotocol_v2.daily_component_prices(
    component_address bytea NOT NULL
    , symbol text
    , date date NOT NULL
    , data_source text
    , avg_price_usd numeric
    , eth_price numeric
    , avg_price_eth numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_addr_date_uniq_idx on setprotocol_v2.daily_component_prices (component_address, date);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_addr_idx on setprotocol_v2.daily_component_prices (component_address);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_date_idx on setprotocol_v2.daily_component_prices (date);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_symbol_idx on setprotocol_v2.daily_component_prices (symbol);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_avg_price_usd_idx on setprotocol_v2.daily_component_prices (avg_price_usd);

-- use this version for testing on the user-enabled schema
/*
CREATE TABLE IF NOT EXISTS dune_user_generated.daily_component_prices(
    component_address bytea NOT NULL
    , symbol text
    , date date NOT NULL
    , data_source text
    , avg_price_usd numeric
    , eth_price numeric
    , avg_price_eth numeric
);

CREATE UNIQUE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_addr_date_uniq_idx on dune_user_generated.daily_component_prices (component_address, date);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_addr_idx on dune_user_generated.daily_component_prices (component_address);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_date_idx on dune_user_generated.daily_component_prices (date);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_symbol_idx on dune_user_generated.daily_component_prices (symbol);
CREATE INDEX IF NOT EXISTS setprotocol_v2_daily_component_prices_avg_price_usd_idx on dune_user_generated.daily_component_prices (avg_price_usd);
*/
-- don't forget to drop the test table before deploying because the index names will conflict
