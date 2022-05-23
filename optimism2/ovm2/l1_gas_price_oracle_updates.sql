CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.l1_gas_price_oracle_updates (
    block_number numeric PRIMARY KEY,
    l1_gas_price numeric NOT NULL,
    block_time timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ovm2_l1_gas_prices_block_number_idx ON ovm2.l1_gas_price_oracle_updates (block_number);
CREATE INDEX IF NOT EXISTS ovm2_l1_gas_prices_block_time_idx ON ovm2.l1_gas_price_oracle_updates (block_time);
CREATE INDEX IF NOT EXISTS ovm2_l1_gas_prices_block_number_block_time_idx ON ovm2.l1_gas_price_oracle_updates (block_number, block_time);

CREATE UNIQUE INDEX IF NOT EXISTS ovm2_uniq_l1_gas_prices_block_number_idx ON ovm2.l1_gas_price_oracle_updates (block_number);
CREATE UNIQUE INDEX IF NOT EXISTS ovm2_uniq_l1_gas_prices_block_number_time_idx ON ovm2.l1_gas_price_oracle_updates (block_number, block_time);
