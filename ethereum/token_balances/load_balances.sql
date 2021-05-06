CREATE TABLE IF NOT EXISTS token_balances(
   timestamp timestamptz,
   wallet_address bytea,
   token_address bytea,
   token_symbol text,
   amount_raw NUMERIC,
   amount NUMERIC,
   PRIMARY KEY(timestamp, wallet_address, token_address)
);

CREATE INDEX IF NOT EXISTS token_balances_wallet_address_token_address_idx ON token_balances USING btree (wallet_address, token_address);
CREATE INDEX IF NOT EXISTS token_balances_wallet_address_token_address_timestamp_idx ON token_balances USING btree (wallet_address, token_address, timestamp) include (amount);
CREATE INDEX IF NOT EXISTS token_balances_token_timestamp_idx ON token_balances USING btree (timestamp, token_address);

