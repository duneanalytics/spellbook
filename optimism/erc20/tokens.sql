CREATE TABLE IF NOT EXISTS erc20.tokens (
	contract_address bytea UNIQUE,
	symbol text,
	decimals integer
);

BEGIN;
DELETE FROM erc20.tokens *;


COPY erc20.tokens (contract_address, symbol, decimals) FROM stdin;
\\x4200000000000000000000000000000000000006	ETH	18
\\xda10009cbd5d07dd0cecc66161fc93d7c9000da1	DAI	18
\\x68f180fcce6836688e9084f035309e29bf0a2095	WBTC	8
\\x94b008aa00579c1307b0ef2c499ad98a8ce58e58	USDT	6
\\x8700daec35af8ff88c16bdf0418774cb3d7599b4	SNX	18
\\x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9	sUSD	18
\\xe405de8f52ba7559f9df3c368500b6e6ae6cee49	sETH	18
\\xc5db22719a06418028a40a9b5e9a7c02959d0d08	sLINK	18
\.


COMMIT;

CREATE INDEX IF NOT EXISTS tokens_contract_address_decimals_idx ON erc20.tokens USING btree (contract_address) INCLUDE (decimals);
CREATE INDEX IF NOT EXISTS tokens_symbol_decimals_idx ON erc20.tokens USING btree (symbol) INCLUDE (decimals);
