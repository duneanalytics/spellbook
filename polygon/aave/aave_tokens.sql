CREATE TABLE IF NOT EXISTS aave.aave_tokens (
	token_address bytea UNIQUE,
	symbol text,
	decimals int4,
  	underlying_token_address bytea,
  	underlying_token_symbol text,
  	underlying_token_decimals int4
);

BEGIN;
DELETE FROM aave.aave_tokens *;


COPY aave.aave_tokens (token_address, symbol, decimals,underlying_token_address,underlying_token_symbol,underlying_token_decimals) FROM stdin;
\\x1d2a0e5ec8e5bbdca5cb219e649b565d8e5c3360	amAAVE	18	\\xd6df932a45c0f255f85145f286ea0b292b21c90b	AAVE	18
\\x27f8d03b3a2196956ed754badc28d73be8830a6e	amDAI	18	\\x8f3cf7ad23cd3cadbd9735aff958023239c6a063	DAI	18
\\x1a13f4ca1d028320a707d99520abfefca3998b7f	amUSDC	6	\\x2791bca1f2de4661ed88a30c99a7a9449aa84174	USDC	6
\\x60d55f02a771d515e077c9c2403a1ef324885cec	amUSDT	6	\\xc2132d05d31c914a87c6611c10748aeb04b58e8f	USDT	6
\\x5c2ed810328349100a66b82b78a1791b101c9d61	amWBTC	8	\\x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6	WBTC	8
\\x28424507fefb6f7f8e9d3860f56504e4e5f5f390	amWETH	18	\\x7ceb23fd6bc0add59e62ac25578270cff1b9f619	WETH	18
\\x8df3aad3a84da6b69a4da8aec3ea40d9091b2ac4	amWMATIC	18	\\x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270	WMATIC	18
\.


COMMIT;

CREATE INDEX IF NOT EXISTS llama_aave_tokens_address_decimals_idx ON aave.aave_tokens USING btree (token_address) INCLUDE (decimals);
CREATE INDEX IF NOT EXISTS llama_aave_tokens_symbol_decimals_idx ON aave.aave_tokens USING btree (symbol) INCLUDE (decimals);
CREATE INDEX IF NOT EXISTS llama_aave_tokens_erc20address_decimals_idx ON aave.aave_tokens USING btree (underlying_token_address) INCLUDE (underlying_token_decimals);
CREATE INDEX IF NOT EXISTS llama_aave_tokens_erc20symbol_decimals_idx ON aave.aave_tokens USING btree (underlying_token_symbol) INCLUDE (underlying_token_decimals);
