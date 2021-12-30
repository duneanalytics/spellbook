CREATE SCHEMA IF NOT EXISTS llama;

CREATE TABLE IF NOT EXISTS llama.aave_tokens (   
    token_address bytea PRIMARY KEY,
    decimals numeric,
    symbol text,
    erc20_address bytea,
    erc20_symbol text,
    side text
);

CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_token_address_idx ON llama.aave_tokens (token_address);
CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_erc20_address_idx ON llama.aave_tokens (erc20_address);
CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_token_and_erc20_address_idx ON llama.aave_tokens (token_address,erc20_address);
