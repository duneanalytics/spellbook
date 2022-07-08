CREATE SCHEMA IF NOT EXISTS aave;

CREATE TABLE IF NOT EXISTS aave.aave_tokens (   
    token_address bytea PRIMARY KEY,
    decimals int4,
    symbol text,
    underlying_token_address bytea,
    underlying_token_symbol text,
    side text,
    token_name text,
    program_type text,
        UNIQUE (token_address, underlying_token_address, side)
);

CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_token_address_idx ON aave.aave_tokens (token_address);
CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_erc20_address_idx ON aave.aave_tokens (underlying_token_address);
CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_token_and_erc20_address_idx ON aave.aave_tokens (token_address,underlying_token_address);
CREATE INDEX IF NOT EXISTS llama_aave_treasury_aave_tokens_program_type_idx ON aave.aave_tokens (program_type);
