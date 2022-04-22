CREATE SCHEMA IF NOT EXISTS llama;

CREATE TABLE IF NOT EXISTS llama.llama_token_migrations (
	address	bytea,
	old_address	bytea,
	symbol	text,
	ratio	decimal,
		UNIQUE(address,old_address)
);

BEGIN;
DELETE FROM llama.llama_token_migrations *;

COPY llama.llama_token_migrations (address,old_address,symbol,ratio) FROM stdin;
\\xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f	\\xc011a72400e58ecd99ee497cf89e3775d4bd732f	SNX	1
\.


COMMIT;

CREATE INDEX IF NOT EXISTS llama_token_migrations_address_old_address_idx ON llama.llama_token_migrations(address,old_address);
