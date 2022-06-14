CREATE SCHEMA IF NOT EXISTS llama;

CREATE TABLE IF NOT EXISTS llama.llama_treasury_addresses (
	protocol text,
	address bytea UNIQUE,
	version text,
	blockchain text,
	tags text
);

BEGIN;
DELETE FROM llama.llama_treasury_addresses *;


COPY llama.llama_treasury_addresses (protocol,address,version,blockchain,tags) FROM stdin;
Aave	\\xe3d9988f676457123c5fd01297605efdd0cba1ae	V1	Ethereum	\N
Aave	\\x464c71f6c2f760dda6093dcb91c24c39e5d6e18c	V2	Ethereum	\N
Aave	\\x25F2226B597E8F9514B3F68F00f494cF4f286491	'Ecosystem Reserve'	Ethereum	Treasury
Aave	\\x7734280A4337F37Fbf4651073Db7c28C80B339e9	MATIC	Polygon	\N
\.


COMMIT;

CREATE INDEX IF NOT EXISTS llama_treasury_addresses_address_idx ON llama.llama_treasury_addresses (address);
CREATE INDEX IF NOT EXISTS llama_treasury_addresses_address_protocol_idx ON llama.llama_treasury_addresses (address,protocol);
CREATE INDEX IF NOT EXISTS llama_treasury_addresses_address_protocol_version_idx ON llama.llama_treasury_addresses (address,protocol,version);