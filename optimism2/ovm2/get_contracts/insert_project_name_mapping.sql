--Fix project display names where the dune decoding doesn't match up (i.e. Perp vs Perp v2, Lyra vs Lyra v1)

CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.project_name_mappings (
  dune_name text,
  mapped_name text,
        UNIQUE (dune_name, mapped_name)
);


BEGIN;
DELETE FROM ovm2.project_name_mappings *;

COPY ovm2.project_name_mappings (dune_name,mapped_name) FROM stdin;
lyra_v1	Lyra
Lyra V1	Lyra
aave_v3	Aave
perp_v2	Perpetual Protocol
synthetix_futures	Kwenta
zeroex	0x
uniswap_v3	Uniswap V3
Uniswap V3	Uniswap V3
OneInch	1inch
\.
COMMIT;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS dune_name_mapped_name_uniq_idx ON ovm2.project_name_mappings (dune_name);
