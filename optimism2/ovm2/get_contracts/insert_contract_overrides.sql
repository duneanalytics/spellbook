--Add contracts where we know their mapping, but their creator is not deterministic and the contracts are not verified.

CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.contract_overrides (

  contract_address  bytea NOT NULL,
  project_name text,
  contract_name text,
        PRIMARY KEY (contract_address)
);


BEGIN;
DELETE FROM ovm2.contract_overrides *;

COPY ovm2.contract_overrides (contract_address,project_name,contract_name) FROM stdin;
\\xc30141B657f4216252dc59Af2e7CdB9D8792e1B0	Socket	Socket Registry
\.
COMMIT;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS contract_overrides_list_addr_uniq_idx ON ovm2.contract_overrides (contract_address);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS contract_overrides_list_addr_proj_uniq_idx ON ovm2.contract_overrides (contract_address,project_name);
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS contract_overrides_list_addr_proj_name_uniq_idx ON ovm2.contract_overrides (contract_address,project_name, contract_name);
