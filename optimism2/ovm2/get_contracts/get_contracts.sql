CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.get_contracts (
	contract_address bytea UNIQUE,
	contract_project text,
	erc20_symbol bytea, 
	contract_name text,
	creator_address bytea, 
	created_time timestamp,
	contract_creator_if_factory bytea
);

CREATE INDEX IF NOT EXISTS ovm2_get_contracts_contract_address_idx ON ovm2.get_contracts (contract_address);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_created_time_idx ON ovm2.get_contracts (created_time);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_contract_project_idx ON ovm2.get_contracts (contract_project);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_contract_address_contract_project_idx ON ovm2.get_contracts (contract_address,contract_project);
