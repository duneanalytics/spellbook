CREATE SCHEMA IF NOT EXISTS ovm2;

CREATE TABLE IF NOT EXISTS ovm2.get_contracts (
	contract_address bytea UNIQUE,
	contract_project text,
	token_symbol text, 
	contract_name text,
	creator_address bytea, 
	created_time timestamptz,
	contract_creator_if_factory bytea,
	is_self_destruct boolean
);

CREATE INDEX IF NOT EXISTS ovm2_get_contracts_address_idx ON ovm2.get_contracts (contract_address);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_created_idx ON ovm2.get_contracts (created_time);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_project_idx ON ovm2.get_contracts (contract_project);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_address_project_idx ON ovm2.get_contracts (contract_address,contract_project);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_address_project_time_idx ON ovm2.get_contracts (contract_address,contract_project,created_time);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_address_project_time_destruct_idx ON ovm2.get_contracts (contract_address,contract_project,created_time,is_self_destruct);
CREATE INDEX IF NOT EXISTS ovm2_get_contracts_destruct_idx ON ovm2.get_contracts (is_self_destruct);
