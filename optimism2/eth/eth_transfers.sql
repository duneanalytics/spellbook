CREATE SCHEMA IF NOT EXISTS eth;

CREATE TABLE IF NOT EXISTS eth.eth_transfers (
	"from"	bytea,
	"to"	bytea,
	"contract_address"	bytea,
	value	numeric,
	value_decimal	numeric,
	tx_hash	bytea,
	trace_address	_int4,
	tx_block_time 	timestamptz,
	tx_block_number	int8,
	tx_method_id	bytea,
		PRIMARY KEY (tx_hash, trace_address)
	);


CREATE UNIQUE INDEX "eth_transfer_pkey" ON eth."eth_transfers" USING btree (tx_hash, trace_address);
CREATE INDEX "eth_transfer_contract_address_from_tx_block_time_val_idx" ON eth."eth_transfers" USING btree (contract_address, "from", tx_block_time) INCLUDE (value, value_decimal);
CREATE INDEX "eth_transfer_contract_address_to_tx_block_time_value_idx" ON eth."eth_transfers" USING btree (contract_address, "to", tx_block_time) INCLUDE (value, value_decimal);
CREATE INDEX "eth_transfer_tx_block_number_idx" ON eth."eth_transfers" USING brin (tx_block_number);
CREATE INDEX "eth_transfer_tx_block_time_idx" ON eth."eth_transfers" USING brin (tx_block_time);
CREATE INDEX "eth_transfer_from_contract_address_tx_block_time_val_idx" ON eth."eth_transfers" USING btree ("from", contract_address, tx_block_time) INCLUDE (value, value_decimal);
CREATE INDEX "eth_transfer_from_idx" ON eth."eth_transfers" USING btree ("from");
CREATE INDEX "eth_transfer_to_contract_address_tx_block_time_value_idx" ON eth."eth_transfers" USING btree ("to", contract_address, tx_block_time) INCLUDE (value, value_decimal);
CREATE INDEX "eth_transfer_incl_value_tx_idx_using_to_contract_addr_ev" ON eth."eth_transfers" USING btree ("to", contract_address, tx_block_time) INCLUDE (value, value_decimal, trace_address);
CREATE INDEX "eth_transfer_incl_value_tx_idx_using_from_contract_time_" ON eth."eth_transfers" USING btree ("from", contract_address, tx_block_time) INCLUDE (value, value_decimal, trace_address);
CREATE INDEX "eth_transfer_incl_value_tx_idx_using_contract_from_time_" ON eth."eth_transfers" USING btree (contract_address, "from", tx_block_time) INCLUDE (value, value_decimal, trace_address);
CREATE INDEX "eth_transfer_incl_value_tx_idx_using_contract_to_time_id" ON eth."eth_transfers" USING btree (contract_address, "to", tx_block_time) INCLUDE (value, value_decimal, trace_address);

CREATE INDEX IF NOT EXISTS "eth_transfer_tx_method_id" ON eth.eth_transfers (tx_method_id);
CREATE INDEX IF NOT EXISTS "eth_transfer_tx_method_id_block_time" ON eth.eth_transfers (tx_method_id,tx_block_time);
CREATE INDEX IF NOT EXISTS "eth_transfer_tx_method_id_to" ON eth.eth_transfers (tx_method_id, "to");
CREATE INDEX IF NOT EXISTS "eth_transfer_tx_method_id_to_from" ON eth.eth_transfers (tx_method_id, "to","from");
CREATE INDEX IF NOT EXISTS "eth_transfer_tx_method_id_to_block_time" ON eth.eth_transfers (tx_method_id, "to",tx_block_time);
