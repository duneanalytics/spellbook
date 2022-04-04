CREATE SCHEMA IF NOT EXISTS eth;

CREATE TABLE IF NOT EXISTS eth.eth_transfers (
"from"	bytea,
"to"	bytea,
value	decimal,
trace_tx_hash	bytea,
trace_index	int8,
trace_block_time 	timestamptz,
trace_block_number	int8,
	PRIMARY KEY (trace_tx_hash, trace_index)
);


CREATE UNIQUE INDEX "eth_transfer_pkey" ON eth."eth_transfer" USING btree (trace_tx_hash, trace_index)
CREATE INDEX "eth_transfer_contract_address_from_trace_block_time_val_idx" ON eth."eth_transfer" USING btree (contract_address, "from", trace_block_time) INCLUDE (value)
CREATE INDEX "eth_transfer_contract_address_to_trace_block_time_value_idx" ON eth."eth_transfer" USING btree (contract_address, "to", trace_block_time) INCLUDE (value)
CREATE INDEX "eth_transfer_trace_block_number_idx" ON eth."eth_transfer" USING brin (trace_block_number)
CREATE INDEX "eth_transfer_trace_block_time_idx" ON eth."eth_transfer" USING brin (trace_block_time)
CREATE INDEX "eth_transfer_from_contract_address_trace_block_time_val_idx" ON eth."eth_transfer" USING btree ("from", contract_address, trace_block_time) INCLUDE (value)
CREATE INDEX "eth_transfer_from_idx" ON eth."eth_transfer" USING btree ("from")
CREATE INDEX "eth_transfer_to_contract_address_trace_block_time_value_idx" ON eth."eth_transfer" USING btree ("to", contract_address, trace_block_time) INCLUDE (value)
CREATE INDEX "eth_transfer_incl_value_trace_idx_using_to_contract_addr_ev" ON eth."eth_transfer" USING btree ("to", contract_address, trace_block_time) INCLUDE (value, trace_index)
CREATE INDEX "eth_transfer_incl_value_trace_idx_using_from_contract_time_" ON eth."eth_transfer" USING btree ("from", contract_address, trace_block_time) INCLUDE (value, trace_index)
CREATE INDEX "eth_transfer_incl_value_trace_idx_using_contract_from_time_" ON eth."eth_transfer" USING btree (contract_address, "from", trace_block_time) INCLUDE (value, trace_index)
CREATE INDEX "eth_transfer_incl_value_trace_idx_using_contract_to_time_id" ON eth."eth_transfer" USING btree (contract_address, "to", trace_block_time) INCLUDE (value, trace_index)
