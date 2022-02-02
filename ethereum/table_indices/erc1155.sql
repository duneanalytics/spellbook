-- erc1155."ERC1155_evt_TransferBatch"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "erc1155_ERC1155_evt_TransferBatch_contract_address_idx" ON erc1155."ERC1155_evt_TransferBatch" USING btree (contract_address);

-- erc1155."ERC1155_evt_TransferSingle"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "erc1155_ERC1155_evt_TransferSingle_contract_address_id_idx" ON erc1155."ERC1155_evt_TransferSingle" USING btree (contract_address, id);
