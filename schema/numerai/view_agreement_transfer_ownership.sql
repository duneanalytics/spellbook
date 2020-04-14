CREATE OR REPLACE VIEW numerai.view_agreement_transfer_ownership AS
SELECT "previousOwner" AS previous_owner,
       "newOwner" AS new_owner,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."Erasure_Agreements_evt_OwnershipTransferred"
;
