CREATE OR REPLACE VIEW numerai.view_remove_user AS
SELECT "user",
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."Erasure_Users_evt_UserRemoved"
;
