CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_update_operator AS
SELECT operator,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_evt_OperatorUpdated"
;