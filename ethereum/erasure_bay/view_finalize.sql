CREATE OR REPLACE VIEW erasure_bay.view_finalize AS
SELECT agreement,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_evt_Finalized"
;
