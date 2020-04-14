CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_register_instance AS
SELECT "instance",
       creator,
       "callData" AS call_data,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_Factory_evt_InstanceCreated"
;
