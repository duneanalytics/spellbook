CREATE OR REPLACE VIEW numerai.view_countdown_griefing_set_length AS
SELECT "length",
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefing_evt_LengthSet"
;
