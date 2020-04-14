CREATE OR REPLACE VIEW numerai.view_countdown_griefing_grief AS
SELECT punisher,
       staker,
       punishment / 10^18 as punishment,
       cost / 10^18 as cost,
       convert_from("message", 'UTF8') AS message,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefing_evt_Griefed"
;

-- using 10^18 cause there is no direct link to DAI
