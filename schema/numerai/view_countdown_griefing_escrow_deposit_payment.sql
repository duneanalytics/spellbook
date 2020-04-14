CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_deposit_payment AS
SELECT buyer AS requester,
       "amount" / 10^18 AS reward,
       contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_evt_PaymentDeposited"
;

-- using 10^18 cause there is no direct link to DAI
