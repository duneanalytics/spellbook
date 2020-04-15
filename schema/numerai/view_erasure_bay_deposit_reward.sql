CREATE OR REPLACE VIEW erasure_v130.view_erasure_bay_deposit_reward AS
SELECT buyer AS requester,
       "amount" / 10^t.decimals AS reward,
       s.symbol,
       s.contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM
  (SELECT *,
          'DAI' AS symbol
   FROM erasure_v130."CountdownGriefingEscrow_evt_PaymentDeposited") s
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;

-- dummy 'token' column to skip explicit 10^18
