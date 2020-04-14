CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_increase_deposit AS
SELECT "user" ,
      amount / 10^t.decimals AS old_deposit,
      "newDeposit" / 10^t.decimals AS new_deposit,
      "tokenID" AS token_id,
      t.symbol,
      d."contract_address",
      "evt_tx_hash" AS tx_hash,
      "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_evt_DepositIncreased" d
LEFT JOIN
  (SELECT *
  FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = d."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;
