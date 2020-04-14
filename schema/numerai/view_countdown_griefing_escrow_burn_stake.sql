CREATE OR REPLACE VIEW numerai.view_countdown_griefing_escrow_burn_stake AS
SELECT staker AS fulfiller,
       amount / 10^t.decimals AS stake,
       "tokenID" AS token_id,
       b.contract_address,
      "evt_tx_hash" AS tx_hash,
      "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefingEscrow_evt_StakeBurned" b
LEFT JOIN
  (SELECT *
   FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = b."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;
