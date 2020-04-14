CREATE OR REPLACE VIEW numerai.view_countdown_griefing_set_ratio AS
SELECT staker,
       "tokenID" AS token_id,
       ratio / 10^t.decimals AS ratio,
       "ratioType" AS ratio_type,
       r.contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefing_evt_RatioSet" r
LEFT JOIN
  (SELECT *
   FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = r."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
;
