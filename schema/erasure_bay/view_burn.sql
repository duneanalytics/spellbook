CREATE OR REPLACE VIEW erasure_bay.view_burn AS
SELECT "tokenID" AS token_id,
       staker,
       amount / 10^t.decimals AS dai_amount,
       amount / 10^t.decimals * p.price AS usd_amount,
       t.symbol AS token_symbol,
       t.contract_address AS token_address,
       r.contract_address,
      "evt_tx_hash" AS tx_hash,
      "evt_block_time" AS block_time
FROM erasure_v130."CountdownGriefing_evt_StakeBurned" r
LEFT JOIN
  (SELECT *
  FROM (
         VALUES (2::int,
                 'DAI'::text)) AS tmp (token_id, symbol)) s ON s.token_id = r."tokenID"
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', r.evt_block_time)
      AND p.symbol = t.symbol
;
