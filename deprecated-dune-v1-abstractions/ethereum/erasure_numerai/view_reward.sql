CREATE OR REPLACE VIEW erasure_numerai.view_reward AS
SELECT "currentStake" / 10^t.decimals AS nmr_stake,
       "amountToAdd" / 10^t.decimals AS nmr_reward,
       "currentStake" / 10^t.decimals * p.price AS usd_stake,
       "amountToAdd" / 10^t.decimals * p.price AS usd_reward,
       s.contract_address,
       call_tx_hash AS tx_hash,
       call_block_time AS block_time
FROM
  (SELECT *,
          'NMR' AS symbol
   FROM erasure_v110."SimpleGriefing_call_reward") s
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', s.call_block_time)
AND p.symbol = t.symbol
WHERE call_success = TRUE
;
