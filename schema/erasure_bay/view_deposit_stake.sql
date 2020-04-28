CREATE OR REPLACE VIEW erasure_bay.view_deposit_stake AS
SELECT seller AS fulfiller,
       "amount" / 10^t.decimals AS dai_stake,
       "amount" / 10^t.decimals * p.price AS usd_stake,
       s.symbol AS token_symbol,
       t.contract_address AS token_address,
       s.contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM
  (SELECT *,
          'DAI' AS symbol
   FROM erasure_v130."CountdownGriefingEscrow_evt_StakeDeposited") s
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', s.evt_block_time)
      AND p.symbol = t.symbol
;

-- dummy 'symbol' column to skip explicit 10^18
