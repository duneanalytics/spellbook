CREATE OR REPLACE VIEW erasure_numerai.view_stake AS
SELECT staker,
       funder,
       amount / 10^t.decimals AS nmr_stake,
       "newStake" / 10^t.decimals AS nmr_new_stake,
       amount / 10^t.decimals * p.price AS usd_stake,
       "newStake" / 10^t.decimals * p.price AS usd_new_stake,
       s.contract_address,
       evt_tx_hash AS tx_hash,
       evt_block_time AS block_time
FROM
  (SELECT *,
          'NMR' AS symbol
  FROM erasure_v110."SimpleGriefing_evt_StakeAdded") s
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', s.evt_block_time)
AND p.symbol = t.symbol
;
