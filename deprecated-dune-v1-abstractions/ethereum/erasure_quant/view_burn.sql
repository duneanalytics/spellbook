CREATE OR REPLACE VIEW erasure_quant.view_burn AS
SELECT punisher,
       staker,
       punishment / 10^t.decimals AS nmr_punishment,
       cost / 10^t.decimals AS nmr_cost,
       punishment / 10^t.decimals * p.price AS usd_punishment,
       cost / 10^t.decimals * p.price AS usd_cost,
       convert_from("message", 'UTF8') AS message,
       s.contract_address,
       "evt_tx_hash" AS tx_hash,
       "evt_block_time" AS block_time
FROM
  (SELECT *,
          'NMR' AS symbol
   FROM erasure_v100."OneWayGriefing_evt_Griefed") s
LEFT JOIN erc20.tokens t ON s.symbol = t.symbol
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', s.evt_block_time)
AND p.symbol = t.symbol
;
