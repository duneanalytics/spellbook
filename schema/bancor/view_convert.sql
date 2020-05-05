CREATE OR REPLACE VIEW bancor.view_convert AS
SELECT "_fromToken" AS source_token_address,
       t1.symbol AS source_token_symbol,
       "_toToken" AS target_token_address,
       t2.symbol AS target_token_symbol,
       "_trader" AS trader,
       "_amount" / 10^t1.decimals AS source_token_amount,
       "_amount" / 10^t1.decimals * p1.price AS source_usd_amount,
       "_return" / 10^t2.decimals AS target_token_amount,
       "_return" / 10^t2.decimals * p2.price AS target_usd_amount,
       "_conversionFee" / 10^t2.decimals AS conversion_fee,
       s.contract_address,
       s.evt_tx_hash AS tx_hash,
       s.evt_block_time AS block_time
FROM bancor."BancorConverter_evt_Conversion" s
LEFT JOIN erc20.tokens t1 ON s."_fromToken" = t1.contract_address
LEFT JOIN prices.usd p1 ON p1.minute = date_trunc('minute', s.evt_block_time)
    AND p1.symbol = t1.symbol
LEFT JOIN erc20.tokens t2 ON s."_toToken" = t2.contract_address
LEFT JOIN prices.usd p2 ON p2.minute = date_trunc('minute', s.evt_block_time)
    AND p2.symbol = t2.symbol
;
