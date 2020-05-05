CREATE OR REPLACE VIEW bancor.view_change AS
SELECT "fromToken" AS source_token_address,
       t1.symbol AS source_token_symbol,
       "toToken" AS target_token_address,
       t2.symbol AS target_token_symbol,
       "trader" AS trader,
       "inputAmount" / 10^t1.decimals AS source_token_amount,
       "inputAmount" / 10^t1.decimals * p1.price AS source_usd_amount,
       "outputAmount" / 10^t2.decimals AS target_token_amount,
       "outputAmount" / 10^t2.decimals * p2.price AS target_usd_amount,
       s.contract_address,
       s.evt_tx_hash AS tx_hash,
       s.evt_block_time AS block_time
FROM bancor."BancorChange_evt_Change" s
LEFT JOIN erc20.tokens t1 ON s."fromToken" = t1.contract_address
LEFT JOIN prices.usd p1 ON p1.minute = date_trunc('minute', s.evt_block_time)
    AND p1.symbol = t1.symbol
LEFT JOIN erc20.tokens t2 ON s."toToken" = t2.contract_address
LEFT JOIN prices.usd p2 ON p2.minute = date_trunc('minute', s.evt_block_time)
    AND p2.symbol = t2.symbol
;
