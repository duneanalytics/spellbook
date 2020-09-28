CREATE OR REPLACE VIEW balancer.view_swaps AS
SELECT
  a.evt_block_time AS block_time,
  t."from" AS "trader",
  ta.symbol AS from_token_symbol,
  a."tokenAmountIn" / 10 ^ ta.decimals AS from_token_amount,
  (a."tokenAmountIn" / 10 ^ ta.decimals) * pa.price AS from_usd_amount,
  tb.symbol AS to_token_symbol,
  a."tokenAmountOut" / 10 ^ tb.decimals AS to_token_amount,
  (a."tokenAmountOut" / 10 ^ tb.decimals) * pb.price AS to_usd_amount,
  a.contract_address exchange_address,
  a.evt_tx_hash AS tx_hash
FROM
  balancer."BPool_evt_LOG_SWAP" a
  INNER JOIN ethereum.transactions t ON t.hash = a.evt_tx_hash
  LEFT JOIN erc20.tokens ta ON ta.contract_address = a."tokenIn"
  LEFT JOIN erc20.tokens tb ON tb.contract_address = a."tokenOut"
  LEFT JOIN prices.usd pa ON date_trunc('minute', a.evt_block_time) = pa.minute AND pa.contract_address = ta.contract_address
  LEFT JOIN prices.usd pb ON date_trunc('minute', a.evt_block_time) = pb.minute AND pb.contract_address = tb.contract_address
WHERE success = 'true'
