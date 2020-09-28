CREATE OR REPLACE VIEW balancer.view_add_liquidity AS
SELECT
  a.caller AS liquidity_provider,
  a.contract_address AS exchange_address,
  a."tokenAmountIn" / 10 ^ t.decimals AS token_amount,
  (a."tokenAmountIn" / 10 ^ t.decimals) * p.price AS usd_amount,
  t.symbol AS token_symbol,
  a.evt_tx_hash AS tx_hash,
  a.evt_block_time AS block_time
FROM
  balancer."BPool_evt_LOG_JOIN" a
  LEFT JOIN erc20.tokens t ON t.contract_address = a."tokenIn"
  LEFT JOIN prices.usd p ON date_trunc('minute', a.evt_block_time) = p.minute AND p.contract_address = t.contract_address
  