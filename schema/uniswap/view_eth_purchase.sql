CREATE OR REPLACE VIEW uniswap.view_eth_purchase AS
SELECT 
buyer AS buyer,
t.symbol AS token_symbol,
eth_bought/1e18 AS eth_bought,
(eth_bought/1e18 * p.price) AS usd_value_of_eth,
tokens_sold/10^decimals AS tokens_sold,
((tokens_sold/10^decimals) * tp.price) AS usd_value_of_token,
tokens_sold AS tokens_sold_raw,
e.token AS token_address,
a.contract_address AS exchange_address,
a.evt_tx_hash AS tx_hash,
a.evt_block_time AS block_time
FROM uniswap."Exchange_evt_EthPurchase" a
LEFT JOIN uniswap."Factory_evt_NewExchange" e ON e.exchange = a.contract_address
LEFT JOIN erc20.tokens t ON t.contract_address = e.token
LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', a.evt_block_time) AND p.symbol = 'ETH'
LEFT JOIN prices.usd tp ON tp.minute = date_trunc('minute', a.evt_block_time) AND tp.contract_address = e.token
;
