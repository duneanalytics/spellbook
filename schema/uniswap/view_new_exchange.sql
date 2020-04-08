CREATE OR REPLACE VIEW uniswap.view_new_exchange AS
SELECT 
symbol AS token_symbol,
token AS token_address,
exchange AS exchange_address,
e.contract_address AS factory_address,
evt_tx_hash AS tx_hash,
evt_block_time AS block_time
FROM uniswap."Factory_evt_NewExchange" e
LEFT JOIN erc20.tokens t
ON t.contract_address = e.token
;
