CREATE OR REPLACE VIEW uniswap_v1.view_new_exchange AS
SELECT
    t.symbol AS token_symbol,
    e.token AS token_address,
    e.exchange AS exchange_address,
    e.contract_address AS factory_address,
    e.evt_tx_hash AS tx_hash,
    e.evt_block_time AS block_time
FROM
    uniswap."Factory_evt_NewExchange" e
    LEFT JOIN erc20.tokens t ON t.contract_address = e.token;
