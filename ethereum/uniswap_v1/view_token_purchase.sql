CREATE OR REPLACE VIEW uniswap_v1.view_token_purchase AS
SELECT
    a.buyer AS buyer,
    t.symbol AS token_symbol,
    a.eth_sold / 1e18 AS eth_sold,
    (a.eth_sold / 1e18 * p.price) AS usd_value_of_eth,
    a.tokens_bought / 10 ^ t.decimals AS tokens_bought,
    ((a.tokens_bought / 10 ^ t.decimals) * tp.price) AS usd_value_of_token,
    a.tokens_bought AS tokens_bought_raw,
    e.token AS token_address,
    a.contract_address AS exchange_address,
    a.evt_tx_hash AS tx_hash,
    a.evt_block_time AS block_time
FROM
    uniswap."Exchange_evt_TokenPurchase" a
    LEFT JOIN uniswap."Factory_evt_NewExchange" e ON e.exchange = a.contract_address
    LEFT JOIN erc20.tokens t ON t.contract_address = e.token
    LEFT JOIN prices.layer1_usd p ON p.minute = date_trunc('minute', a.evt_block_time)
        AND p.symbol = 'ETH'
    LEFT JOIN prices.usd tp ON tp.minute = date_trunc('minute', a.evt_block_time)
        AND tp.contract_address = e.token;
