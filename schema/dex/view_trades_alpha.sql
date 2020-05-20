CREATE OR REPLACE VIEW dex.view_trades_alpha AS

SELECT 
block_time,
erc20a.symbol AS token_a_symbol,
erc20b.symbol AS token_b_symbol,
token_a_amount_raw / 10^erc20a.decimals AS token_a_amount,
token_b_amount_raw / 10^erc20b.decimals AS token_b_amount,
project,
trader_a,
trader_b,
token_a_amount_raw,
token_b_amount_raw,
token_a_address,
token_b_address,
exchange_contract_address,
tx_hash,
trace_address,
evt_index 

FROM (

-- Uniswap v1
    (
    SELECT
    t.evt_block_time AS block_time,
    'Uniswap' AS "project",
    buyer AS trader_a,
    '\x'::BYTEA AS trader_b,
    tokens_bought token_a_amount_raw,
    eth_sold token_b_amount_raw,
    f.token token_a_address,
     '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA AS token_b_address, --Using WETH for easier joining with USD price table
    t.contract_address exchange_contract_address,
    t.evt_tx_hash AS tx_hash,
    '\x'::BYTEA AS trace_address,
    t.evt_index 
    FROM uniswap. "Exchange_evt_TokenPurchase" t
    INNER JOIN uniswap."Factory_evt_NewExchange" f ON f.exchange = t.contract_address
    )
        
        UNION ALL
        
    (
    SELECT 
    t.evt_block_time AS block_time,
    'Uniswap' AS "project",
    buyer AS trader_a,
    '\x'::BYTEA AS trader_b,
    eth_bought  token_a_amount_raw,
    tokens_sold token_b_amount_raw,
    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::BYTEA token_a_address, --Using WETH for easier joining with USD price table
    f.token AS token_b_address, 
    t.contract_address exchange_contract_address,
    t.evt_tx_hash AS tx_hash,
    '\x'::BYTEA AS trace_address,
    t.evt_index 
    FROM uniswap. "Exchange_evt_EthPurchase" t
    INNER JOIN uniswap."Factory_evt_NewExchange" f ON f.exchange = t.contract_address
)

UNION ALL

-- Kyber
(

    SELECT 
    t.evt_block_time AS block_time
    , 'Kyber' AS "project"
    , trader AS trader_a
    , '\x'::BYTEA AS trader_b
    , "dstAmount" AS token_a_amount_raw
    , "srcAmount" token_b_amount_raw
    , CASE WHEN t.dest = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE t.dest END AS token_a_address
    , CASE WHEN t.src = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE t.src END AS token_b_address
    , t.contract_address exchange_contract_address
    , t.evt_tx_hash AS tx_hash
    , '\x'::BYTEA AS trace_address
    , t.evt_index 
    FROM kyber."Network_evt_KyberTrade" t
)




) dexs
LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
;
