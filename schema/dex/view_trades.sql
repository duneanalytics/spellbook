BEGIN;
DROP MATERIALIZED VIEW IF EXISTS dex.view_trades;
CREATE MATERIALIZED VIEW dex.view_trades (
    block_time, 
    token_a_symbol,
    token_b_symbol,
    token_a_amount,
    token_b_amount,
    project,
    version,
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
) AS
SELECT
    block_time,
    erc20a.symbol AS token_a_symbol,
    erc20b.symbol AS token_b_symbol,
    token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
    token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
    project,
    version,
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
    -- Uniswap v1 TokenPurchase
    SELECT
        t.evt_block_time AS block_time,
        'Uniswap' AS project,
        '1' AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        tokens_bought token_a_amount_raw,
        eth_sold token_b_amount_raw,
        f.token token_a_address,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token_b_address, --Using WETH for easier joining with USD price table
        t.contract_address exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index
    FROM
        uniswap. "Exchange_evt_TokenPurchase" t
    INNER JOIN uniswap. "Factory_evt_NewExchange" f ON f.exchange = t.contract_address
    
    UNION
    
    -- Uniswap v1 EthPurchase
    SELECT
        t.evt_block_time AS block_time,
        'Uniswap' AS project,
        '1' AS version,
        buyer AS trader_a,
        NULL::bytea AS trader_b,
        eth_bought token_a_amount_raw,
        tokens_sold token_b_amount_raw,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea token_a_address, --Using WETH for easier joining with USD price table
        f.token AS token_b_address,
        t.contract_address exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index
    FROM
        uniswap. "Exchange_evt_EthPurchase" t
    INNER JOIN uniswap. "Factory_evt_NewExchange" f ON f.exchange = t.contract_address
    
    UNION

    -- Uniswap v2
    SELECT
        t.evt_block_time AS block_time,
        'Uniswap' AS project,
        '2' AS version,
        sender AS trader_a,
        NULL::bytea AS trader_b,
        CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_a_amount_raw,
        CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_b_amount_raw,
        CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
        CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
        t.contract_address exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index
    FROM
        uniswap_v2."Pair_evt_Swap" t
    INNER JOIN uniswap_v2."Factory_evt_PairCreated" f ON f.pair = t.contract_address
    
    UNION

    -- Kyber: trade from Token - ETH
    SELECT 
        evt_block_time AS block_time,
        'Kyber' AS project,
        NULL AS version,
        trader AS trader_a,
        NULL::bytea AS trader_b,
        CASE 
            WHEN src IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
            ELSE "srcAmount" 
        END AS token_a_amount_raw,
        "ethWeiValue" AS token_b_amount_raw,
        src AS token_a_address,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address,
        contract_address exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index AS evt_index
    FROM
        kyber."Network_evt_KyberTrade"
    WHERE src NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

    UNION

    -- Kyber: trade from ETH - Token
    SELECT
        evt_block_time AS block_time,
        'Kyber' AS project,
        NULL AS version,
        trader AS trader_a,
        NULL::bytea AS trader_b,
        "ethWeiValue" AS token_a_amount_raw,
        CASE 
            WHEN dest IN ('\x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN 0 -- ignore volume of token PT
            ELSE "dstAmount" 
        END AS token_b_amount_raw,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address,
        dest AS token_b_address,
        contract_address exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index AS evt_index
    FROM
        kyber."Network_evt_KyberTrade"
    WHERE dest NOT IN ('\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') 

    UNION

    -- Old Oasis (eth2dai) contract
    SELECT 
        t.evt_block_time AS block_time,
        'Oasis' AS project,
        '1' AS version,
        take.taker AS trader_a,
        take.maker AS trader_b,
        t.buy_amt AS token_a_amount_raw,
        t.pay_amt AS token_b_amount_raw,
        t.buy_gem AS token_a_address,
        t.pay_gem AS token_b_address,
        t.contract_address AS exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index
    FROM oasisdex."eth2dai_evt_LogTrade" t
    LEFT JOIN LATERAL (
        SELECT taker, maker
        FROM oasisdex."eth2dai_evt_LogTake" take
        WHERE t.evt_tx_hash = take.evt_tx_hash
        AND take.evt_index > t.evt_index
        ORDER BY take.evt_index ASC
        LIMIT 1
    ) take
    ON TRUE
    
    UNION 
    
    -- Oasis contract
    SELECT 
        t.evt_block_time AS block_time,
        'Oasis' AS project,
        '2' AS version,
        take.taker AS trader_a,
        take.maker AS trader_b,
        t.buy_amt AS token_a_amount_raw,
        t.pay_amt AS token_b_amount_raw,
        t.buy_gem AS token_a_address,
        t.pay_gem AS token_b_address,
        t.contract_address AS exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        t.evt_index
    FROM oasisdex."MatchingMarket_evt_LogTrade" t
    LEFT JOIN LATERAL (
        SELECT taker, maker
        FROM oasisdex."MatchingMarket_evt_LogTake" take
        WHERE t.evt_tx_hash = take.evt_tx_hash
        AND take.evt_index > t.evt_index
        ORDER BY take.evt_index ASC
        LIMIT 1
    ) take
    ON TRUE

    UNION

    -- 0x v2.1
    SELECT 
        evt_block_time AS block_time,
        '0x' AS project,
        '2.1' AS version,
        "takerAddress" AS trader_a,
        "makerAddress" AS trader_b,
        "takerAssetFilledAmount" AS token_a_amount_raw,
        "makerAssetFilledAmount" AS token_b_amount_raw,
        substring("takerAssetData" for 20 from 17) as token_a_address,
        substring("makerAssetData" for 20 from 17) as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
     FROM zeroex_v2."Exchange2.1_evt_Fill"

    UNION

    -- 0x v3
    SELECT 
        evt_block_time AS block_time,
        '0x' AS project,
        '3' AS version,
        "takerAddress" AS trader_a,
        "makerAddress" AS trader_b,
        "takerAssetFilledAmount" AS token_a_amount_rawr,
        "makerAssetFilledAmount" AS token_b_amount_raw,
        substring("takerAssetData" for 20 from 17) as token_a_address,
        substring("makerAssetData" for 20 from 17) as token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
     FROM zeroex_v3."Exchange_evt_Fill"

    UNION

    -- dYdX Solo Margin v2
    SELECT
        evt_block_time AS block_time,
        'dYdX' AS project,
        'Solo Margin v2' AS version,
        "takerAccountOwner" AS trader_a,
        "makerAccountOwner" AS trader_b,
        abs(("takerOutputUpdate"->'deltaWei'->'value')::numeric)/2 AS token_a_amount_raw, --"takerOutputNumber"
        abs(("takerInputUpdate"->'deltaWei'->'value')::numeric)/2 AS token_b_amount_raw, --"takerInputNumber"
        CASE
            WHEN "outputMarket" = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            WHEN "outputMarket" = 1 THEN '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'::bytea
            WHEN "outputMarket" = 2 THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
            WHEN "outputMarket" = 3 THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
        END AS token_a_address,
        CASE
            WHEN "inputMarket" = 0 THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
            WHEN "inputMarket" = 1 THEN '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359'::bytea
            WHEN "inputMarket" = 2 THEN '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea
            WHEN "inputMarket" = 3 THEN '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea
        END AS token_b_address,
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM dydx."SoloMargin_evt_LogTrade"
    
    UNION
    
    -- dYdX BTC-USDC Perpetual
    SELECT 
        evt_block_time AS block_time,
        'dYdX' AS project,
        'BTC-USDC Perpetual' AS version,
        maker AS trader_a,
        taker AS trader_b,
        "positionAmount" AS token_a_amount_raw,
        "marginAmount" AS token_b_amount_raw,
        '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' AS token_a_address,
        '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS token_b_address, 
        contract_address AS exchange_contract_address,
        evt_tx_hash AS tx_hash,
        NULL::integer[] AS trace_address,
        evt_index
    FROM
        dydx_perpetual."PerpetualV1_evt_LogTrade"
    WHERE "isBuy" = 'True'

    UNION

    -- Loopring v3.1
    (
        WITH trades AS (
            SELECT loopring.fn_process_trade_block(CAST(b."blockSize" AS INT), b._3, b.call_block_time) as trade,
                b."contract_address" as exchange_contract_address,
                b.call_tx_hash as tx_hash,
                b.call_trace_address as trace_address,
                NULL::bigint as evt_index
            FROM loopring."DEXBetaV1_call_commitBlock" b
            WHERE b."blockType" = '0'
        ), token_table AS (
            SELECT 0 AS "token_id", '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS token
            UNION
            SELECT "tokenId" AS "token_id", "token"
            FROM loopring."DEXBetaV1_evt_TokenRegistered" e
            WHERE token != '\x0000000000000000000000000000000000000000'
        )
        SELECT (t.trade).block_timestamp as block_time,
            'Loopring' AS project,
            '3.1' AS version,
            (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountA) as trader_a,
            (SELECT "owner" FROM loopring."DEXBetaV1_evt_AccountCreated" WHERE "id" = (t.trade).accountB) as trader_B,
            (t.trade).fillA::numeric as token_a_amount_raw,
            (t.trade).fillB::numeric as token_b_amount_raw,
            (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenA) as token_a_address,
            (SELECT "token" FROM token_table WHERE "token_id" = (t.trade).tokenB) as token_b_address,
            exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM trades t
    )

    UNION

    -- IDEX v1
    SELECT
        call_block_time AS block_time,
        'IDEX' AS project,
        '1' AS version,
        "tradeAddresses"[3] AS trader_a,
        "tradeAddresses"[4] AS trader_b,
        "tradeValues"[1] AS token_a_amount_raw,
        "tradeValues"[2] AS token_b_amount_raw,
        CASE WHEN "tradeAddresses"[1] = '\x0000000000000000000000000000000000000000' THEN
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        ELSE "tradeAddresses"[1]
        END AS token_a_address,
        CASE WHEN "tradeAddresses"[2] = '\x0000000000000000000000000000000000000000' THEN
            '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea
        ELSE "tradeAddresses"[2]
        END AS token_b_address,
        contract_address AS exchange_contract_address,
        call_tx_hash AS tx_hash,
        call_trace_address AS trace_address,
        NULL::integer AS evt_index
    FROM idex."IDEX1_call_trade"
    WHERE call_success

    UNION

    --Curve
    SELECT
        block_time,
        project,
        version,
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
    FROM curvefi.view_trades
) dexs
LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
;

CREATE UNIQUE INDEX dex_trades_unique ON dex.view_trades (tx_hash, trace_address, evt_index);
CREATE INDEX dex_trades_project ON dex.view_trades (project);
CREATE INDEX dex_trades_block_time ON dex.view_trades (block_time);
CREATE INDEX dex_trades_token_a ON dex.view_trades (token_a_address, token_a_amount);
CREATE INDEX dex_trades_token_b ON dex.view_trades (token_b_address, token_b_amount);


INSERT INTO cron.job (schedule, command) VALUES ('0,10,20,30,40,50 * * * *', 'REFRESH MATERIALIZED VIEW dex.view_trades') 
ON CONFLICT DO UPDATE SET schedule=EXCLUDED.schedule, command=EXCLUDED.command;
