CREATE OR REPLACE FUNCTION dex.insert_1inch(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO dex.trades (
        block_time,
        token_a_symbol,
        token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )
    SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / 10 ^ erc20a.decimals AS token_a_amount,
        token_b_amount_raw / 10 ^ erc20b.decimals AS token_b_amount,
        project,
        version,
        category,
        coalesce(trader_a, tx."from") as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / 10 ^ erc20a.decimals * pa.price,
            token_b_amount_raw / 10 ^ erc20b.decimals * pb.price
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" as tx_from,
        tx."to" as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        SELECT
            oi.block_time,
            '1inch' AS project,
            '1' AS version,
            'Aggregator' AS category,
            tx_from AS trader_a,
            NULL::bytea AS trader_b,
            to_amount AS token_a_amount_raw,
            from_amount AS token_b_amount_raw,
            GREATEST(from_usd, to_usd) AS usd_amount,
            to_token AS token_a_address,
            from_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            trace_address,
            evt_index
        FROM (
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, trace_address, evt_index AS evt_index FROM oneinch.swaps
            UNION ALL
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, trace_address, NULL::integer AS evt_index FROM onesplit.swaps
            WHERE tx_hash NOT IN (SELECT tx_hash FROM oneinch.swaps)
            UNION ALL
            SELECT to_token, from_token, to_amount, from_amount, tx_hash, tx_from, block_time, from_usd, to_usd, contract_address, NULL::integer[] AS trace_address, evt_index FROM oneproto.swaps
            WHERE tx_hash NOT IN (SELECT tx_hash FROM oneinch.swaps)
        ) oi

        UNION ALL

        -- 1inch Limit Orders (0x)
        SELECT
            evt_block_time as block_time,
            '1inch' AS project,
            '1' AS version,
            'Aggregator' AS category,
            "takerAddress" AS trader_a,
            "makerAddress" AS trader_b,
            "takerAssetFilledAmount" AS token_a_amount_raw,
            "makerAssetFilledAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            substring("takerAssetData" for 20 from 17) AS token_a_address,
            substring("makerAssetData" for 20 from 17) AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM zeroex_v2."Exchange2.1_evt_Fill"
        WHERE "feeRecipientAddress" IN ('\x910bf2d50fa5e014fd06666f456182d4ab7c8bd2', '\x68a17b587caf4f9329f0e372e3a78d23a46de6b5')

        UNION ALL

        -- 1inch Unoswap
        SELECT
            call_block_time as block_time,
            '1inch' AS project,
            '1' AS version,
            'Aggregator' AS category,
            COALESCE(tr.address, tx."from") AS trader_a,
            NULL::bytea AS trader_b,
            "output_returnAmount" AS token_a_amount_raw,
            "amount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            (CASE WHEN ll.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AND substring("_3"[ARRAY_LENGTH("_3", 1)] from 1 for 1) IN ('\xc0', '\x40') THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE ll.contract_address END) AS token_a_address,
            (CASE WHEN "srcToken" = '\x0000000000000000000000000000000000000000' THEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE "srcToken" END) AS token_b_address,
            us.contract_address AS exchange_contract_address,
            call_tx_hash,
            call_trace_address AS trace_address,
            NULL::integer AS evt_index
        FROM oneinch_v3."AggregationRouterV3_call_unoswap" us
        LEFT JOIN ethereum.transactions tx ON tx.hash = us.call_tx_hash
        LEFT JOIN ethereum.traces tr ON tr.tx_hash = us.call_tx_hash AND tr.trace_address = us.call_trace_address[:ARRAY_LENGTH(us.call_trace_address, 1)-1]
        LEFT JOIN ethereum.logs ll ON ll.tx_hash = us.call_tx_hash
            AND topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer(addresss,addresss,uint256)
            AND substring(topic2 from 13 for 20) = substring("_3"[ARRAY_LENGTH("_3", 1)] from 13 for 20)
        WHERE tx.success

    ) dexs
    INNER JOIN ethereum.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN (
        SELECT contract_address, decimals FROM erc20.tokens UNION

        SELECT '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 18 UNION -- ETH
        SELECT '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18 UNION -- WETH
        SELECT '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb', 18 UNION -- sETH
        SELECT '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04', 18 UNION -- aETH
        SELECT '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315', 18 UNION -- BETH

        SELECT '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d', 8 UNION -- renBTC
        SELECT '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 8 UNION -- WBTC
        SELECT '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6', 18 UNION -- sBTC

        SELECT '\xdac17f958d2ee523a2206206994597c13d831ec7', 6 UNION -- USDT
        SELECT '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6 UNION -- USDC
        SELECT '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', 18 UNION -- SAI
        SELECT '\x6b175474e89094c44da98b954eedeac495271d0f', 18 UNION -- DAI
        SELECT '\x0000000000085d4780B73119b644AE5ecd22b376', 18 UNION -- TUSD
        SELECT '\x309627af60f0926daa6041b8279484312f2bf060', 18 UNION -- USDB
        SELECT '\x8E870D67F660D95d5be530380D0eC0bd388289E1', 18 UNION -- PAX
        SELECT '\x57ab1e02fee23774580c119740129eac7081e9d3', 18 UNION -- sUSD
        SELECT '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51', 18 -- sUSD
    ) erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN (
        SELECT contract_address, decimals FROM erc20.tokens UNION

        SELECT '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 18 UNION -- ETH
        SELECT '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18 UNION -- WETH
        SELECT '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb', 18 UNION -- sETH
        SELECT '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04', 18 UNION -- aETH
        SELECT '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315', 18 UNION -- BETH

        SELECT '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d', 8 UNION -- renBTC
        SELECT '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 8 UNION -- WBTC
        SELECT '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6', 18 UNION -- sBTC

        SELECT '\xdac17f958d2ee523a2206206994597c13d831ec7', 6 UNION -- USDT
        SELECT '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6 UNION -- USDC
        SELECT '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', 18 UNION -- SAI
        SELECT '\x6b175474e89094c44da98b954eedeac495271d0f', 18 UNION -- DAI
        SELECT '\x0000000000085d4780B73119b644AE5ecd22b376', 18 UNION -- TUSD
        SELECT '\x309627af60f0926daa6041b8279484312f2bf060', 18 UNION -- USDB
        SELECT '\x8E870D67F660D95d5be530380D0eC0bd388289E1', 18 UNION -- PAX
        SELECT '\x57ab1e02fee23774580c119740129eac7081e9d3', 18 UNION -- sUSD
        SELECT '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51', 18 -- sUSD
    ) erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN (
        SELECT symbol, decimals, contract_address, price from prices.usd WHERE minute = date_trunc('minute', dexs.block_time)
        
        UNION

        SELECT eth_table.symbol, eth_table.decimals, eth_table.contract_address, price from prices.layer1_usd p
        LEFT JOIN (
            SELECT 'ETH' as symbol, 18 as decimals, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as contract_address union all
            SELECT 'WETH', 18, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' UNION ALL
            SELECT 'sETH', 18, '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb' UNION ALL
            SELECT 'aETH', 18, '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04' UNION ALL
            SELECT 'BETH', 18, '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315'
        ) eth_table ON true WHERE p.symbol = 'ETH' AND p.minute = date_trunc('minute', dexs.block_time)
        
        UNION 
        
        SELECT eth_table.symbol, eth_table.decimals, eth_table.contract_address, price from prices.layer1_usd p
        LEFT JOIN (
            SELECT 'renBTC' as symbol, 8 as decimals, '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d' as contract_address union all
            SELECT 'WBTC', 8, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' UNION ALL
            SELECT 'sBTC', 18, '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
        ) eth_table ON true WHERE p.symbol = 'BTC' AND p.minute = date_trunc('minute', dexs.block_time)

        UNION

        select 'USDT', 6, '\xdac17f958d2ee523a2206206994597c13d831ec7', 1 UNION
        select 'USDC', 6, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 1 UNION
        -- select 'SAI', 18, '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', 1 UNION
        select 'DAI', 18, '\x6b175474e89094c44da98b954eedeac495271d0f', 1 UNION
        select 'TUSD', 18, '\x0000000000085d4780B73119b644AE5ecd22b376', 1 UNION
        select 'USDB', 18, '\x309627af60f0926daa6041b8279484312f2bf060', 1 UNION
        select 'PAX', 18, '\x8E870D67F660D95d5be530380D0eC0bd388289E1', 1 UNION
        select 'sUSD', 18, '\x57ab1e02fee23774580c119740129eac7081e9d3', 1 UNION
        select 'sUSD', 18, '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51', 1
    ) pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN (
        SELECT symbol, decimals, contract_address, price from prices.usd WHERE minute = date_trunc('minute', dexs.block_time)
        
        UNION

        SELECT eth_table.symbol, eth_table.decimals, eth_table.contract_address, price from prices.layer1_usd p
        LEFT JOIN (
            SELECT 'ETH' as symbol, 18 as decimals, '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as contract_address union all
            SELECT 'WETH', 18, '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' UNION ALL
            SELECT 'sETH', 18, '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb' UNION ALL
            SELECT 'aETH', 18, '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04' UNION ALL
            SELECT 'BETH', 18, '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315'
        ) eth_table ON true WHERE p.symbol = 'ETH' AND p.minute = date_trunc('minute', dexs.block_time)
        
        UNION
        
        SELECT eth_table.symbol, eth_table.decimals, eth_table.contract_address, price from prices.layer1_usd p
        LEFT JOIN (
            SELECT 'renBTC' as symbol, 8 as decimals, '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d' as contract_address union all
            SELECT 'WBTC', 8, '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599' UNION ALL
            SELECT 'sBTC', 18, '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'
        ) eth_table ON true WHERE p.symbol = 'BTC' AND p.minute = date_trunc('minute', dexs.block_time)

        UNION

        select 'USDT', 6, '\xdac17f958d2ee523a2206206994597c13d831ec7', 1 UNION
        select 'USDC', 6, '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 1 UNION
        -- select 'SAI', 18, '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', 1 UNION
        select 'DAI', 18, '\x6b175474e89094c44da98b954eedeac495271d0f', 1 UNION
        select 'TUSD', 18, '\x0000000000085d4780B73119b644AE5ecd22b376', 1 UNION
        select 'USDB', 18, '\x309627af60f0926daa6041b8279484312f2bf060', 1 UNION
        select 'PAX', 18, '\x8E870D67F660D95d5be530380D0eC0bd388289E1', 1 UNION
        select 'sUSD', 18, '\x57ab1e02fee23774580c119740129eac7081e9d3', 1 UNION
        select 'sUSD', 18, '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51', 1
    ) pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.minute >= start_ts
        AND pb.minute < end_ts
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2017
SELECT dex.insert_1inch(
    '2017-01-01',
    '2018-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2017-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2018-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2017-01-01'
    AND block_time <= '2018-01-01'
    AND project = '1inch'
);

-- fill 2018
SELECT dex.insert_1inch(
    '2018-01-01',
    '2019-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2018-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2018-01-01'
    AND block_time <= '2019-01-01'
    AND project = '1inch'
);

-- fill 2019
SELECT dex.insert_1inch(
    '2019-01-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2019-01-01'
    AND block_time <= '2020-01-01'
    AND project = '1inch'
);

-- fill 2020
SELECT dex.insert_1inch(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = '1inch'
);

-- fill 2021
SELECT dex.insert_1inch(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT max(number) FROM ethereum.blocks)
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now()
    AND project = '1inch'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_1inch(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch'),
        (SELECT now()),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='1inch')),
        (SELECT MAX(number) FROM ethereum.blocks));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;