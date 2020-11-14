BEGIN;
DROP MATERIALIZED VIEW IF EXISTS onesplit.view_swaps;
CREATE MATERIALIZED VIEW onesplit.view_swaps AS
SELECT * FROM (
    SELECT
        tx."from" as tx_from,
        tx."to" as tx_to,
        from_token,
        to_token,
        from_amount,
        to_amount,
        tx_hash,
        tmp.call_trace_address as trace_address,
        tmp.block_time,
        tmp.contract_address,
        tmp.evt_index,
        from_amount * (
            CASE
                WHEN from_token IN (
                    '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', -- ETH
                    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH
                    '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb', -- sETH
                    '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04', -- aETH
                    '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315'  -- BETH
                ) THEN (
                    SELECT p.price/1e18
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'ETH'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN from_token IN (
                    '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d', -- renBTC
                    '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'  -- WBTC
                ) THEN (
                    SELECT p.price/1e8
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'BTC'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN from_token IN (
                    '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'  -- sBTC
                ) THEN (
                    SELECT p.price/1e18
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'BTC'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN from_token IN (
                    '\xdac17f958d2ee523a2206206994597c13d831ec7', -- USDT (6)
                    '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'  -- USDC (6)
                ) THEN (1/1e6)
                WHEN from_token IN (
                    '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', -- SAI
                    '\x6b175474e89094c44da98b954eedeac495271d0f', -- DAI
                    '\x0000000000085d4780B73119b644AE5ecd22b376', -- TUSD
                    '\x309627af60f0926daa6041b8279484312f2bf060', -- USDB
                    '\x8E870D67F660D95d5be530380D0eC0bd388289E1', -- PAX
                    '\x57ab1e02fee23774580c119740129eac7081e9d3', -- sUSD 1
                    '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'  -- sUSD 2
                ) THEN (1/1e18)
                ELSE (
                    SELECT p.price
                    FROM prices."usd" p
                    WHERE p.contract_address = from_token
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                ) / POWER(10, CASE t1.decimals IS NULL WHEN true THEN 18 else t1.decimals END)
            END
        ) as from_usd,
        to_amount * (
            CASE
                WHEN to_token IN (
                    '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', -- ETH
                    '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', -- WETH
                    '\x5e74c9036fb86bd7ecdcb084a0673efc32ea31cb', -- sETH
                    '\x3a3a65aab0dd2a17e3f1947ba16138cd37d08c04', -- aETH
                    '\xc0829421c1d260bd3cb3e0f06cfe2d52db2ce315'  -- BETH
                ) THEN (
                    SELECT p.price/1e18
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'ETH'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN to_token IN (
                    '\xeb4c2781e4eba804ce9a9803c67d0893436bb27d', -- renBTC
                    '\x2260fac5e5542a773aa44fbcfedf7c193bc2c599'  -- WBTC
                ) THEN (
                    SELECT p.price/1e8
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'BTC'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN to_token IN (
                    '\xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6'  -- sBTC
                ) THEN (
                    SELECT p.price/1e18
                    FROM prices.layer1_usd p
                    WHERE p.symbol = 'BTC'
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                )
                WHEN to_token IN (
                    '\xdac17f958d2ee523a2206206994597c13d831ec7', -- USDT (6)
                    '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'  -- USDC (6)
                ) THEN (1/1e6)
                WHEN to_token IN (
                    '\x89d24a6b4ccb1b6faa2625fe562bdd9a23260359', -- SAI
                    '\x6b175474e89094c44da98b954eedeac495271d0f', -- DAI
                    '\x0000000000085d4780B73119b644AE5ecd22b376', -- TUSD
                    '\x309627af60f0926daa6041b8279484312f2bf060', -- USDB
                    '\x8E870D67F660D95d5be530380D0eC0bd388289E1', -- PAX
                    '\x57ab1e02fee23774580c119740129eac7081e9d3', -- sUSD 1
                    '\x57Ab1ec28D129707052df4dF418D58a2D46d5f51'  -- sUSD 2
                ) THEN (1/1e18)
                ELSE (
                    SELECT p.price
                    FROM prices."usd" p
                    WHERE p.contract_address = to_token
                    AND p.minute = date_trunc('minute', tmp.block_time)
                    LIMIT 1
                ) / POWER(10, CASE t2.decimals IS NULL WHEN true THEN 18 else t2.decimals END)
            END
        ) as to_usd
    FROM (
        SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, call_tx_hash as tx_hash, call_trace_address, call_block_time as block_time, contract_address FROM onesplit."OneSplit_call_swap"     WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, call_tx_hash as tx_hash, call_trace_address, call_block_time as block_time, contract_address FROM onesplit."OneSplit_call_goodSwap" WHERE call_success
    ) tmp
    LEFT JOIN ethereum.transactions tx ON tx.hash = tx_hash
    LEFT JOIN erc20.tokens t1 ON t1.contract_address = from_token
    LEFT JOIN erc20.tokens t2 ON t2.contract_address = to_token
) tt;

CREATE UNIQUE INDEX IF NOT EXISTS onesplit_swaps_unique_idx ON onesplit.view_swaps (tx_hash, trace_address);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_1 ON onesplit.view_swaps (from_token) INCLUDE (from_amount, from_usd);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_2 ON onesplit.view_swaps (to_token) INCLUDE (to_amount, to_usd);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_3 ON onesplit.view_swaps (block_time);

INSERT INTO cron.job (schedule, command)
VALUES ('0,10,20,30,40,50 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY onesplit.view_swaps')
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;
