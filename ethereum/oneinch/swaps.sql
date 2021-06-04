CREATE TABLE IF NOT EXISTS oneinch.swaps2 (
    tx_from bytea,
    tx_to bytea,
    from_token bytea,
    to_token bytea,
    from_amount numeric,
    to_amount numeric,
    from_usd numeric,
    to_usd numeric,
    tx_hash bytea,
    block_time timestamptz NOT NULL,
    contract_address bytea,
    evt_index integer,
    trace_address integer[]
);

CREATE OR REPLACE FUNCTION oneinch.insert_swap(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH swap AS (
    SELECT tx."from" AS tx_from,
        tx."to" AS tx_to,
        from_token,
        to_token,
        from_amount,
        to_amount,
        tx_hash,
        tmp.evt_index,
        tmp.block_time,
        tmp.contract_address,
        tmp.trace_address,
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
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v1_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v2_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v3_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v4_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v5_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v6_call_aggregate" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."exchange_v7_call_swap" WHERE call_success UNION ALL
        SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address as trace_address, NULL::integer as evt_index, contract_address FROM oneinch."OneInchExchange_call_swap" WHERE call_success UNION ALL
        SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address FROM oneinch_v2."OneInchExchange_evt_Swapped" UNION ALL
        SELECT "srcToken" as from_token, "dstToken" as to_token, "spentAmount" as from_amount, "returnAmount" as to_amount, evt_tx_hash as tx_hash, evt_block_time as block_time, NULL::integer[] as call_trace_address, evt_index, contract_address FROM oneinch_v3."AggregationRouterV3_evt_Swapped"
    ) tmp
    INNER JOIN ethereum.transactions tx ON tx.hash = tx_hash
    LEFT JOIN erc20.tokens t1 ON t1.contract_address = from_token
    LEFT JOIN erc20.tokens t2 ON t2.contract_address = to_token
    WHERE tmp.block_time >= start_ts
    AND tmp.block_time < end_ts
),
rows AS (
    INSERT INTO oneinch.swaps2 (
        tx_from,
        tx_to,
        from_token,
        to_token,
        from_amount,
        to_amount,
        from_usd,
        to_usd,
        tx_hash,
        block_time,
        contract_address,
        evt_index,
        trace_address
    )
    SELECT
        tx_from,
        tx_to,
        from_token,
        to_token,
        from_amount,
        to_amount,
        from_usd,
        to_usd,
        tx_hash,
        block_time,
        contract_address,
        evt_index,
        trace_address
    FROM swap
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- CREATE UNIQUE INDEX IF NOT EXISTS oneinch_swaps_unique_trace_address_idx_aea ON oneinch.swaps2 (tx_hash, trace_address);
-- CREATE UNIQUE INDEX IF NOT EXISTS oneinch_swaps_unique_evt_index_idx ON oneinch.swaps2 (tx_hash, evt_index);
-- CREATE INDEX IF NOT EXISTS oneinch_swaps_idx ON oneinch.swaps2 USING BRIN (block_time);
-- CREATE INDEX IF NOT EXISTS oneinch_swaps_idx_tx_from ON oneinch.swaps2 (tx_from);

-- backfill
SELECT oneinch.insert_swap('2019-01-01', (SELECT now()), (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'), SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes') WHERE NOT EXISTS (SELECT * FROM oneinch.swaps2 LIMIT 1);

-- INSERT INTO cron.job (schedule, command)
-- VALUES ('*/15 * * * *', $$SELECT oneinch.insert_swap((SELECT max(block_time) - interval '2 days' FROM oneinch.swaps2), (SELECT now() - interval '20 minutes'), (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '2 days' FROM oneinch.swaps2)), SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes');$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
