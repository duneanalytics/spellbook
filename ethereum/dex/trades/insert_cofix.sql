CREATE OR REPLACE FUNCTION dex.insert_cofix(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
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
        --Cofix v1 WETH/HBTC
        SELECT
            t.evt_block_time AS block_time,
            'Cofix' AS project,
            '1' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL::bytea AS trader_b,
            t."amountOut" AS token_a_amount_raw,
            t."amountIn" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."outToken" AS token_a_address,
            CASE WHEN t."outToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN f.token ELSE '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' END AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM cofix."eth_hbtc_evt_Swap" t
        INNER JOIN cofix."CoFiXFactory_evt_PairCreated" f ON f.pair = t.contract_address

        UNION ALL

         --Cofix v1 WETH/USDT
        SELECT
            t.evt_block_time AS block_time,
            'Cofix' AS project,
            '1' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL::bytea AS trader_b,
            t."amountOut" AS token_a_amount_raw,
            t."amountIn" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."outToken" AS token_a_address,
            CASE WHEN t."outToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN f.token ELSE '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' END AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM cofix."Pair_ETH_USDT_evt_Swap" t
        INNER JOIN cofix."CoFiXFactory_evt_PairCreated" f ON f.pair = t.contract_address

        UNION ALL

         --Cofix v2
        SELECT
            t.evt_block_time AS block_time,
            'Cofix' AS project,
            '2' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL::bytea AS trader_b,
            t."amountOut" AS token_a_amount_raw,
            t."amountIn" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            t."outToken" AS token_a_address,
            CASE WHEN t."outToken" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN f.token ELSE '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' END AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            t.evt_index
        FROM cofix_v2."CoFiXV2Pair_evt_Swap" t
        INNER JOIN cofix_v2."CoFiXV2Factory_evt_PairCreated" f ON f.pair = t.contract_address
    ) dexs
    INNER JOIN ethereum.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN erc20.tokens erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN prices.usd pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.minute >= start_ts
        AND pa.minute < end_ts
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
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

-- fill 2020
SELECT dex.insert_cofix(
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
    AND project = 'Cofix'
);

-- fill 2021
SELECT dex.insert_cofix(
    '2021-01-01',
    '2022-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2022-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= '2022-01-01'
    AND project = 'Cofix'
);

-- fill 2022
SELECT dex.insert_cofix(
    '2022-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2022-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Cofix'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_cofix(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Cofix'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Cofix')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;