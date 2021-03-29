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
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id
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