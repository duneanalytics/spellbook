CREATE OR REPLACE FUNCTION dex.insert_airswap(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id
    FROM (
        SELECT
            evt_block_time as block_time,
            'airswap' as project,
            'light' as version,
            'DEX' as category,
            "senderWallet" as trader_a, --define taker as trader a
            "signerWallet" as trader_b, --define maker as trader b
            "senderAmount" as token_a_amount_raw,
            "signerAmount" as token_b_amount_raw,
            "senderToken" as token_a_address,
            "signerToken" as token_b_address,
            contract_address as exchange_contract_address,
            evt_tx_hash as tx_hash,
            NULL::integer[] as trace_address,
            NULL::numeric as usd_amount,
            evt_index
        FROM airswap."Light_evt_Swap"

        UNION ALL

        SELECT
            evt_block_time as block_time,
            'airswap' as project,
            'swap' as version,
            'DEX' as category,
            "senderWallet" as trader_a, --define taker as trader a
            "signerWallet" as trader_b, --define maker as trader b
            "senderAmount" as token_a_amount_raw,
            "signerAmount" as token_b_amount_raw,
            "senderToken" as token_a_address,
            "signerToken" as token_b_address,
            contract_address as exchange_contract_address,
            evt_tx_hash as tx_hash,
            NULL::integer[] as trace_address,
            NULL::numeric as usd_amount,
            evt_index
        FROM airswap."swap_evt_Swap"
        
        UNION ALL

        SELECT
            evt_block_time as block_time,
            'airswap' as project,
            'swap_v3' as version,
            'DEX' as category,
            "senderWallet" as trader_a, --define taker as trader a
            "signerWallet" as trader_b, --define maker as trader b
            "senderAmount" as token_a_amount_raw,
            "signerAmount" as token_b_amount_raw,
            "senderToken" as token_a_address,
            "signerToken" as token_b_address,
            contract_address as exchange_contract_address,
            evt_tx_hash as tx_hash,
            NULL::integer[] as trace_address,
            NULL::numeric as usd_amount,
            evt_index
        FROM airswap."Swap_v3_evt_Swap"
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

--fill 2018
SELECT dex.insert_airswap(
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
    AND project = 'airswap'
);

--fill 2019
SELECT dex.insert_airswap(
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
    AND project = 'airswap'
);

-- fill 2020
SELECT dex.insert_airswap(
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
    AND project = 'airswap'
);

-- fill 2021
SELECT dex.insert_airswap(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'airswap'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_airswap(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='airswap'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='airswap')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
