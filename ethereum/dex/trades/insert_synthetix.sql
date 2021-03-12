CREATE OR REPLACE FUNCTION dex.insert_synthetix(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
            tr.block_time,
            a.symbol AS token_a_symbol,
            b.symbol AS token_b_symbol,
            token_a_amount,
            token_b_amount,
            'Synthetix' AS project,
            NULL AS version,
            'DEX' AS category,
            trader_a,
            trader_b,
            token_a_amount_raw,
            token_b_amount_raw,
            token_a_amount_usd AS usd_amount,
            token_a_address,
            token_b_address,
            exchange_contract_address,
            tx_hash,
            tx."from" as tx_from,
            tx."to" as tx_to,
            NULL AS trace_address,
            evt_index,
            trade_id
        FROM synthetix.trades tr
        LEFT JOIN synthetix.symbols a ON tr.token_a_address = a.address
        LEFT JOIN synthetix.symbols b ON tr.token_b_address = b.address
        INNER JOIN ethereum.transactions tx
            ON tr.tx_hash = tx.hash
            AND tx.block_time >= start_ts
            AND tx.block_time < end_ts
            AND tx.block_number >= start_block
            AND tx.block_number < end_block
        WHERE tr.block_time >= start_ts
        AND tr.block_time < end_ts
    ) dexs

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT dex.insert_synthetix(
    '2019-01-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2019-01-01'
    AND block_time <= '2020-01-01'
    AND project = 'Synthetix'
);

-- fill 2020
SELECT dex.insert_synthetix(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND project = 'Synthetix'
);

-- fill 2021
SELECT dex.insert_synthetix(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-07-01'),
    (SELECT max(number) FROM ethereum.blocks)
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now()
    AND project = 'Synthetix'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_synthetix(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Synthetix'),
        (SELECT now()),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Synthetix')),
        (SELECT MAX(number) FROM ethereum.blocks));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;