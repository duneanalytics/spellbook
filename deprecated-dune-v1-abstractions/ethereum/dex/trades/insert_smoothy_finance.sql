CREATE OR REPLACE FUNCTION dex.insert_smoothy_finance(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        coalesce(trader_a, tx."from") AS trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / 10 ^ pa.decimals * pa.price,
            token_b_amount_raw / 10 ^ pb.decimals * pb.price
        ) AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        SELECT 
            evt_block_time AS block_time,
            'Smoothy Finance' AS project,
            '1' AS version,
            'DEX' AS category,
            buyer AS trader_a,
            NULL::bytea AS trader_b,
            "outAmount" AS token_a_amount_raw,
            "inAmount" AS token_b_amount_raw,
            NULL::numeric AS usd_amount,
            ta.address AS token_a_address,
            tb.address AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL::integer[] AS trace_address,
            evt_index
        FROM smoothy."Root_evt_Swap"
        LEFT JOIN (
            SELECT 
                a.nr - 1 AS id,
                a.elem AS address
            FROM (
                SELECT array_agg("tokens") AS tokens
                FROM smoothy."Root_call_addTokens"
            ) f
            LEFT JOIN LATERAL UNNEST(f.tokens)
                WITH ORDINALITY AS a(elem, nr) ON TRUE
        ) ta ON ta.id = "bTokenIdOut"
        LEFT JOIN (
            SELECT 
                a.nr - 1 AS id,
                a.elem AS address
            FROM (
                SELECT array_agg("tokens") AS tokens
                FROM smoothy."Root_call_addTokens"
            ) f
            LEFT JOIN LATERAL UNNEST(f.tokens)
                WITH ORDINALITY AS a(elem, nr) ON TRUE
        ) tb ON tb.id = "bTokenIdIn"
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

-- fill 2021
SELECT dex.insert_smoothy_finance(
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
    AND project = 'Smoothy Finance'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/12 * * * *', $$
    SELECT dex.insert_smoothy_finance(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Smoothy Finance'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='Smoothy Finance')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;