CREATE OR REPLACE FUNCTION dex.insert_hashflow(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
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
        bep20a.symbol AS token_a_symbol,
        bep20b.symbol AS token_b_symbol,
        token_a_amount,
        token_b_amount,
        project,
        version,
        category,
        trader_a as trader_a,
        trader_b,
        token_a_amount * 10 ^ bep20a.decimals AS token_a_amount_raw,
        token_b_amount * 10 ^ bep20b.decimals AS token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount * pa.price,
            token_b_amount * pb.price
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
            block_time,
            maker_symbol AS token_a_symbol,
            taker_symbol AS token_b_symbol,
            maker_token_amount AS token_a_amount,
            taker_token_amount AS token_b_amount,
            'hashflow' as project,
            '1' as version,
            'DEX' as category,
            pool as trader_a,
            trader as trader_b,
            usd_amount,
            maker_token as token_a_address,
            taker_token as token_b_address,
            router_contract as exchange_contract_address,
            tx_hash,
            NULL::integer[] as trace_address,
            (case when composite_index=-1 then NULL::integer else composite_index end) as evt_index -- -1 means decoded from traces
        FROM hashflow.trades
        WHERE fill_status is true -- success trade
                and block_time >= start_ts AND block_time < end_ts
    ) dexs
    INNER JOIN bsc.transactions tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN bep20.tokens bep20a ON bep20a.contract_address = dexs.token_a_address
    LEFT JOIN bep20.tokens bep20b ON bep20b.contract_address = dexs.token_b_address
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


-- fill 2022
delete from dex.trades WHERE project='hashflow';
SELECT dex.insert_hashflow(
    '2022-01-24',
    now(),
    (SELECT max(number) FROM bsc.blocks WHERE time < '2022-01-24'),
    (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2022-01-24'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'hashflow'
);

INSERT INTO cron.job (schedule, command)
VALUES ('*/10 * * * *', $$
    SELECT dex.insert_hashflow(
        (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='hashflow'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM bsc.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM dex.trades WHERE project='hashflow')),
        (SELECT MAX(number) FROM bsc.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;